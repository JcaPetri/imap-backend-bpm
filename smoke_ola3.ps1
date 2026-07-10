# =============================================================================
# smoke_ola3.ps1 - Ola 3: terminate end + parallel M×N + timer ciclico
# =============================================================================
#   A) TERMINATE END: parallel split -> {user_task ; svc -> end(terminate=true)}.
#      La rama que llega al terminate mata la otra (user_task) y completa ya.
#   B) PARALLEL M×N: split(1-2) -> {a,b} -> gw(2-in/2-out join+split) -> {c,d}
#      -> join(2-1) -> end. Verifica join-then-split combinado.
#   C) TIMER CICLICO: user_task + boundary non-interrupting timer
#      (repeatEverySeconds=2, maxRepeats=2) -> svc_rem cada vez. 2 disparos +
#      1 reschedule; completar la task corta el ciclo.
#
# ASCII puro, sin ternario (PowerShell 5.1). Self-cleanup al final.
# =============================================================================

param(
    [string]$Email    = $env:IMAP_EMAIL,
    [string]$Password = $env:IMAP_PASSWORD,
    [string]$TenantId = $env:IMAP_TENANT,
    [switch]$Local,
    [switch]$Prod
)

if ($Prod) {
    $iam = 'https://imaps.com.ar/imap/iam'
    $bpm = 'https://imaps.com.ar/imap/bpm'
} else {
    $iam = 'http://localhost:8091/imap/iam'
    $bpm = 'http://localhost:8093/imap/bpm'
}

if (-not $Email -or -not $Password -or -not $TenantId) {
    Write-Error 'Faltan creds IMAP_EMAIL / IMAP_PASSWORD / IMAP_TENANT'
    exit 1
}

$script:passes = 0
$script:failures = 0
function Fail($m)  { Write-Host "FAIL: $m" -ForegroundColor Red;   $script:failures++ }
function OKMsg($m) { Write-Host "OK   $m"   -ForegroundColor Green; $script:passes++ }
function Step($n, $m) { Write-Host ''; Write-Host "[$n] $m" -ForegroundColor Cyan }

Step 1 'Login'
$tok = (Invoke-RestMethod -Uri "$iam/v1/auth/login" -Method POST -ContentType 'application/json' `
    -Body (@{ email=$Email; password=$Password } | ConvertTo-Json)).accessToken
$H = @{ Authorization="Bearer $tok"; 'X-Tenant-Id'=$TenantId; 'Content-Type'='application/json' }
OKMsg 'logged in'

$run = (Get-Date).ToString('yyyyMMddHHmmss')
$createdPdIds = @()
$startedInstances = @()

function Get-Instance($id) { return Invoke-RestMethod -Uri "$bpm/v1/bpm/instance/$id" -Headers $H }
function Count-Audit($inst, $evt) { return (@($inst.auditLog | Where-Object { $_.eventType -eq $evt })).Count }
function Svc-Ran($inst, $code) { return (@($inst.auditLog | Where-Object { $_.eventType -eq 'service_task.completed' -and $_.data.elementCode -eq $code })).Count }
function Boundary-Fired($inst, $code) { return (@($inst.auditLog | Where-Object { $_.eventType -eq 'boundary.fired' -and $_.data.boundaryCode -eq $code })).Count }
function New-Pd($body) {
    $resp = Invoke-RestMethod -Uri "$bpm/v1/bpm/admin/processdef" -Method POST -Headers $H -Body ($body | ConvertTo-Json -Depth 14)
    $script:createdPdIds += $resp.processdefId
    return $resp
}
function Start-Inst($verId, $payload) {
    $inst = Invoke-RestMethod -Uri "$bpm/v1/bpm/process/$verId/start" -Method POST -Headers $H -Body ($payload | ConvertTo-Json)
    $script:startedInstances += $inst.id
    return $inst
}

# =============================================================================
# A) TERMINATE END EVENT
# =============================================================================
Step 2 'A: terminate end mata la rama paralela'
$pdA = New-Pd @{
    header = @{ code="mi_term_$run"; name='Terminate'; description='tmp'; lifecycle='active' }
    flowElements = @(
        @{ code='start';    name='Start';   type='start_event';      sortOrder=1 }
        @{ code='psplit';   name='PSplit';  type='parallel_gateway'; sortOrder=2 }
        @{ code='utask';    name='UTask';   type='user_task';        sortOrder=3 }
        @{ code='end_main'; name='EndMain'; type='end_event';        sortOrder=4 }
        @{ code='svc';      name='Svc';     type='service_task';     sortOrder=5; config=@{ serviceCode='bpm.test.echo' } }
        @{ code='tend';     name='TEnd';    type='end_event';        sortOrder=6; config=@{ terminate=$true } }
    )
    sequenceFlows = @(
        @{ sourceCode='start';  targetCode='psplit';   sortOrder=1 }
        @{ sourceCode='psplit'; targetCode='utask';    sortOrder=2 }
        @{ sourceCode='psplit'; targetCode='svc';      sortOrder=3 }
        @{ sourceCode='utask';  targetCode='end_main'; sortOrder=4 }
        @{ sourceCode='svc';    targetCode='tend';     sortOrder=5 }
    )
}
$ia = Start-Inst $pdA.processversionId @{}
$a = Get-Instance $ia.id
if ((Count-Audit $a 'instance.terminated') -ge 1) { OKMsg 'A: instance.terminated presente' } else { Fail 'A: falta instance.terminated' }
if ($a.lifecycle -eq 'completed') { OKMsg 'A: instance completed (abortada)' } else { Fail "A: lifecycle=$($a.lifecycle)" }
$utaskCancelled = @($a.tasks | Where-Object { $_.flowElementCode -eq 'utask' -and $_.lifecycle -eq 'cancelled' }).Count
if ($utaskCancelled -ge 1) { OKMsg 'A: user_task de la otra rama cancelada' } else { Fail 'A: la user_task no quedo cancelada' }

# =============================================================================
# B) PARALLEL M×N (join-then-split)
# =============================================================================
Step 3 'B: parallel M×N (2-in/2-out)'
$pdB = New-Pd @{
    header = @{ code="mi_mxn_$run"; name='MxN'; description='tmp'; lifecycle='active' }
    flowElements = @(
        @{ code='start'; name='Start'; type='start_event';      sortOrder=1 }
        @{ code='pj1';   name='PJ1';   type='parallel_gateway'; sortOrder=2 }
        @{ code='svc_a'; name='SvcA';  type='service_task';     sortOrder=3; config=@{ serviceCode='bpm.test.echo' } }
        @{ code='svc_b'; name='SvcB';  type='service_task';     sortOrder=4; config=@{ serviceCode='bpm.test.echo' } }
        @{ code='mxn';   name='MxN';   type='parallel_gateway'; sortOrder=5 }
        @{ code='svc_c'; name='SvcC';  type='service_task';     sortOrder=6; config=@{ serviceCode='bpm.test.echo' } }
        @{ code='svc_d'; name='SvcD';  type='service_task';     sortOrder=7; config=@{ serviceCode='bpm.test.echo' } }
        @{ code='pj2';   name='PJ2';   type='parallel_gateway'; sortOrder=8 }
        @{ code='end';   name='End';   type='end_event';        sortOrder=9 }
    )
    sequenceFlows = @(
        @{ sourceCode='start'; targetCode='pj1';   sortOrder=1 }
        @{ sourceCode='pj1';   targetCode='svc_a'; sortOrder=2 }
        @{ sourceCode='pj1';   targetCode='svc_b'; sortOrder=3 }
        @{ sourceCode='svc_a'; targetCode='mxn';   sortOrder=4 }
        @{ sourceCode='svc_b'; targetCode='mxn';   sortOrder=5 }
        @{ sourceCode='mxn';   targetCode='svc_c'; sortOrder=6 }
        @{ sourceCode='mxn';   targetCode='svc_d'; sortOrder=7 }
        @{ sourceCode='svc_c'; targetCode='pj2';   sortOrder=8 }
        @{ sourceCode='svc_d'; targetCode='pj2';   sortOrder=9 }
        @{ sourceCode='pj2';   targetCode='end';   sortOrder=10 }
    )
}
$ib = Start-Inst $pdB.processversionId @{}
$b = Get-Instance $ib.id
if ((Count-Audit $b 'gateway.joinsplit') -ge 1) { OKMsg 'B: gateway.joinsplit presente' } else { Fail 'B: falta gateway.joinsplit' }
if ((Svc-Ran $b 'svc_c') -ge 1 -and (Svc-Ran $b 'svc_d') -ge 1) { OKMsg 'B: ambas ramas del fan-out (svc_c, svc_d) corrieron' } else { Fail 'B: no corrieron las 2 ramas post-M×N' }
if ($b.lifecycle -eq 'completed') { OKMsg 'B: instance completed' } else { Fail "B: lifecycle=$($b.lifecycle)" }

# =============================================================================
# C) TIMER CICLICO (boundary non-interrupting, repeatEverySeconds=2, maxRepeats=2)
# =============================================================================
Step 4 'C: timer ciclico (2 disparos + 1 reschedule)'
$pdC = New-Pd @{
    header = @{ code="mi_cyc_$run"; name='Cyclic'; description='tmp'; lifecycle='active' }
    flowElements = @(
        @{ code='start';    name='Start';   type='start_event';    sortOrder=1 }
        @{ code='utask';    name='UTask';   type='user_task';      sortOrder=2 }
        @{ code='end_main'; name='EndMain'; type='end_event';      sortOrder=3 }
        @{ code='bnd';      name='Bnd';     type='boundary_event'; sortOrder=4; config=@{ boundary=@{ attachedTo='utask'; interrupting=$false }; timer=@{ delaySeconds=2; repeatEverySeconds=2; maxRepeats=2 } } }
        @{ code='svc_rem';  name='SvcRem';  type='service_task';   sortOrder=5; config=@{ serviceCode='bpm.test.echo' } }
        @{ code='end_rem';  name='EndRem';  type='end_event';      sortOrder=6 }
    )
    sequenceFlows = @(
        @{ sourceCode='start'; targetCode='utask';    sortOrder=1 }
        @{ sourceCode='utask'; targetCode='end_main'; sortOrder=2 }
        @{ sourceCode='bnd';   targetCode='svc_rem';  sortOrder=3 }
        @{ sourceCode='svc_rem'; targetCode='end_rem'; sortOrder=4 }
    )
}
$ic = Start-Inst $pdC.processversionId @{}
Write-Host '     esperando los disparos ciclicos (worker poll ~5s)...'
Start-Sleep -Seconds 16
$c = Get-Instance $ic.id
$fired = Boundary-Fired $c 'bnd'
if ($fired -eq 2) { OKMsg "C: 2 disparos ciclicos (maxRepeats=2)" } else { Fail "C: boundary.fired(bnd)=$fired, esperaba 2" }
if ((Count-Audit $c 'boundary.timer.rescheduled') -ge 1) { OKMsg 'C: boundary.timer.rescheduled presente' } else { Fail 'C: falta boundary.timer.rescheduled' }
if ((Svc-Ran $c 'svc_rem') -ge 2) { OKMsg 'C: svc_rem corrio en cada disparo' } else { Fail "C: svc_rem corrio $(Svc-Ran $c 'svc_rem') veces" }
# completar la task del utask corta el ciclo
$utaskC = @($c.tasks | Where-Object { $_.flowElementCode -eq 'utask' -and $_.lifecycle -ne 'completed' })[0]
if ($utaskC) {
    Invoke-RestMethod -Uri "$bpm/v1/bpm/task/$($utaskC.taskId)/complete" -Method POST -Headers $H -Body '{}' | Out-Null
    $c2 = Get-Instance $ic.id
    if ($c2.lifecycle -eq 'completed') { OKMsg 'C: instance completed tras completar la task' } else { Fail "C: lifecycle=$($c2.lifecycle)" }
} else { Fail 'C: no se encontro la task de utask para completar' }

# =============================================================================
# Cleanup
# =============================================================================
Step 5 'Cleanup'
foreach ($iid in $startedInstances) {
    try { Invoke-RestMethod -Uri "$bpm/v1/bpm/instance/$iid`?force=true" -Method DELETE -Headers $H | Out-Null } catch {}
}
foreach ($pdid in $createdPdIds) {
    try { Invoke-RestMethod -Uri "$bpm/v1/bpm/admin/processdef/$pdid" -Method DELETE -Headers $H | Out-Null } catch {}
}
OKMsg 'cleanup done'

Write-Host ''
Write-Host "PASSES=$script:passes  FAILURES=$script:failures" -ForegroundColor Cyan
if ($script:failures -gt 0) { exit 1 } else { exit 0 }
