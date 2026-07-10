# =============================================================================
# smoke_inclusive_gateway.ps1 - Inclusive gateway (OR)
# =============================================================================
# Valida inclusive_gateway: en el SPLIT se activan TODAS las ramas cuya condicion
# es true (o el default si ninguna); el JOIN espera EXACTAMENTE a las activadas.
#
# Topologia:
#   start -> inc_split -> { svc_x [cond ${a}], svc_y [cond ${b}], svc_def [default] }
#            -> inc_join -> end
#
# Escenarios:
#   A) {a:true,  b:true}  -> activadas 2 (svc_x, svc_y), svc_def NO corre, join espera 2
#   B) {a:true,  b:false} -> activada 1 (svc_x), svc_y y svc_def NO corren, join espera 1
#   C) {a:false, b:false} -> default (svc_def), svc_x/svc_y NO corren, join espera 1
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
function Split-Activated($inst) {
    $s = @($inst.auditLog | Where-Object { $_.eventType -eq 'inclusive.split' })[0]
    if ($s) { return [int]$s.data.activated } else { return -1 }
}
function Join-Expected($inst) {
    $j = @($inst.auditLog | Where-Object { $_.eventType -eq 'inclusive.join.completed' })[0]
    if ($j) { return [int]$j.data.expected } else { return -1 }
}

# Un solo processdef; se arranca 3 veces con distintos payloads
Step 2 'Create processdef inclusive OR (3 ramas: x[a], y[b], default)'
$pd = Invoke-RestMethod -Uri "$bpm/v1/bpm/admin/processdef" -Method POST -Headers $H -Body (@{
    header = @{ code="mi_inc_$run"; name='Inclusive OR smoke'; description='tmp'; lifecycle='active' }
    flowElements = @(
        @{ code='start';     name='Start';    type='start_event';       sortOrder=1 }
        @{ code='inc_split'; name='IncSplit'; type='inclusive_gateway';  sortOrder=2 }
        @{ code='svc_x';     name='SvcX';     type='service_task';       sortOrder=3; config=@{ serviceCode='bpm.test.echo' } }
        @{ code='svc_y';     name='SvcY';     type='service_task';       sortOrder=4; config=@{ serviceCode='bpm.test.echo' } }
        @{ code='svc_def';   name='SvcDef';   type='service_task';       sortOrder=5; config=@{ serviceCode='bpm.test.echo' } }
        @{ code='inc_join';  name='IncJoin';  type='inclusive_gateway';  sortOrder=6 }
        @{ code='end';       name='End';      type='end_event';          sortOrder=7 }
    )
    sequenceFlows = @(
        @{ sourceCode='start';     targetCode='inc_split'; sortOrder=1 }
        @{ sourceCode='inc_split'; targetCode='svc_x';     sortOrder=2; conditionExpr='${a}' }
        @{ sourceCode='inc_split'; targetCode='svc_y';     sortOrder=3; conditionExpr='${b}' }
        @{ sourceCode='inc_split'; targetCode='svc_def';   sortOrder=4 }
        @{ sourceCode='svc_x';     targetCode='inc_join';  sortOrder=5 }
        @{ sourceCode='svc_y';     targetCode='inc_join';  sortOrder=6 }
        @{ sourceCode='svc_def';   targetCode='inc_join';  sortOrder=7 }
        @{ sourceCode='inc_join';  targetCode='end';       sortOrder=8 }
    )
} | ConvertTo-Json -Depth 14)
$createdPdIds += $pd.processdefId
$verId = $pd.processversionId
OKMsg "processdef creado verId=$verId"

function Run-Scenario($label, $payload, $expectActivated, $expectRan, $expectNotRan) {
    $inst = Invoke-RestMethod -Uri "$bpm/v1/bpm/process/$verId/start" -Method POST -Headers $H -Body ($payload | ConvertTo-Json)
    $script:startedInstances += $inst.id
    $i = Get-Instance $inst.id
    if ((Split-Activated $i) -eq $expectActivated) { OKMsg "${label}: activadas=$expectActivated" } else { Fail "${label}: activadas=$(Split-Activated $i), esperaba $expectActivated" }
    foreach ($c in $expectRan)    { if ((Svc-Ran $i $c) -ge 1) { OKMsg "${label}: $c corrio" } else { Fail "${label}: $c NO corrio" } }
    foreach ($c in $expectNotRan) { if ((Svc-Ran $i $c) -eq 0) { OKMsg "${label}: $c NO corrio (correcto)" } else { Fail "${label}: $c corrio y no debia" } }
    if ((Join-Expected $i) -eq $expectActivated) { OKMsg "${label}: join espero $expectActivated" } else { Fail "${label}: join expected=$(Join-Expected $i), esperaba $expectActivated" }
    if ($i.lifecycle -eq 'completed') { OKMsg "${label}: instance completed" } else { Fail "${label}: lifecycle=$($i.lifecycle), esperaba completed" }
}

Step 3 'Escenario A: {a:true, b:true} -> 2 ramas (svc_x, svc_y)'
Run-Scenario 'A' @{ a=$true;  b=$true }  2 @('svc_x','svc_y') @('svc_def')

Step 4 'Escenario B: {a:true, b:false} -> 1 rama (svc_x)'
Run-Scenario 'B' @{ a=$true;  b=$false } 1 @('svc_x') @('svc_y','svc_def')

Step 5 'Escenario C: {a:false, b:false} -> default (svc_def)'
Run-Scenario 'C' @{ a=$false; b=$false } 1 @('svc_def') @('svc_x','svc_y')

# =============================================================================
# Cleanup
# =============================================================================
Step 6 'Cleanup'
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
