# =============================================================================
# smoke_inclusive_reachability.ps1 - Ola 6.2: inclusive-join full-reachability
# =============================================================================
#   A) ESTRUCTURADO (regresion): inclusive split (2 condiciones true) -> 2 ramas ->
#      inclusive join. Completa por cardinalidad del ancla (mode=structured). Sin cambios.
#   B) NO-ESTRUCTURADO (nuevo): exclusive split -> UNA rama de 2 -> inclusive join
#      (2 incoming). El viejo fallback esperaba incoming.size()=2 -> DEADLOCK. Ahora
#      la reachability detecta que ningun token vivo puede llegar -> completa (mode=reachability).
#
# ASCII puro. Self-cleanup.
# =============================================================================

param(
    [string]$Email    = $env:IMAP_EMAIL,
    [string]$Password = $env:IMAP_PASSWORD,
    [string]$TenantId = $env:IMAP_TENANT,
    [switch]$Local,
    [switch]$Prod
)

if ($Prod) { $iam='https://imaps.com.ar/imap/iam'; $bpm='https://imaps.com.ar/imap/bpm' }
else       { $iam='http://localhost:8091/imap/iam'; $bpm='http://localhost:8093/imap/bpm' }
if (-not $Email -or -not $Password -or -not $TenantId) { Write-Error 'Faltan creds'; exit 1 }

$script:passes=0; $script:failures=0
function Fail($m){ Write-Host "FAIL: $m" -ForegroundColor Red; $script:failures++ }
function OKMsg($m){ Write-Host "OK   $m" -ForegroundColor Green; $script:passes++ }
function Step($n,$m){ Write-Host ''; Write-Host "[$n] $m" -ForegroundColor Cyan }

Step 1 'Login'
$tok=(Invoke-RestMethod -Uri "$iam/v1/auth/login" -Method POST -ContentType 'application/json' -Body (@{email=$Email;password=$Password}|ConvertTo-Json)).accessToken
$H=@{Authorization="Bearer $tok";'X-Tenant-Id'=$TenantId;'Content-Type'='application/json'}
OKMsg 'logged in'

$run=(Get-Date).ToString('yyyyMMddHHmmss'); $script:createdPdIds=@(); $script:startedInstances=@()
function Get-Instance($id){ return Invoke-RestMethod -Uri "$bpm/v1/bpm/instance/$id" -Headers $H }
function First-Audit($inst,$evt){ return @($inst.auditLog | Where-Object { $_.eventType -eq $evt })[0] }
function New-Pd($body){ $pd=Invoke-RestMethod -Uri "$bpm/v1/bpm/admin/processdef" -Method POST -Headers $H -Body ($body|ConvertTo-Json -Depth 14); $script:createdPdIds+=$pd.processdefId; return $pd }
function Start-Inst($verId,$vars){ $i=Invoke-RestMethod -Uri "$bpm/v1/bpm/process/$verId/start" -Method POST -Headers $H -Body ($vars|ConvertTo-Json); $script:startedInstances+=$i.id; return $i }

Step 2 'A: ESTRUCTURADO - inclusive split (2 true) -> join por cardinalidad'
$pdA=New-Pd @{
    header=@{ code="incr_struct_$run"; name='Inc struct'; description='tmp'; lifecycle='active' }
    flowElements=@(
        @{ code='start';     name='Start'; type='start_event';       sortOrder=1 }
        @{ code='inc_split'; name='Split'; type='inclusive_gateway'; sortOrder=2 }
        @{ code='svc_a';     name='A';     type='service_task';      sortOrder=3; config=@{ serviceCode='bpm.test.echo' } }
        @{ code='svc_b';     name='B';     type='service_task';      sortOrder=4; config=@{ serviceCode='bpm.test.echo' } }
        @{ code='inc_join';  name='Join';  type='inclusive_gateway'; sortOrder=5 }
        @{ code='end';       name='End';   type='end_event';         sortOrder=6 }
    )
    sequenceFlows=@(
        @{ sourceCode='start';     targetCode='inc_split'; sortOrder=1 }
        @{ sourceCode='inc_split'; targetCode='svc_a';     sortOrder=2; conditionExpr='${a}' }
        @{ sourceCode='inc_split'; targetCode='svc_b';     sortOrder=3; conditionExpr='${b}' }
        @{ sourceCode='svc_a';     targetCode='inc_join';  sortOrder=4 }
        @{ sourceCode='svc_b';     targetCode='inc_join';  sortOrder=5 }
        @{ sourceCode='inc_join';  targetCode='end';       sortOrder=6 }
    )
}
$ia=Start-Inst $pdA.processversionId @{ a=$true; b=$true }
Start-Sleep -Milliseconds 600
$a=Get-Instance $ia.id
$jcA=First-Audit $a 'inclusive.join.completed'
if ($jcA -and $jcA.data.mode -eq 'structured') { OKMsg 'A: join completado mode=structured (sin regresion)' } else { Fail "A: mode inesperado ($($jcA.data.mode))" }
if ($jcA -and [int]$jcA.data.branchesJoined -eq 2) { OKMsg 'A: unio 2 ramas' } else { Fail "A: branchesJoined=$($jcA.data.branchesJoined), esperaba 2" }
if ($a.lifecycle -eq 'completed') { OKMsg 'A: instance completed' } else { Fail "A: lifecycle=$($a.lifecycle)" }

Step 3 'B: NO-ESTRUCTURADO - exclusive split -> 1 rama -> inclusive join (2 incoming)'
$pdB=New-Pd @{
    header=@{ code="incr_unstruct_$run"; name='Inc unstruct'; description='tmp'; lifecycle='active' }
    flowElements=@(
        @{ code='start';    name='Start'; type='start_event';       sortOrder=1 }
        @{ code='xor';      name='Xor';   type='exclusive_gateway'; sortOrder=2 }
        @{ code='svc_a';    name='A';     type='service_task';      sortOrder=3; config=@{ serviceCode='bpm.test.echo' } }
        @{ code='svc_b';    name='B';     type='service_task';      sortOrder=4; config=@{ serviceCode='bpm.test.echo' } }
        @{ code='inc_join'; name='Join';  type='inclusive_gateway'; sortOrder=5 }
        @{ code='end';      name='End';   type='end_event';         sortOrder=6 }
    )
    sequenceFlows=@(
        @{ sourceCode='start'; targetCode='xor';      sortOrder=1 }
        @{ sourceCode='xor';   targetCode='svc_a';    sortOrder=2; conditionExpr='${go}' }
        @{ sourceCode='xor';   targetCode='svc_b';    sortOrder=3 }
        @{ sourceCode='svc_a'; targetCode='inc_join'; sortOrder=4 }
        @{ sourceCode='svc_b'; targetCode='inc_join'; sortOrder=5 }
        @{ sourceCode='inc_join'; targetCode='end';   sortOrder=6 }
    )
}
$ib=Start-Inst $pdB.processversionId @{ go=$true }
Start-Sleep -Milliseconds 700
$b=Get-Instance $ib.id
$jcB=First-Audit $b 'inclusive.join.completed'
if ($jcB -and $jcB.data.mode -eq 'reachability') { OKMsg 'B: join completado mode=reachability (no deadlock)' } else { Fail "B: mode inesperado ($($jcB.data.mode)) - sin el fix esto deadlockeaba" }
if ($jcB -and [int]$jcB.data.branchesJoined -eq 1) { OKMsg 'B: unio 1 rama (la unica que llego)' } else { Fail "B: branchesJoined=$($jcB.data.branchesJoined), esperaba 1" }
if ($b.lifecycle -eq 'completed') { OKMsg 'B: instance completed (reachability evito el deadlock)' } else { Fail "B: lifecycle=$($b.lifecycle), esperaba completed" }

Step 4 'Cleanup'
foreach ($iid in ($startedInstances | Select-Object -Unique)){ try { Invoke-RestMethod -Uri "$bpm/v1/bpm/instance/$iid`?force=true" -Method DELETE -Headers $H | Out-Null } catch {} }
foreach ($pdid in $createdPdIds){ try { Invoke-RestMethod -Uri "$bpm/v1/bpm/admin/processdef/$pdid" -Method DELETE -Headers $H | Out-Null } catch {} }
OKMsg 'cleanup done'

Write-Host ''
Write-Host "PASSES=$script:passes  FAILURES=$script:failures" -ForegroundColor Cyan
