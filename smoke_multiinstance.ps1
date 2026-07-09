# =============================================================================
# smoke_multiinstance.ps1 - Multi-instance parallel MVP (motor BPM)
# =============================================================================
# Valida el marcador config.multiInstance sobre user_task:
#   start -> review(user_task, MI over ${items}) -> end
#
# Escenarios:
#   A) items=[a,b,c] -> 3 tasks -> completar las 3 -> instance completed
#      (audit mi.split cardinality=3 + mi.join.completed)
#   B) items=[]      -> skip (mi.empty) -> instance completed, 0 tasks
#   C) items=[solo]  -> 1 task -> completar -> instance completed
#
# Modela el patron de smoke_service_task_registry.ps1 (auth -> seed -> start ->
# assert audit -> cleanup). ASCII puro, sin ternario (PowerShell 5.1).
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

# --- Login ---
Step 1 'Login'
$tok = (Invoke-RestMethod -Uri "$iam/v1/auth/login" -Method POST -ContentType 'application/json' `
    -Body (@{ email=$Email; password=$Password } | ConvertTo-Json)).accessToken
$H = @{ Authorization="Bearer $tok"; 'X-Tenant-Id'=$TenantId; 'Content-Type'='application/json' }
OKMsg 'logged in'

# --- Seed processdef: start -> review(user_task MI) -> end ---
Step 2 'Create processdef demo_multiinstance (user_task multi-instance)'
$run = (Get-Date).ToString('yyyyMMddHHmmss')
$code = "demo_mi_$run"
$body = @{
    header = @{ code=$code; name='Smoke multi-instance'; description='tmp'; lifecycle='active' }
    flowElements = @(
        @{ code='start';  name='Start';  type='start_event'; sortOrder=1 }
        @{ code='review'; name='Review'; type='user_task';   sortOrder=2; config=@{
              multiInstance=@{ collection='${items}'; elementVar='line'; mode='parallel'; outputCollection='reviews' } } }
        @{ code='end';    name='End';    type='end_event';   sortOrder=3 }
    )
    sequenceFlows = @(
        @{ sourceCode='start';  targetCode='review'; sortOrder=1 }
        @{ sourceCode='review'; targetCode='end';    sortOrder=2 }
    )
} | ConvertTo-Json -Depth 12
$pd = Invoke-RestMethod -Uri "$bpm/v1/bpm/admin/processdef" -Method POST -Headers $H -Body $body
$verId = $pd.processversionId
$pdId  = $pd.processdefId
if ($verId) { OKMsg "processdef created verId=$verId" } else { Fail 'no processversionId'; }

$startedInstances = @()

function Get-Instance($id) { return Invoke-RestMethod -Uri "$bpm/v1/bpm/instance/$id" -Headers $H }
function Count-Audit($inst, $evt) {
    return (@($inst.audit | Where-Object { $_.eventType -eq $evt })).Count
}

# =============================================================================
# Escenario A: items=[a,b,c] -> 3 tasks -> completar -> completed
# =============================================================================
Step 3 'Escenario A: start con items=[alpha,beta,gamma]'
$instA = Invoke-RestMethod -Uri "$bpm/v1/bpm/process/$verId/start" -Method POST -Headers $H `
    -Body (@{ items=@('alpha','beta','gamma') } | ConvertTo-Json)
$startedInstances += $instA.id
$a = Get-Instance $instA.id

$reviewTasks = @($a.tasks | Where-Object { $_.flowElementCode -eq 'review' })
if ($reviewTasks.Count -eq 3) { OKMsg 'MI split creo 3 tasks' } else { Fail "esperaba 3 tasks, hubo $($reviewTasks.Count)" }
if ((Count-Audit $a 'mi.split') -ge 1) { OKMsg 'audit mi.split presente' } else { Fail 'falta audit mi.split' }
$split = @($a.audit | Where-Object { $_.eventType -eq 'mi.split' })[0]
if ($split -and "$($split.data.cardinality)" -eq '3') { OKMsg 'mi.split cardinality=3' } else { Fail "cardinality != 3 (fue $($split.data.cardinality))" }
if ($a.lifecycle -eq 'active') { OKMsg 'instance sigue active (ancla esperando)' } else { Fail "instance lifecycle=$($a.lifecycle), esperaba active" }

Step 4 'Completar las 3 tasks MI'
foreach ($t in $reviewTasks) {
    Invoke-RestMethod -Uri "$bpm/v1/bpm/task/$($t.taskId)/complete" -Method POST -Headers $H `
        -Body (@{ decision='ok' } | ConvertTo-Json) | Out-Null
}
$a2 = Get-Instance $instA.id
if ($a2.lifecycle -eq 'completed') { OKMsg 'instance completed tras las 3' } else { Fail "lifecycle=$($a2.lifecycle), esperaba completed" }
if ((Count-Audit $a2 'mi.join.completed') -ge 1) { OKMsg 'audit mi.join.completed presente' } else { Fail 'falta mi.join.completed' }
$progress = Count-Audit $a2 'mi.join.progress'
if ($progress -eq 2) { OKMsg 'mi.join.progress = 2 (parciales antes del ultimo)' } else { Write-Host "INFO mi.join.progress=$progress (esperaba 2; no bloqueante)" -ForegroundColor Yellow }

# =============================================================================
# Escenario B: items=[] -> skip -> completed, 0 tasks
# =============================================================================
Step 5 'Escenario B: start con items=[] (coleccion vacia)'
$instB = Invoke-RestMethod -Uri "$bpm/v1/bpm/process/$verId/start" -Method POST -Headers $H `
    -Body (@{ items=@() } | ConvertTo-Json)
$startedInstances += $instB.id
$b = Get-Instance $instB.id
$bReview = @($b.tasks | Where-Object { $_.flowElementCode -eq 'review' })
if ($bReview.Count -eq 0) { OKMsg 'coleccion vacia -> 0 tasks' } else { Fail "esperaba 0 tasks, hubo $($bReview.Count)" }
if ((Count-Audit $b 'mi.empty') -ge 1) { OKMsg 'audit mi.empty presente' } else { Fail 'falta mi.empty' }
if ($b.lifecycle -eq 'completed') { OKMsg 'instance completed (skip)' } else { Fail "lifecycle=$($b.lifecycle), esperaba completed" }

# =============================================================================
# Escenario C: items=[solo] -> 1 task -> completar -> completed
# =============================================================================
Step 6 'Escenario C: start con items=[solo] (N=1)'
$instC = Invoke-RestMethod -Uri "$bpm/v1/bpm/process/$verId/start" -Method POST -Headers $H `
    -Body (@{ items=@('solo') } | ConvertTo-Json)
$startedInstances += $instC.id
$c = Get-Instance $instC.id
$cReview = @($c.tasks | Where-Object { $_.flowElementCode -eq 'review' })
if ($cReview.Count -eq 1) { OKMsg 'N=1 -> 1 task' } else { Fail "esperaba 1 task, hubo $($cReview.Count)" }
Invoke-RestMethod -Uri "$bpm/v1/bpm/task/$($cReview[0].taskId)/complete" -Method POST -Headers $H `
    -Body (@{ decision='ok' } | ConvertTo-Json) | Out-Null
$c2 = Get-Instance $instC.id
if ($c2.lifecycle -eq 'completed') { OKMsg 'instance completed (N=1)' } else { Fail "lifecycle=$($c2.lifecycle), esperaba completed" }

# =============================================================================
# Cleanup: delete instances + soft-delete processdef
# =============================================================================
Step 7 'Cleanup'
foreach ($iid in $startedInstances) {
    try { Invoke-RestMethod -Uri "$bpm/v1/bpm/instance/$iid`?force=true" -Method DELETE -Headers $H | Out-Null } catch {}
}
if ($pdId) {
    try { Invoke-RestMethod -Uri "$bpm/v1/bpm/admin/processdef/$pdId" -Method DELETE -Headers $H | Out-Null } catch {}
}
OKMsg 'cleanup done'

# --- Resultado ---
Write-Host ''
Write-Host "PASSES=$script:passes  FAILURES=$script:failures" -ForegroundColor Cyan
if ($script:failures -gt 0) { exit 1 } else { exit 0 }
