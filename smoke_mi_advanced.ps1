# =============================================================================
# smoke_mi_advanced.ps1 - Multi-instance: sequential + completionCondition
# =============================================================================
#   A) SEQUENTIAL: user_task MI mode=sequential sobre ${items} (3) -> una tarea a
#      la vez, en orden; instance completa tras las 3.
#   B) QUORUM (parallel + completionCondition): 5 tareas, corta con
#      ${nrOfCompletedInstances >= 3} -> completar 3 -> completa + 2 canceladas.
#   C) FAIL-FAST (parallel + completionCondition): corta con ${decision == 'rechazado'}
#      -> un rechazo corta y cancela el resto.
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

if ($Prod) { $iam='https://imaps.com.ar/imap/iam'; $bpm='https://imaps.com.ar/imap/bpm' }
else       { $iam='http://localhost:8091/imap/iam'; $bpm='http://localhost:8093/imap/bpm' }

if (-not $Email -or -not $Password -or -not $TenantId) { Write-Error 'Faltan creds IMAP_EMAIL/PASSWORD/TENANT'; exit 1 }

$script:passes=0; $script:failures=0
function Fail($m){ Write-Host "FAIL: $m" -ForegroundColor Red; $script:failures++ }
function OKMsg($m){ Write-Host "OK   $m" -ForegroundColor Green; $script:passes++ }
function Step($n,$m){ Write-Host ''; Write-Host "[$n] $m" -ForegroundColor Cyan }

Step 1 'Login'
$tok=(Invoke-RestMethod -Uri "$iam/v1/auth/login" -Method POST -ContentType 'application/json' -Body (@{email=$Email;password=$Password}|ConvertTo-Json)).accessToken
$H=@{Authorization="Bearer $tok";'X-Tenant-Id'=$TenantId;'Content-Type'='application/json'}
OKMsg 'logged in'

$run=(Get-Date).ToString('yyyyMMddHHmmss'); $createdPdIds=@(); $startedInstances=@()
function Get-Instance($id){ return Invoke-RestMethod -Uri "$bpm/v1/bpm/instance/$id" -Headers $H }
function Count-Audit($inst,$evt){ return (@($inst.auditLog | Where-Object { $_.eventType -eq $evt })).Count }
function Active-Reviews($inst){ return @($inst.tasks | Where-Object { $_.flowElementCode -eq 'review' -and ($_.lifecycle -eq 'created' -or $_.lifecycle -eq 'assigned' -or $_.lifecycle -eq 'in_progress') }) }
function Cancelled-Reviews($inst){ return @($inst.tasks | Where-Object { $_.flowElementCode -eq 'review' -and $_.lifecycle -eq 'cancelled' }) }
function Complete-Task($taskId,$body){ Invoke-RestMethod -Uri "$bpm/v1/bpm/task/$taskId/complete" -Method POST -Headers $H -Body ($body|ConvertTo-Json) | Out-Null }
function New-MiPd($code,$mode,$cc){
    $miCfg=@{ collection='${items}'; elementVar='line'; mode=$mode }
    if ($cc){ $miCfg['completionCondition']=$cc }
    $body=@{
        header=@{ code=$code; name="MI adv $mode"; description='tmp'; lifecycle='active' }
        flowElements=@(
            @{ code='start';  name='Start';  type='start_event'; sortOrder=1 }
            @{ code='review'; name='Review'; type='user_task';   sortOrder=2; config=@{ multiInstance=$miCfg } }
            @{ code='end';    name='End';    type='end_event';   sortOrder=3 }
        )
        sequenceFlows=@(
            @{ sourceCode='start';  targetCode='review'; sortOrder=1 }
            @{ sourceCode='review'; targetCode='end';    sortOrder=2 }
        )
    } | ConvertTo-Json -Depth 12
    $resp=Invoke-RestMethod -Uri "$bpm/v1/bpm/admin/processdef" -Method POST -Headers $H -Body $body
    $script:createdPdIds+=$resp.processdefId
    return $resp
}
function Start-Inst($verId,$items){
    $inst=Invoke-RestMethod -Uri "$bpm/v1/bpm/process/$verId/start" -Method POST -Headers $H -Body (@{items=$items}|ConvertTo-Json)
    $script:startedInstances+=$inst.id
    return $inst
}

# =============================================================================
# A) SEQUENTIAL
# =============================================================================
Step 2 'A: sequential MI (3 items, una tarea a la vez, en orden)'
$pdA=New-MiPd "mi_seq_$run" 'sequential' $null
$ia=Start-Inst $pdA.processversionId @('a','b','c')
$a=Get-Instance $ia.id
if ((Active-Reviews $a).Count -eq 1) { OKMsg 'A: arranca con 1 sola tarea (secuencial)' } else { Fail "A: activas=$((Active-Reviews $a).Count), esperaba 1" }
$steps=0
for ($k=0; $k -lt 5; $k++){
    $cur=Get-Instance $ia.id
    $act=Active-Reviews $cur
    if ($act.Count -eq 0){ break }
    if ($act.Count -ne 1){ Fail "A: en paso $k hay $($act.Count) tareas activas, esperaba 1"; break }
    Complete-Task $act[0].taskId @{ decision='ok' }
    $steps++
}
if ($steps -eq 3) { OKMsg 'A: se completaron 3 en secuencia' } else { Fail "A: pasos=$steps, esperaba 3" }
$a2=Get-Instance $ia.id
if ((Count-Audit $a2 'mi.sequential.next') -eq 2) { OKMsg 'A: mi.sequential.next = 2 (creo el 2do y 3ro)' } else { Fail "A: mi.sequential.next=$(Count-Audit $a2 'mi.sequential.next'), esperaba 2" }
if ($a2.lifecycle -eq 'completed') { OKMsg 'A: instance completed' } else { Fail "A: lifecycle=$($a2.lifecycle)" }

# =============================================================================
# B) QUORUM (parallel + completionCondition nrOfCompletedInstances >= 3)
# =============================================================================
Step 3 'B: quorum 3 de 5 (parallel + completionCondition)'
$pdB=New-MiPd "mi_quorum_$run" 'parallel' '${nrOfCompletedInstances >= 3}'
$ib=Start-Inst $pdB.processversionId @(1,2,3,4,5)
$b=Get-Instance $ib.id
$tasksB=Active-Reviews $b
if ($tasksB.Count -eq 5) { OKMsg 'B: 5 tareas en paralelo' } else { Fail "B: activas=$($tasksB.Count), esperaba 5" }
for ($k=0; $k -lt 3; $k++){ Complete-Task $tasksB[$k].taskId @{ decision='ok' } }
$b2=Get-Instance $ib.id
if ((Count-Audit $b2 'mi.join.completed') -ge 1) { OKMsg 'B: mi.join.completed (corte por quorum)' } else { Fail 'B: no cerro el join' }
$jc=@($b2.auditLog | Where-Object { $_.eventType -eq 'mi.join.completed' })[0]
if ($jc -and $jc.data.reason -eq 'completionCondition') { OKMsg 'B: reason=completionCondition' } else { Fail "B: reason=$($jc.data.reason)" }
if ((Cancelled-Reviews $b2).Count -eq 2) { OKMsg 'B: 2 tareas restantes canceladas' } else { Fail "B: canceladas=$((Cancelled-Reviews $b2).Count), esperaba 2" }
if ($b2.lifecycle -eq 'completed') { OKMsg 'B: instance completed' } else { Fail "B: lifecycle=$($b2.lifecycle)" }

# =============================================================================
# C) FAIL-FAST (parallel + completionCondition decision == 'rechazado')
# =============================================================================
Step 4 "C: fail-fast (un rechazo corta el resto)"
$pdC=New-MiPd "mi_failfast_$run" 'parallel' "`${decision == 'rechazado'}"
$ic=Start-Inst $pdC.processversionId @(1,2,3)
$c=Get-Instance $ic.id
$tasksC=Active-Reviews $c
if ($tasksC.Count -eq 3) { OKMsg 'C: 3 tareas en paralelo' } else { Fail "C: activas=$($tasksC.Count), esperaba 3" }
# completar 1 con 'ok' (NO corta)
Complete-Task $tasksC[0].taskId @{ decision='ok' }
$cMid=Get-Instance $ic.id
if ($cMid.lifecycle -eq 'active') { OKMsg 'C: tras un ok sigue active (no corta)' } else { Fail "C: corto de mas (lifecycle=$($cMid.lifecycle))" }
# completar otro con 'rechazado' (CORTA)
Complete-Task $tasksC[1].taskId @{ decision='rechazado' }
$c2=Get-Instance $ic.id
if ($c2.lifecycle -eq 'completed') { OKMsg 'C: un rechazo corta -> instance completed' } else { Fail "C: lifecycle=$($c2.lifecycle), esperaba completed" }
if ((Cancelled-Reviews $c2).Count -eq 1) { OKMsg 'C: la tarea restante quedo cancelada' } else { Fail "C: canceladas=$((Cancelled-Reviews $c2).Count), esperaba 1" }

# =============================================================================
Step 5 'Cleanup'
foreach ($iid in $startedInstances){ try { Invoke-RestMethod -Uri "$bpm/v1/bpm/instance/$iid`?force=true" -Method DELETE -Headers $H | Out-Null } catch {} }
foreach ($pdid in $createdPdIds){ try { Invoke-RestMethod -Uri "$bpm/v1/bpm/admin/processdef/$pdid" -Method DELETE -Headers $H | Out-Null } catch {} }
OKMsg 'cleanup done'

Write-Host ''
Write-Host "PASSES=$script:passes  FAILURES=$script:failures" -ForegroundColor Cyan
if ($script:failures -gt 0){ exit 1 } else { exit 0 }
