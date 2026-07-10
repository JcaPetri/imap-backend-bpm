# =============================================================================
# smoke_incident.ps1 - 4.2 incident management + retry-from-failure
# =============================================================================
#   A) service_task falla terminal (bpm.test.fail, sin boundary) -> incidente
#      'open'; la instancia queda active (colgada), NO failed silencioso.
#   B) resolve -> el incidente pasa a resolved y sale de la lista open.
#   C) retry -> re-corre el paso desde donde fallo (service_task re-invocado).
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
if (-not $Email -or -not $Password -or -not $TenantId) { Write-Error 'Faltan creds'; exit 1 }

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
function Open-Incidents($instId){
    $all=Invoke-RestMethod -Uri "$bpm/v1/bpm/incidents?lifecycle=open" -Headers $H
    return ,@($all | Where-Object { $_.processinstanceId -eq $instId })
}
function New-FailPd($code){
    $body=@{
        header=@{ code=$code; name='Incident smoke'; description='tmp'; lifecycle='active' }
        flowElements=@(
            @{ code='start'; name='Start'; type='start_event';  sortOrder=1 }
            @{ code='svc';   name='Svc';   type='service_task'; sortOrder=2; config=@{ serviceCode='bpm.test.fail' } }
            @{ code='end';   name='End';   type='end_event';    sortOrder=3 }
        )
        sequenceFlows=@(
            @{ sourceCode='start'; targetCode='svc'; sortOrder=1 }
            @{ sourceCode='svc';   targetCode='end'; sortOrder=2 }
        )
    } | ConvertTo-Json -Depth 12
    $resp=Invoke-RestMethod -Uri "$bpm/v1/bpm/admin/processdef" -Method POST -Headers $H -Body $body
    $script:createdPdIds+=$resp.processdefId
    return $resp
}
function Start-Inst($verId){
    $inst=Invoke-RestMethod -Uri "$bpm/v1/bpm/process/$verId/start" -Method POST -Headers $H -Body '{}'
    $script:startedInstances+=$inst.id
    return $inst
}

$pd=New-FailPd "incident_$run"

# =============================================================================
# A) falla -> incidente open
# =============================================================================
Step 2 'A: service_task falla terminal -> incidente open'
$ia=Start-Inst $pd.processversionId
$a=Get-Instance $ia.id
if ((Count-Audit $a 'service_task.failed') -ge 1) { OKMsg 'A: service_task.failed' } else { Fail 'A: no fallo el service_task' }
if ((Count-Audit $a 'incident.opened') -ge 1) { OKMsg 'A: incident.opened' } else { Fail 'A: no se abrio incidente' }
if ($a.lifecycle -ne 'completed') { OKMsg "A: instance NO completed (colgada, lifecycle=$($a.lifecycle))" } else { Fail 'A: la instance completo, esperaba colgada' }
$incs=Open-Incidents $ia.id
if ($incs.Count -eq 1) { OKMsg 'A: 1 incidente open listado' } else { Fail "A: incidentes open=$($incs.Count), esperaba 1" }
$inc=$incs[0]
if ($inc.elementCode -eq 'svc' -and $inc.incidentType -eq 'service_task_failure') { OKMsg "A: incidente correcto (elementCode=svc, errorCode=$($inc.errorCode))" } else { Fail "A: incidente mal formado (elementCode=$($inc.elementCode), type=$($inc.incidentType))" }

# =============================================================================
# B) resolve
# =============================================================================
Step 3 'B: resolve -> sale de la lista open'
Invoke-RestMethod -Uri "$bpm/v1/bpm/incident/$($inc.id)/resolve" -Method POST -Headers $H | Out-Null
$incsAfter=Open-Incidents $ia.id
if ($incsAfter.Count -eq 0) { OKMsg 'B: 0 incidentes open tras resolve' } else { Fail "B: quedan $($incsAfter.Count) open" }
$a2=Get-Instance $ia.id
if ((Count-Audit $a2 'incident.resolved') -ge 1) { OKMsg 'B: incident.resolved auditado' } else { Fail 'B: falta incident.resolved' }

# =============================================================================
# C) retry -> re-corre el paso
# =============================================================================
Step 4 'C: retry -> re-corre el service_task'
$ic=Start-Inst $pd.processversionId
$incsC=Open-Incidents $ic.id
if ($incsC.Count -ge 1) { OKMsg 'C: incidente inicial abierto' } else { Fail 'C: no abrio incidente inicial' }
Invoke-RestMethod -Uri "$bpm/v1/bpm/incident/$($incsC[0].id)/retry" -Method POST -Headers $H | Out-Null
$c=Get-Instance $ic.id
if ((Count-Audit $c 'incident.retried') -ge 1) { OKMsg 'C: incident.retried auditado' } else { Fail 'C: falta incident.retried' }
if ((Count-Audit $c 'service_task.invoked') -ge 2) { OKMsg 'C: service_task re-invocado (corrio de nuevo)' } else { Fail "C: service_task.invoked=$(Count-Audit $c 'service_task.invoked'), esperaba >=2" }
if ((Count-Audit $c 'incident.opened') -ge 2) { OKMsg 'C: nuevo incidente tras re-fallar' } else { Fail "C: incident.opened=$(Count-Audit $c 'incident.opened'), esperaba >=2" }

# =============================================================================
Step 5 'Cleanup'
foreach ($iid in $startedInstances){ try { Invoke-RestMethod -Uri "$bpm/v1/bpm/instance/$iid`?force=true" -Method DELETE -Headers $H | Out-Null } catch {} }
foreach ($pdid in $createdPdIds){ try { Invoke-RestMethod -Uri "$bpm/v1/bpm/admin/processdef/$pdid" -Method DELETE -Headers $H | Out-Null } catch {} }
OKMsg 'cleanup done'

Write-Host ''
Write-Host "PASSES=$script:passes  FAILURES=$script:failures" -ForegroundColor Cyan
if ($script:failures -gt 0){ exit 1 } else { exit 0 }
