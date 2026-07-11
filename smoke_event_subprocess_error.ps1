# =============================================================================
# smoke_event_subprocess_error.ps1 - Ola 6.1: event sub-process ERROR-triggered
# =============================================================================
# Un event_sub_process de error a nivel SCOPE de instancia (distinto del boundary
# error que es por-activity): cuando un service_task falla terminal SIN boundary,
# en vez de dejar el token failed + abrir incidente, se dispara el handler de error
# (interrumpe + corre cleanup) y la instancia completa.
#
#   main: start -> svc_fail (bpm.test.fail, sin boundary) -> end_ok
#         + event_sub_process(trigger=error, code='*', interrupting) -> handler
#   Esperado: el fallo dispara el handler, NO abre incidente, NO deja failed,
#             la instancia completa via el handler.
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
function Count-Audit($inst,$evt){ return (@($inst.auditLog | Where-Object { $_.eventType -eq $evt })).Count }
function First-Audit($inst,$evt){ return @($inst.auditLog | Where-Object { $_.eventType -eq $evt })[0] }
function New-Pd($body){ $pd=Invoke-RestMethod -Uri "$bpm/v1/bpm/admin/processdef" -Method POST -Headers $H -Body ($body|ConvertTo-Json -Depth 14); $script:createdPdIds+=$pd.processdefId; return $pd }
function Start-Inst($verId){ $i=Invoke-RestMethod -Uri "$bpm/v1/bpm/process/$verId/start" -Method POST -Headers $H -Body '{}'; $script:startedInstances+=$i.id; return $i }

Step 2 'Handler def (start -> svc echo -> end) — cuerpo del error handler'
$handler=New-Pd @{
    header=@{ code="esperr_handler_$run"; name='ESPerr handler'; description='tmp'; lifecycle='active' }
    flowElements=@(
        @{ code='start'; name='Start'; type='start_event';  sortOrder=1 }
        @{ code='svc';   name='Svc';   type='service_task'; sortOrder=2; config=@{ serviceCode='bpm.test.echo' } }
        @{ code='end';   name='End';   type='end_event';    sortOrder=3 }
    )
    sequenceFlows=@(
        @{ sourceCode='start'; targetCode='svc'; sortOrder=1 }
        @{ sourceCode='svc';   targetCode='end'; sortOrder=2 }
    )
}
OKMsg "handler creado (ver=$($handler.processversionId))"

Step 3 'Main: svc_fail (sin boundary) + event_sub_process(error, *) interrupting'
$main=New-Pd @{
    header=@{ code="esperr_main_$run"; name='ESPerr main'; description='tmp'; lifecycle='active' }
    flowElements=@(
        @{ code='start';    name='Start';  type='start_event';       sortOrder=1 }
        @{ code='svc_fail'; name='SvcFail';type='service_task';      sortOrder=2; config=@{ serviceCode='bpm.test.fail' } }
        @{ code='end_ok';   name='EndOk';  type='end_event';         sortOrder=3 }
        @{ code='esp';      name='Esp';    type='event_sub_process'; sortOrder=9; config=@{ eventSubProcess=@{ trigger='error'; code='*'; interrupting=$true }; callActivity=@{ calledProcessversionId="$($handler.processversionId)" } } }
    )
    sequenceFlows=@(
        @{ sourceCode='start';    targetCode='svc_fail'; sortOrder=1 }
        @{ sourceCode='svc_fail'; targetCode='end_ok';   sortOrder=2 }
    )
}
$ia=Start-Inst $main.processversionId
Start-Sleep -Milliseconds 1500
$a=Get-Instance $ia.id
$et=First-Audit $a 'event_subprocess.triggered'
if ($et) { OKMsg 'event_subprocess.triggered (el fallo disparo el handler de error)' } else { Fail 'no se disparo el event_sub_process de error' }
if ($et -and $et.data.interrupting -eq $true) { OKMsg 'interrupting=true' } else { Fail "interrupting inesperado ($($et.data.interrupting))" }
if ((Count-Audit $a 'incident.opened') -eq 0) { OKMsg 'NO incident.opened (el handler de error lo capturo)' } else { Fail "incident.opened=$(Count-Audit $a 'incident.opened'), esperaba 0" }
if ((Count-Audit $a 'service_task.failed') -eq 0) { OKMsg 'NO service_task.failed (manejado antes del failure definitivo)' } else { Fail "service_task.failed=$(Count-Audit $a 'service_task.failed'), esperaba 0" }
if ($a.lifecycle -eq 'completed') { OKMsg 'instance completed via el error handler' } else { Fail "lifecycle=$($a.lifecycle), esperaba completed" }

Step 4 'Cleanup'
foreach ($pdid in $createdPdIds){
    try { $insts=@(Invoke-RestMethod -Uri "$bpm/v1/bpm/processdef/$pdid/instances" -Headers $H); foreach($x in $insts){ $script:startedInstances+=$x.id } } catch {}
}
foreach ($iid in ($startedInstances | Select-Object -Unique)){ try { Invoke-RestMethod -Uri "$bpm/v1/bpm/instance/$iid`?force=true" -Method DELETE -Headers $H | Out-Null } catch {} }
foreach ($pdid in $createdPdIds){ try { Invoke-RestMethod -Uri "$bpm/v1/bpm/admin/processdef/$pdid" -Method DELETE -Headers $H | Out-Null } catch {} }
OKMsg 'cleanup done'

Write-Host ''
Write-Host "PASSES=$script:passes  FAILURES=$script:failures" -ForegroundColor Cyan
