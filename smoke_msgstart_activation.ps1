# =============================================================================
# smoke_msgstart_activation.ps1 - chip task_9de0cde9: sync message-start en activacion
# =============================================================================
# Antes: la subscripcion message-start solo se sincronizaba lazy en el 1er load()
# (primer start manual). Un proceso message-triggered nunca arrancado a mano NO
# recibia mensajes -> POST /messages/start devolvia instancesStarted=0.
#
# Ahora: al CREAR/activar el processdef se sincroniza la subscripcion para el tenant.
# El test crea el proceso y SIN arrancarlo a mano manda el mensaje -> debe arrancar.
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
$msg="MSGSTART_$run"
function New-Pd($body){ $pd=Invoke-RestMethod -Uri "$bpm/v1/bpm/admin/processdef" -Method POST -Headers $H -Body ($body|ConvertTo-Json -Depth 14); $script:createdPdIds+=$pd.processdefId; return $pd }

Step 2 "Crear processdef message-start (message '$msg') - SIN arrancarlo a mano"
$pd=New-Pd @{
    header=@{ code="msgstart_$run"; name='Msg start activation'; description='tmp'; lifecycle='active' }
    flowElements=@(
        @{ code='start'; name='Start'; type='start_event';  sortOrder=1; config=@{ message=@{ messageCode=$msg } } }
        @{ code='svc';   name='Svc';   type='service_task'; sortOrder=2; config=@{ serviceCode='bpm.test.echo' } }
        @{ code='end';   name='End';   type='end_event';    sortOrder=3 }
    )
    sequenceFlows=@(
        @{ sourceCode='start'; targetCode='svc'; sortOrder=1 }
        @{ sourceCode='svc';   targetCode='end'; sortOrder=2 }
    )
}
OKMsg "processdef creado (ver=$($pd.processversionId)) - NO se arranco manualmente"

Step 3 "Mandar el mensaje -> debe arrancar (sync en activacion)"
$r=Invoke-RestMethod -Uri "$bpm/v1/bpm/messages/start" -Method POST -Headers $H -Body (@{ messageCode=$msg; variables=@{ message='hi' } }|ConvertTo-Json)
if ([int]$r.instancesStarted -ge 1) { OKMsg "instancesStarted=$($r.instancesStarted) (el mensaje arranco el proceso SIN start manual previo)" } else { Fail "instancesStarted=$($r.instancesStarted), esperaba >=1 (subscripcion NO sincronizada en activacion)" }
foreach ($inst in @($r.instances)){ if ($inst.id) { $script:startedInstances+=$inst.id } }

Step 4 'Cleanup'
# recolectar instancias arrancadas por el mensaje
try { $insts=@(Invoke-RestMethod -Uri "$bpm/v1/bpm/processdef/$($pd.processdefId)/instances" -Headers $H); foreach($x in $insts){ $script:startedInstances+=$x.id } } catch {}
foreach ($iid in ($startedInstances | Select-Object -Unique)){ try { Invoke-RestMethod -Uri "$bpm/v1/bpm/instance/$iid`?force=true" -Method DELETE -Headers $H | Out-Null } catch {} }
foreach ($pdid in $createdPdIds){ try { Invoke-RestMethod -Uri "$bpm/v1/bpm/admin/processdef/$pdid" -Method DELETE -Headers $H | Out-Null } catch {} }
OKMsg 'cleanup done'

Write-Host ''
Write-Host "PASSES=$script:passes  FAILURES=$script:failures" -ForegroundColor Cyan
