# =============================================================================
# smoke_event_subprocess.ps1 - Ola 6.1: event sub-process (signal-triggered)
# =============================================================================
#   A) INTERRUPTING: main espera en un signal-catch; llega el signal del
#      event_sub_process -> cancela el token del flujo principal, corre el handler,
#      y al terminar el handler la instancia (sin tokens vivos) completa.
#   B) NON-INTERRUPTING: llega el signal -> el handler corre EN PARALELO, el flujo
#      principal sigue esperando; luego el signal del catch principal lo completa.
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
function Broadcast($code){ Invoke-RestMethod -Uri "$bpm/v1/bpm/signals/broadcast" -Method POST -Headers $H -Body (@{ signalCode=$code }|ConvertTo-Json) | Out-Null }

Step 2 'Handler def (start -> svc echo -> end) — cuerpo del event_sub_process'
$handler=New-Pd @{
    header=@{ code="esp_handler_$run"; name='ESP handler'; description='tmp'; lifecycle='active' }
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

# ---------------------------------------------------------------------------
Step 3 'A: INTERRUPTING - signal aborta el flujo principal y corre el handler'
$waitA="ESPWAITA_$run"; $abortA="ESPABORTA_$run"
$mainA=New-Pd @{
    header=@{ code="esp_maina_$run"; name='ESP main A'; description='tmp'; lifecycle='active' }
    flowElements=@(
        @{ code='start';  name='Start'; type='start_event';        sortOrder=1 }
        @{ code='ev_wait';name='Wait';  type='intermediate_event'; sortOrder=2; config=@{ signal=@{ signalCode=$waitA } } }
        @{ code='end_ok'; name='EndOk'; type='end_event';          sortOrder=3 }
        @{ code='esp';    name='Esp';   type='event_sub_process';  sortOrder=9; config=@{ eventSubProcess=@{ trigger='signal'; code=$abortA; interrupting=$true }; callActivity=@{ calledProcessversionId="$($handler.processversionId)" } } }
    )
    sequenceFlows=@(
        @{ sourceCode='start';   targetCode='ev_wait'; sortOrder=1 }
        @{ sourceCode='ev_wait'; targetCode='end_ok';  sortOrder=2 }
    )
}
$ia=Start-Inst $mainA.processversionId
Start-Sleep -Milliseconds 400
$a0=Get-Instance $ia.id
if ((Count-Audit $a0 'event_subprocess.subscribed') -ge 1) { OKMsg 'A: event_subprocess.subscribed al arrancar' } else { Fail 'A: no se suscribio el event_sub_process' }
if ($a0.lifecycle -eq 'active') { OKMsg 'A: instance esperando en el signal-catch (active)' } else { Fail "A: lifecycle=$($a0.lifecycle), esperaba active" }
Broadcast $abortA
Start-Sleep -Milliseconds 700
$a=Get-Instance $ia.id
$et=First-Audit $a 'event_subprocess.triggered'
if ($et) { OKMsg 'A: event_subprocess.triggered' } else { Fail 'A: no se disparo el event_sub_process' }
if ($et -and $et.data.interrupting -eq $true) { OKMsg 'A: interrupting=true' } else { Fail "A: interrupting inesperado ($($et.data.interrupting))" }
if ($et -and [int]$et.data.cancelledTokens -ge 1) { OKMsg "A: cancelo $($et.data.cancelledTokens) token(s) del flujo principal" } else { Fail "A: cancelledTokens=$($et.data.cancelledTokens), esperaba >=1" }
if ($a.lifecycle -eq 'completed') { OKMsg 'A: instance completed tras correr el handler' } else { Fail "A: lifecycle=$($a.lifecycle), esperaba completed" }

# ---------------------------------------------------------------------------
Step 4 'B: NON-INTERRUPTING - el handler corre en paralelo, el flujo sigue'
$waitB="ESPWAITB_$run"; $notifyB="ESPNOTIFYB_$run"
$mainB=New-Pd @{
    header=@{ code="esp_mainb_$run"; name='ESP main B'; description='tmp'; lifecycle='active' }
    flowElements=@(
        @{ code='start';  name='Start'; type='start_event';        sortOrder=1 }
        @{ code='ev_wait';name='Wait';  type='intermediate_event'; sortOrder=2; config=@{ signal=@{ signalCode=$waitB } } }
        @{ code='end_ok'; name='EndOk'; type='end_event';          sortOrder=3 }
        @{ code='esp';    name='Esp';   type='event_sub_process';  sortOrder=9; config=@{ eventSubProcess=@{ trigger='signal'; code=$notifyB; interrupting=$false }; callActivity=@{ calledProcessversionId="$($handler.processversionId)" } } }
    )
    sequenceFlows=@(
        @{ sourceCode='start';   targetCode='ev_wait'; sortOrder=1 }
        @{ sourceCode='ev_wait'; targetCode='end_ok';  sortOrder=2 }
    )
}
$ib=Start-Inst $mainB.processversionId
Start-Sleep -Milliseconds 400
Broadcast $notifyB
Start-Sleep -Milliseconds 700
$b=Get-Instance $ib.id
$etB=First-Audit $b 'event_subprocess.triggered'
if ($etB -and $etB.data.interrupting -eq $false) { OKMsg 'B: event_subprocess.triggered interrupting=false' } else { Fail "B: interrupting inesperado ($($etB.data.interrupting))" }
if ($b.lifecycle -eq 'active') { OKMsg 'B: instance SIGUE active (el handler no interrumpio el flujo)' } else { Fail "B: lifecycle=$($b.lifecycle), esperaba active" }
Broadcast $waitB
Start-Sleep -Milliseconds 500
$b2=Get-Instance $ib.id
if ($b2.lifecycle -eq 'completed') { OKMsg 'B: instance completed tras el signal del flujo principal' } else { Fail "B: lifecycle=$($b2.lifecycle), esperaba completed" }

# ---------------------------------------------------------------------------
Step 5 'Cleanup'
# incluir children (handlers) spawneados
foreach ($pdid in $createdPdIds){
    try { $insts=@(Invoke-RestMethod -Uri "$bpm/v1/bpm/processdef/$pdid/instances" -Headers $H); foreach($x in $insts){ $script:startedInstances+=$x.id } } catch {}
}
foreach ($iid in ($startedInstances | Select-Object -Unique)){ try { Invoke-RestMethod -Uri "$bpm/v1/bpm/instance/$iid`?force=true" -Method DELETE -Headers $H | Out-Null } catch {} }
foreach ($pdid in $createdPdIds){ try { Invoke-RestMethod -Uri "$bpm/v1/bpm/admin/processdef/$pdid" -Method DELETE -Headers $H | Out-Null } catch {} }
OKMsg 'cleanup done'

Write-Host ''
Write-Host "PASSES=$script:passes  FAILURES=$script:failures" -ForegroundColor Cyan
