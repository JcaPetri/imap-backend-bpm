# =============================================================================
# smoke_event_subprocess_msgtimer.ps1 - Ola 6.1: event sub-process message + timer
# =============================================================================
#   A) MESSAGE: event_sub_process(trigger=message, code, correlationKey). Un
#      correlate con la key CORRECTA lo dispara; con key incorrecta NO (routing).
#   B) TIMER: event_sub_process(trigger=timer, delaySeconds). El job dispara el
#      handler tras el delay (interrumpe el flujo principal).
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
function Correlate($code,$key){ Invoke-RestMethod -Uri "$bpm/v1/bpm/messages/correlate" -Method POST -Headers $H -Body (@{ messageCode=$code; correlationKey=$key }|ConvertTo-Json) | Out-Null }

Step 2 'Handler def (start -> svc echo -> end)'
$handler=New-Pd @{
    header=@{ code="espmt_handler_$run"; name='ESPmt handler'; description='tmp'; lifecycle='active' }
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
OKMsg "handler ver=$($handler.processversionId)"

# ---------------------------------------------------------------------------
Step 3 'A: MESSAGE - key incorrecta NO dispara, key correcta SI'
$msgA="ESPMSG_$run"; $keyA="KEY_$run"
$mainA=New-Pd @{
    header=@{ code="espmt_maina_$run"; name='ESPmt main A'; description='tmp'; lifecycle='active' }
    flowElements=@(
        @{ code='start';  name='Start'; type='start_event';        sortOrder=1 }
        @{ code='ev_wait';name='Wait';  type='intermediate_event'; sortOrder=2; config=@{ signal=@{ signalCode="GOA_$run" } } }
        @{ code='end_ok'; name='EndOk'; type='end_event';          sortOrder=3 }
        @{ code='esp';    name='Esp';   type='event_sub_process';  sortOrder=9; config=@{ eventSubProcess=@{ trigger='message'; code=$msgA; correlationKey=$keyA; interrupting=$true }; callActivity=@{ calledProcessversionId="$($handler.processversionId)" } } }
    )
    sequenceFlows=@(
        @{ sourceCode='start';   targetCode='ev_wait'; sortOrder=1 }
        @{ sourceCode='ev_wait'; targetCode='end_ok';  sortOrder=2 }
    )
}
$ia=Start-Inst $mainA.processversionId
Start-Sleep -Milliseconds 400
$a0=Get-Instance $ia.id
$sub=First-Audit $a0 'event_subprocess.subscribed'
if ($sub -and $sub.data.correlationKey -eq $keyA) { OKMsg "A: suscrito con correlationKey=$keyA" } else { Fail "A: correlationKey mal (fue $($sub.data.correlationKey))" }
# key incorrecta -> NO dispara
Correlate $msgA "WRONG_$run"
Start-Sleep -Milliseconds 500
$a1=Get-Instance $ia.id
if ((Count-Audit $a1 'event_subprocess.triggered') -eq 0 -and $a1.lifecycle -eq 'active') { OKMsg 'A: key incorrecta NO disparo (routing correcto)' } else { Fail 'A: la key incorrecta disparo el handler!' }
# key correcta -> dispara + completa
Correlate $msgA $keyA
Start-Sleep -Milliseconds 700
$a=Get-Instance $ia.id
if ((Count-Audit $a 'event_subprocess.triggered') -ge 1) { OKMsg 'A: key correcta disparo el handler' } else { Fail 'A: la key correcta NO disparo' }
if ($a.lifecycle -eq 'completed') { OKMsg 'A: instance completed (interrupting via message)' } else { Fail "A: lifecycle=$($a.lifecycle), esperaba completed" }

# ---------------------------------------------------------------------------
Step 4 'B: TIMER - el handler dispara tras el delay (SLA/timeout)'
$mainB=New-Pd @{
    header=@{ code="espmt_mainb_$run"; name='ESPmt main B'; description='tmp'; lifecycle='active' }
    flowElements=@(
        @{ code='start';  name='Start'; type='start_event';        sortOrder=1 }
        @{ code='ev_wait';name='Wait';  type='intermediate_event'; sortOrder=2; config=@{ signal=@{ signalCode="GOB_$run" } } }
        @{ code='end_ok'; name='EndOk'; type='end_event';          sortOrder=3 }
        @{ code='esp';    name='Esp';   type='event_sub_process';  sortOrder=9; config=@{ eventSubProcess=@{ trigger='timer'; delaySeconds=2; interrupting=$true }; callActivity=@{ calledProcessversionId="$($handler.processversionId)" } } }
    )
    sequenceFlows=@(
        @{ sourceCode='start';   targetCode='ev_wait'; sortOrder=1 }
        @{ sourceCode='ev_wait'; targetCode='end_ok';  sortOrder=2 }
    )
}
$ib=Start-Inst $mainB.processversionId
Start-Sleep -Milliseconds 400
$b0=Get-Instance $ib.id
if ((Count-Audit $b0 'event_subprocess.timer_scheduled') -ge 1) { OKMsg 'B: timer job agendado al arrancar' } else { Fail 'B: no se agendo el timer' }
if ($b0.lifecycle -eq 'active') { OKMsg 'B: instance esperando (active)' } else { Fail "B: lifecycle=$($b0.lifecycle)" }
Write-Host '     esperando el disparo del timer (worker escanea cada 5s)...' -ForegroundColor DarkGray
Start-Sleep -Seconds 11
$b=Get-Instance $ib.id
$etB=First-Audit $b 'event_subprocess.triggered'
if ($etB) { OKMsg 'B: event_subprocess.triggered por el timer' } else { Fail 'B: el timer no disparo el handler' }
if ($etB -and $etB.data.trigger -eq 'timer') { OKMsg 'B: trigger=timer' } else { Fail "B: trigger inesperado ($($etB.data.trigger))" }
if ($b.lifecycle -eq 'completed') { OKMsg 'B: instance completed tras el timer (interrumpio+corrio handler)' } else { Fail "B: lifecycle=$($b.lifecycle), esperaba completed" }

# ---------------------------------------------------------------------------
Step 5 'Cleanup'
foreach ($pdid in $createdPdIds){
    try { $insts=@(Invoke-RestMethod -Uri "$bpm/v1/bpm/processdef/$pdid/instances" -Headers $H); foreach($x in $insts){ $script:startedInstances+=$x.id } } catch {}
}
foreach ($iid in ($startedInstances | Select-Object -Unique)){ try { Invoke-RestMethod -Uri "$bpm/v1/bpm/instance/$iid`?force=true" -Method DELETE -Headers $H | Out-Null } catch {} }
foreach ($pdid in $createdPdIds){ try { Invoke-RestMethod -Uri "$bpm/v1/bpm/admin/processdef/$pdid" -Method DELETE -Headers $H | Out-Null } catch {} }
OKMsg 'cleanup done'

Write-Host ''
Write-Host "PASSES=$script:passes  FAILURES=$script:failures" -ForegroundColor Cyan
