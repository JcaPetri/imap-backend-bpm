# =============================================================================
# smoke_event_gateway.ps1 - Event-based gateway (carrera de eventos)
# =============================================================================
# Valida event_based_gateway: el token arma N ramas (intermediate_event
# timer/message/signal); la PRIMERA que dispara gana y las otras se cancelan.
#
# Topologia (ambos escenarios):
#   start -> egw(event_based_gateway) -> { ev_timer(timer) -> end_timeout
#                                          ev_msg(message PAY_OK) -> end_paid }
#
# Escenarios:
#   A) TIMER gana: timer corto (2s), NO se manda el message. El timer dispara,
#      cancela la rama message. Instance completa por end_timeout.
#   B) MESSAGE gana: timer largo (3600s), se manda el message. El message gana,
#      cancela (des-arma) el timer. Instance completa por end_paid.
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
function Branch-Cancelled($inst, $code) {
    return (@($inst.auditLog | Where-Object { $_.eventType -eq 'event_gateway.branch_cancelled' -and $_.data.elementCode -eq $code })).Count
}
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
function New-EgwPd($code, $timerSeconds) {
    return New-Pd @{
        header = @{ code=$code; name='Event gateway smoke'; description='tmp'; lifecycle='active' }
        flowElements = @(
            @{ code='start';       name='Start';    type='start_event';         sortOrder=1 }
            @{ code='egw';         name='Egw';      type='event_based_gateway';  sortOrder=2 }
            @{ code='ev_timer';    name='EvTimer';  type='intermediate_event';   sortOrder=3; config=@{ timer=@{ delaySeconds=$timerSeconds } } }
            @{ code='ev_msg';      name='EvMsg';    type='intermediate_event';   sortOrder=4; config=@{ message=@{ messageCode='PAY_OK' } } }
            @{ code='end_timeout'; name='EndTmo';   type='end_event';            sortOrder=5 }
            @{ code='end_paid';    name='EndPaid';  type='end_event';            sortOrder=6 }
        )
        sequenceFlows = @(
            @{ sourceCode='start';    targetCode='egw';         sortOrder=1 }
            @{ sourceCode='egw';      targetCode='ev_timer';    sortOrder=2 }
            @{ sourceCode='egw';      targetCode='ev_msg';      sortOrder=3 }
            @{ sourceCode='ev_timer'; targetCode='end_timeout'; sortOrder=4 }
            @{ sourceCode='ev_msg';   targetCode='end_paid';    sortOrder=5 }
        )
    }
}

# =============================================================================
# Escenario A: TIMER gana (no se manda el message)
# =============================================================================
Step 2 'Escenario A: timer gana (2s), cancela la rama message'
$pdA = New-EgwPd "mi_egw_a_$run" 2
OKMsg 'processdef A creado (validacion event_gateway acepto)'
$ia = Start-Inst $pdA.processversionId @{}
$armed = Get-Instance $ia.id
if ((Count-Audit $armed 'event_gateway.armed') -ge 1) { OKMsg 'A: event_gateway.armed' } else { Fail 'A: falta event_gateway.armed' }
Write-Host '     esperando el timer (worker poll ~5s)...'
Start-Sleep -Seconds 13
$a = Get-Instance $ia.id
if ((Count-Audit $a 'timer.fired') -ge 1) { OKMsg 'A: timer.fired (gano el timer)' } else { Fail 'A: el timer no disparo' }
if ((Branch-Cancelled $a 'ev_msg') -ge 1) { OKMsg 'A: rama message cancelada' } else { Fail 'A: no se cancelo la rama message' }
if ((Count-Audit $a 'event_gateway.resolved') -ge 1) { OKMsg 'A: event_gateway.resolved' } else { Fail 'A: falta event_gateway.resolved' }
if ($a.lifecycle -eq 'completed') { OKMsg 'A: instance completed via end_timeout' } else { Fail "A: lifecycle=$($a.lifecycle), esperaba completed" }

# =============================================================================
# Escenario B: MESSAGE gana (timer largo, se manda el message)
# =============================================================================
Step 3 'Escenario B: message gana (timer 3600s), cancela la rama timer'
$pdB = New-EgwPd "mi_egw_b_$run" 3600
OKMsg 'processdef B creado'
$ib = Start-Inst $pdB.processversionId @{}
$armedB = Get-Instance $ib.id
if ((Count-Audit $armedB 'event_gateway.armed') -ge 1) { OKMsg 'B: event_gateway.armed' } else { Fail 'B: falta event_gateway.armed' }
# mandar el message PAY_OK (correlationKey wildcard)
$corr = Invoke-RestMethod -Uri "$bpm/v1/bpm/messages/correlate" -Method POST -Headers $H `
    -Body (@{ messageCode='PAY_OK'; correlationKey='*'; payload=@{ paid=$true } } | ConvertTo-Json)
if ($corr.reactivatedTokens -ge 1) { OKMsg "B: message correlacionado (reactivated=$($corr.reactivatedTokens))" } else { Fail 'B: message no reactivo ningun token' }
$b = Get-Instance $ib.id
if ((Count-Audit $b 'message.received') -ge 1) { OKMsg 'B: message.received (gano el message)' } else { Fail 'B: falta message.received' }
if ((Branch-Cancelled $b 'ev_timer') -ge 1) { OKMsg 'B: rama timer cancelada (timer des-armado)' } else { Fail 'B: no se cancelo la rama timer' }
if ((Count-Audit $b 'event_gateway.resolved') -ge 1) { OKMsg 'B: event_gateway.resolved' } else { Fail 'B: falta event_gateway.resolved' }
if ($b.lifecycle -eq 'completed') { OKMsg 'B: instance completed via end_paid' } else { Fail "B: lifecycle=$($b.lifecycle), esperaba completed" }
if ((Count-Audit $b 'timer.fired') -eq 0) { OKMsg 'B: el timer NO disparo (fue des-armado)' } else { Fail 'B: el timer disparo pese a estar cancelado' }

# =============================================================================
# Cleanup
# =============================================================================
Step 4 'Cleanup'
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
