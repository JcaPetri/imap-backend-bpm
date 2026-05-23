# =============================================================================
# smoke_service_task_registry.ps1 - Fase 0.B.1 BPM ServiceTaskRegistry
# =============================================================================
# Valida que el motor BPM ya NO ejecuta service_task como "log + advance"
# sino que invoca handlers reales via ServiceTaskRegistry.
#
# Cubre:
#   1. Login
#   2. Create processdef "smoke_st_echo": start -> service_task(bpm.test.echo) -> end
#   3. Start instance + verify completed + audit service_task.completed con resultVarsCount=3
#   4. Create processdef "smoke_st_fail": start -> service_task(bpm.test.fail) -> end
#   5. Start instance + verify after retries token=failed + audit service_task.failed
#   6. Create processdef "smoke_st_delay": start -> service_task(bpm.test.delay 200ms) -> end
#   7. Start instance + verify completed + actualElapsedMs >= 200
#   8. Verify ServiceTaskExecuteController endpoint /v1/service-tasks/execute responde OK
#   9. Cleanup
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
    $sys = 'https://imaps.com.ar/imap/system'
    $bpm = 'https://imaps.com.ar/imap/bpm'
} else {
    $iam = 'http://localhost:8091/imap/iam'
    $sys = 'http://localhost:8092/imap/system'
    $bpm = 'http://localhost:8093/imap/bpm'
}

if (-not $Email -or -not $Password -or -not $TenantId) {
    Write-Error 'Faltan creds IMAP_EMAIL / IMAP_PASSWORD / IMAP_TENANT'
    exit 1
}

function Fail($m) { Write-Host "FAIL: $m" -ForegroundColor Red; $script:failures++ }
function OKMsg($m) { Write-Host "OK   $m" -ForegroundColor Green; $script:passes++ }
function Step($n, $m) { Write-Host ""; Write-Host "[$n] $m" -ForegroundColor Cyan }
$script:passes = 0
$script:failures = 0

# --- Login ---
Step 1 "Login"
$tok = (Invoke-RestMethod -Uri "$iam/v1/auth/login" -Method POST -ContentType 'application/json' `
    -Body (@{ email=$Email; password=$Password } | ConvertTo-Json)).accessToken
$Hsys = @{ Authorization="Bearer $tok"; 'X-Tenant-Id'='00000000-0000-0000-0000-000000000001'; 'Content-Type'='application/json' }
$Hbpm = @{ Authorization="Bearer $tok"; 'X-Tenant-Id'=$TenantId; 'Content-Type'='application/json' }
OKMsg "logged in"

# ─── Helper: crear processdef simple start -> service_task(<code>) -> end ───
function New-StProcessdef($pdCode, $serviceCode, $extraConfig) {
    $stCfg = @{ serviceCode = $serviceCode }
    if ($extraConfig) { foreach ($k in $extraConfig.Keys) { $stCfg[$k] = $extraConfig[$k] } }
    $body = @{
        header = @{ code=$pdCode; name="Smoke ST $serviceCode"; description='tmp'; lifecycle='active' }
        flowElements = @(
            @{ code='start'; name='Start'; type='start_event'; sortOrder=1 }
            @{ code='svc';   name='Service'; type='service_task'; sortOrder=2; config=$stCfg }
            @{ code='end';   name='End';   type='end_event';   sortOrder=3 }
        )
        sequenceFlows = @(
            @{ sourceCode='start'; targetCode='svc'; sortOrder=1 }
            @{ sourceCode='svc';   targetCode='end'; sortOrder=2 }
        )
    } | ConvertTo-Json -Depth 10
    return Invoke-RestMethod -Uri "$sys/v1/admin/bpm/processdef" -Method POST -Headers $Hsys -Body $body
}

# ─── Test 1: ECHO ───────────────────────────────────────────────────────────
Step 2 "Create processdef + run service_task bpm.test.echo"
$ts = Get-Date -Format yyyyMMddHHmmss
$pdEcho = New-StProcessdef "smoke_st_echo_$ts" 'bpm.test.echo' $null
OKMsg "processdef created pdId=$($pdEcho.processdefId)"

Step 3 "Start instance + verify completed + echo"
$inst = Invoke-RestMethod -Uri "$bpm/v1/bpm/process/$($pdEcho.processversionId)/start" -Method POST -Headers $Hbpm `
    -Body (@{ message='hello smoke' } | ConvertTo-Json)
Write-Host "  instance=$($inst.id) lifecycle=$($inst.lifecycle)"
Start-Sleep -Milliseconds 500   # the engine is sync but give it a moment
$instAfter = Invoke-RestMethod -Uri "$bpm/v1/bpm/instance/$($inst.id)" -Headers $Hbpm
if ($instAfter.lifecycle -eq 'completed') {
    OKMsg "instance completed"
} else {
    Fail "instance lifecycle=$($instAfter.lifecycle), expected 'completed'"
}
$invokedEvt   = $instAfter.auditLog | Where-Object eventType -eq 'service_task.invoked'   | Select-Object -First 1
$completedEvt = $instAfter.auditLog | Where-Object eventType -eq 'service_task.completed' | Select-Object -First 1
if ($invokedEvt -and $completedEvt) {
    OKMsg "audit log: service_task.invoked + service_task.completed present"
} else {
    Fail "missing audit events: invoked=$($invokedEvt -ne $null), completed=$($completedEvt -ne $null)"
}
# Verificar resultVarsCount >= 3 (echo, echoedBy, echoedAt)
$resultCount = $completedEvt.data.resultVarsCount
if ($resultCount -ge 3) {
    OKMsg "echo handler returned $resultCount result variables"
} else {
    Fail "echo handler returned $resultCount variables, expected >= 3"
}

# ─── Test 2: FAIL (with retries) ────────────────────────────────────────────
Step 4 "Create processdef + run service_task bpm.test.fail (will retry 3 times)"
$pdFail = New-StProcessdef "smoke_st_fail_$ts" 'bpm.test.fail' $null
OKMsg "processdef created pdId=$($pdFail.processdefId)"

Step 5 "Start instance + verify token failed after retries (~6s total: backoff 1s+5s)"
$startMs = (Get-Date).Ticks / 10000
$instFail = Invoke-RestMethod -Uri "$bpm/v1/bpm/process/$($pdFail.processversionId)/start" -Method POST -Headers $Hbpm -Body '{}'
$elapsedMs = ((Get-Date).Ticks / 10000) - $startMs
Write-Host "  instance=$($instFail.id) lifecycle=$($instFail.lifecycle) elapsedMs=$([int]$elapsedMs)"
Start-Sleep -Milliseconds 500
$instFailAfter = Invoke-RestMethod -Uri "$bpm/v1/bpm/instance/$($instFail.id)" -Headers $Hbpm
$failedEvt = $instFailAfter.auditLog | Where-Object eventType -eq 'service_task.failed' | Select-Object -First 1
if ($failedEvt) {
    OKMsg "audit log: service_task.failed present (errorCode=$($failedEvt.data.errorCode))"
} else {
    Fail "no service_task.failed audit event found"
}
# Verify backoff actually happened (retries took at least 6 seconds with default 1s + 5s)
if ($elapsedMs -ge 5500) {
    OKMsg "retry backoff respected ($([int]$elapsedMs)ms >= 5500ms)"
} else {
    Fail "retry backoff too short ($([int]$elapsedMs)ms < 5500ms, ¿retries no happened?)"
}

# ─── Test 3: DELAY ──────────────────────────────────────────────────────────
Step 6 "Create processdef + run service_task bpm.test.delay (200ms)"
$pdDelay = New-StProcessdef "smoke_st_delay_$ts" 'bpm.test.delay' @{ delayMs = 200 }
OKMsg "processdef created pdId=$($pdDelay.processdefId)"

Step 7 "Start instance + verify completed within reasonable time"
$start2 = (Get-Date).Ticks / 10000
$instDelay = Invoke-RestMethod -Uri "$bpm/v1/bpm/process/$($pdDelay.processversionId)/start" -Method POST -Headers $Hbpm -Body '{}'
$elapsed2 = ((Get-Date).Ticks / 10000) - $start2
Write-Host "  instance=$($instDelay.id) lifecycle=$($instDelay.lifecycle) elapsedMs=$([int]$elapsed2)"
$instDelayAfter = Invoke-RestMethod -Uri "$bpm/v1/bpm/instance/$($instDelay.id)" -Headers $Hbpm
if ($instDelayAfter.lifecycle -eq 'completed' -and $elapsed2 -ge 200 -and $elapsed2 -lt 3000) {
    OKMsg "delay handler completed in $([int]$elapsed2)ms (>=200ms, <3000ms)"
} else {
    Fail "delay handler: lifecycle=$($instDelayAfter.lifecycle), elapsedMs=$([int]$elapsed2)"
}

# ─── Test 4: /v1/service-tasks/execute endpoint ─────────────────────────────
Step 8 "Verify ServiceTaskExecuteController endpoint"
$execBody = @{ serviceCode='bpm.test.echo'; variables=@{ message='hello from REST' } } | ConvertTo-Json
$execResp = Invoke-RestMethod -Uri "$bpm/v1/service-tasks/execute" -Method POST -Headers $Hbpm -Body $execBody
if ($execResp.status -eq 'SUCCESS' -and $execResp.resultVariables.echo -eq 'hello from REST') {
    OKMsg "endpoint returns SUCCESS + echo='hello from REST'"
} else {
    Fail "endpoint response: status=$($execResp.status), echo=$($execResp.resultVariables.echo)"
}

# ─── Cleanup ────────────────────────────────────────────────────────────────
Step 9 "Cleanup: cascade delete instances + soft-delete processdefs"
foreach ($i in @($inst.id, $instFail.id, $instDelay.id)) {
    try { Invoke-RestMethod -Uri "$bpm/v1/bpm/instance/$i`?force=true" -Method DELETE -Headers $Hbpm | Out-Null } catch {}
}
foreach ($pd in @($pdEcho.processdefId, $pdFail.processdefId, $pdDelay.processdefId)) {
    try { Invoke-RestMethod -Uri "$sys/v1/admin/bpm/processdef/$pd" -Method DELETE -Headers $Hsys | Out-Null } catch {}
}
OKMsg "cleanup attempted"

# ─── Summary ────────────────────────────────────────────────────────────────
Write-Host ""
$total = $script:passes + $script:failures
if ($script:failures -eq 0) {
    Write-Host "=== ALL $script:passes/$total CHECKS PASSED ===" -ForegroundColor Green
    exit 0
} else {
    Write-Host "=== $script:passes/$total PASSED, $script:failures FAILED ===" -ForegroundColor Red
    exit 1
}
