# =============================================================================
# smoke_service_task_boundary.ps1 - Boundary error auto desde service_task
# =============================================================================
# Valida que una falla de service_task dispara automaticamente el boundary error
# adjunto (en vez de dejar el token 'failed' sin recuperacion).
#
# Escenarios:
#   A) boundaryErrorCode EXPLICITO (bpm.test.error_boundary, errorCode=INSUFFICIENT_STOCK)
#      + boundary error.errorCode=INSUFFICIENT_STOCK (exact match)
#      -> boundary.error.fired + instance completed via end_err, NO service_task.failed
#   B) FAILURE PELADO (bpm.test.fail -> errorCode=TEST_FAILURE, sin boundaryErrorCode)
#      + boundary error.errorCode='*' (catch-all)   <- LA NOVEDAD del #2
#      -> boundary.error.fired + instance completed via end_err, NO service_task.failed
#   C) CONTROL: bpm.test.fail SIN boundary
#      -> service_task.failed + token failed + instance NO completed (backward compat)
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
function First-Audit($inst, $evt) { return @($inst.auditLog | Where-Object { $_.eventType -eq $evt })[0] }

# Crea un processdef y devuelve la respuesta (processdefId/processversionId).
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

# =============================================================================
# Escenario A: boundaryErrorCode explicito, exact match
# =============================================================================
Step 2 'Escenario A: service_task boundaryErrorCode explicito (exact match)'
$pdA = New-Pd @{
    header = @{ code="mi_svcbnd_a_$run"; name='STBnd A'; description='tmp'; lifecycle='active' }
    flowElements = @(
        @{ code='start';   name='Start';  type='start_event';   sortOrder=1 }
        @{ code='svc';     name='Svc';    type='service_task';   sortOrder=2; config=@{ serviceCode='bpm.test.error_boundary'; errorCode='INSUFFICIENT_STOCK' } }
        @{ code='end_ok';  name='EndOk';  type='end_event';      sortOrder=3 }
        @{ code='bnd';     name='Bnd';    type='boundary_event'; sortOrder=4; config=@{ boundary=@{ attachedTo='svc'; interrupting=$true }; error=@{ errorCode='INSUFFICIENT_STOCK' } } }
        @{ code='end_err'; name='EndErr'; type='end_event';      sortOrder=5 }
    )
    sequenceFlows = @(
        @{ sourceCode='start'; targetCode='svc';     sortOrder=1 }
        @{ sourceCode='svc';   targetCode='end_ok';  sortOrder=2 }
        @{ sourceCode='bnd';   targetCode='end_err'; sortOrder=3 }
    )
}
OKMsg "processdef A creado (reachability acepto el boundary)"
$ia = Start-Inst $pdA.processversionId @{}
$a = Get-Instance $ia.id
if ((Count-Audit $a 'boundary.error.fired') -ge 1) { OKMsg 'A: boundary.error.fired presente' } else { Fail 'A: falta boundary.error.fired' }
$bef = First-Audit $a 'boundary.error.fired'
if ($bef -and $bef.data.source -eq 'service_task') { OKMsg 'A: source=service_task' } else { Fail "A: source != service_task (fue $($bef.data.source))" }
if ((Count-Audit $a 'service_task.failed') -eq 0) { OKMsg 'A: NO service_task.failed (boundary lo capturo)' } else { Fail 'A: hubo service_task.failed' }
if ($a.lifecycle -eq 'completed') { OKMsg 'A: instance completed via rama de error' } else { Fail "A: lifecycle=$($a.lifecycle), esperaba completed" }

# =============================================================================
# Escenario B: failure pelado + boundary catch-all (LA NOVEDAD)
# =============================================================================
Step 3 'Escenario B: failure pelado (bpm.test.fail) + boundary catch-all *'
$pdB = New-Pd @{
    header = @{ code="mi_svcbnd_b_$run"; name='STBnd B'; description='tmp'; lifecycle='active' }
    flowElements = @(
        @{ code='start';   name='Start';  type='start_event';   sortOrder=1 }
        @{ code='svc';     name='Svc';    type='service_task';   sortOrder=2; config=@{ serviceCode='bpm.test.fail'; errorCode='TEST_FAILURE' } }
        @{ code='end_ok';  name='EndOk';  type='end_event';      sortOrder=3 }
        @{ code='bnd';     name='Bnd';    type='boundary_event'; sortOrder=4; config=@{ boundary=@{ attachedTo='svc'; interrupting=$true }; error=@{ errorCode='*' } } }
        @{ code='end_err'; name='EndErr'; type='end_event';      sortOrder=5 }
    )
    sequenceFlows = @(
        @{ sourceCode='start'; targetCode='svc';     sortOrder=1 }
        @{ sourceCode='svc';   targetCode='end_ok';  sortOrder=2 }
        @{ sourceCode='bnd';   targetCode='end_err'; sortOrder=3 }
    )
}
OKMsg 'processdef B creado'
$ib = Start-Inst $pdB.processversionId @{}
$b = Get-Instance $ib.id
if ((Count-Audit $b 'boundary.error.fired') -ge 1) { OKMsg 'B: boundary.error.fired presente (catch-all capturo failure pelado)' } else { Fail 'B: falta boundary.error.fired' }
$befB = First-Audit $b 'boundary.error.fired'
if ($befB -and $befB.data.errorCode -eq 'TEST_FAILURE') { OKMsg 'B: errorCode=TEST_FAILURE propagado al boundary' } else { Fail "B: errorCode inesperado (fue $($befB.data.errorCode))" }
if ((Count-Audit $b 'service_task.failed') -eq 0) { OKMsg 'B: NO service_task.failed' } else { Fail 'B: hubo service_task.failed' }
if ($b.lifecycle -eq 'completed') { OKMsg 'B: instance completed via rama de error' } else { Fail "B: lifecycle=$($b.lifecycle), esperaba completed" }

# =============================================================================
# Escenario C: control sin boundary -> falla como antes (backward compat)
# =============================================================================
Step 4 'Escenario C: control sin boundary (backward compat)'
$pdC = New-Pd @{
    header = @{ code="mi_svcbnd_c_$run"; name='STBnd C'; description='tmp'; lifecycle='active' }
    flowElements = @(
        @{ code='start';  name='Start'; type='start_event'; sortOrder=1 }
        @{ code='svc';    name='Svc';   type='service_task'; sortOrder=2; config=@{ serviceCode='bpm.test.fail' } }
        @{ code='end_ok'; name='EndOk'; type='end_event';   sortOrder=3 }
    )
    sequenceFlows = @(
        @{ sourceCode='start'; targetCode='svc';    sortOrder=1 }
        @{ sourceCode='svc';   targetCode='end_ok'; sortOrder=2 }
    )
}
OKMsg 'processdef C creado'
$ic = Start-Inst $pdC.processversionId @{}
$c = Get-Instance $ic.id
if ((Count-Audit $c 'service_task.failed') -ge 1) { OKMsg 'C: service_task.failed presente' } else { Fail 'C: falta service_task.failed' }
if ((Count-Audit $c 'boundary.error.fired') -eq 0) { OKMsg 'C: NO boundary.error.fired (no hay boundary)' } else { Fail 'C: hubo boundary.error.fired inesperado' }
if ($c.lifecycle -ne 'completed') { OKMsg "C: instance NO completed (lifecycle=$($c.lifecycle), token failed)" } else { Fail 'C: instance completed, esperaba que NO' }

# =============================================================================
# Cleanup
# =============================================================================
Step 5 'Cleanup'
foreach ($iid in $startedInstances) {
    try { Invoke-RestMethod -Uri "$bpm/v1/bpm/instance/$iid`?force=true" -Method DELETE -Headers $H | Out-Null } catch {}
}
foreach ($pid in $createdPdIds) {
    try { Invoke-RestMethod -Uri "$bpm/v1/bpm/admin/processdef/$pid" -Method DELETE -Headers $H | Out-Null } catch {}
}
OKMsg 'cleanup done'

Write-Host ''
Write-Host "PASSES=$script:passes  FAILURES=$script:failures" -ForegroundColor Cyan
if ($script:failures -gt 0) { exit 1 } else { exit 0 }
