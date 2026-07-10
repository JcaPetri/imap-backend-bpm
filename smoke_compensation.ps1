# =============================================================================
# smoke_compensation.ps1 - Compensation / Saga (rollback de negocio, LIFO)
# =============================================================================
# Valida el patron Saga: cada service_task compensable declara un handler
# off-path (service_task con config.compensationFor='<code>'). Al completar OK
# se registra la compensacion; un end_event con config.compensate=true dispara
# la compensacion LIFO (inverso a la completacion) via ServiceTaskRunner.
#
# Escenarios:
#   A) COMPENSACION DIRECTA: start -> svc_a(comp) -> svc_b(comp) -> end_compensate
#      -> registra 2, compensa 2 en LIFO (comp_b antes que comp_a), instance completed
#   B) SAGA ON FAILURE (compone con feature 2): start -> svc_a -> svc_b ->
#      svc_fail(bpm.test.fail) + boundary error -> end_compensate
#      -> el fallo del paso 3 deshace los pasos 1-2 en LIFO
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
# Devuelve el orden cronologico (ascendente) de los handlerCode compensados.
function Comp-Order($inst) {
    $ex = @($inst.auditLog | Where-Object { $_.eventType -eq 'compensation.executed' })
    [array]::Reverse($ex)   # auditLog viene DESC; revertir a cronologico
    return @($ex | ForEach-Object { $_.data.handlerCode })
}

# Elementos compensables reusables
$comp_a = @{ code='comp_a'; name='CompA'; type='service_task'; sortOrder=90; config=@{ serviceCode='bpm.test.echo'; compensationFor='svc_a' } }
$comp_b = @{ code='comp_b'; name='CompB'; type='service_task'; sortOrder=91; config=@{ serviceCode='bpm.test.echo'; compensationFor='svc_b' } }

# =============================================================================
# Escenario A: compensacion directa (end_compensate alcanzado por flujo normal)
# =============================================================================
Step 2 'Escenario A: start -> svc_a -> svc_b -> end_compensate (LIFO)'
$pdA = New-Pd @{
    header = @{ code="mi_saga_a_$run"; name='Saga A'; description='tmp'; lifecycle='active' }
    flowElements = @(
        @{ code='start';    name='Start';   type='start_event';  sortOrder=1 }
        @{ code='svc_a';    name='SvcA';    type='service_task'; sortOrder=2; config=@{ serviceCode='bpm.test.echo' } }
        @{ code='svc_b';    name='SvcB';    type='service_task'; sortOrder=3; config=@{ serviceCode='bpm.test.echo' } }
        @{ code='end_comp'; name='EndComp'; type='end_event';    sortOrder=4; config=@{ compensate=$true } }
        $comp_a
        $comp_b
    )
    sequenceFlows = @(
        @{ sourceCode='start'; targetCode='svc_a';    sortOrder=1 }
        @{ sourceCode='svc_a'; targetCode='svc_b';    sortOrder=2 }
        @{ sourceCode='svc_b'; targetCode='end_comp'; sortOrder=3 }
    )
}
OKMsg 'processdef A creado (reachability acepto los handlers off-path)'
$ia = Start-Inst $pdA.processversionId @{}
$a = Get-Instance $ia.id
if ((Count-Audit $a 'compensation.registered') -eq 2) { OKMsg 'A: 2 compensaciones registradas' } else { Fail "A: registered=$(Count-Audit $a 'compensation.registered'), esperaba 2" }
if ((Count-Audit $a 'compensation.triggered') -ge 1) { OKMsg 'A: compensation.triggered' } else { Fail 'A: falta compensation.triggered' }
if ((Count-Audit $a 'compensation.executed') -eq 2) { OKMsg 'A: 2 compensaciones ejecutadas' } else { Fail "A: executed=$(Count-Audit $a 'compensation.executed'), esperaba 2" }
$ord = Comp-Order $a
if ($ord.Count -eq 2 -and $ord[0] -eq 'comp_b' -and $ord[1] -eq 'comp_a') { OKMsg "A: orden LIFO correcto [$($ord -join ', ')]" } else { Fail "A: orden LIFO incorrecto [$($ord -join ', ')]" }
if ($a.lifecycle -eq 'completed') { OKMsg 'A: instance completed' } else { Fail "A: lifecycle=$($a.lifecycle), esperaba completed" }

# =============================================================================
# Escenario B: saga on failure (compone con boundary error del feature 2)
# =============================================================================
Step 3 'Escenario B: svc_fail dispara boundary -> end_compensate deshace 1-2'
$pdB = New-Pd @{
    header = @{ code="mi_saga_b_$run"; name='Saga B'; description='tmp'; lifecycle='active' }
    flowElements = @(
        @{ code='start';    name='Start';   type='start_event';   sortOrder=1 }
        @{ code='svc_a';    name='SvcA';    type='service_task';  sortOrder=2; config=@{ serviceCode='bpm.test.echo' } }
        @{ code='svc_b';    name='SvcB';    type='service_task';  sortOrder=3; config=@{ serviceCode='bpm.test.echo' } }
        @{ code='svc_fail'; name='SvcFail'; type='service_task';  sortOrder=4; config=@{ serviceCode='bpm.test.fail' } }
        @{ code='end_ok';   name='EndOk';   type='end_event';     sortOrder=5 }
        @{ code='bnd';      name='Bnd';     type='boundary_event'; sortOrder=6; config=@{ boundary=@{ attachedTo='svc_fail'; interrupting=$true }; error=@{ errorCode='*' } } }
        @{ code='end_comp'; name='EndComp'; type='end_event';     sortOrder=7; config=@{ compensate=$true } }
        $comp_a
        $comp_b
    )
    sequenceFlows = @(
        @{ sourceCode='start';    targetCode='svc_a';    sortOrder=1 }
        @{ sourceCode='svc_a';    targetCode='svc_b';    sortOrder=2 }
        @{ sourceCode='svc_b';    targetCode='svc_fail'; sortOrder=3 }
        @{ sourceCode='svc_fail'; targetCode='end_ok';   sortOrder=4 }
        @{ sourceCode='bnd';      targetCode='end_comp'; sortOrder=5 }
    )
}
OKMsg 'processdef B creado'
$ib = Start-Inst $pdB.processversionId @{}
$b = Get-Instance $ib.id
if ((Count-Audit $b 'compensation.registered') -eq 2) { OKMsg 'B: 2 compensaciones registradas (svc_a, svc_b)' } else { Fail "B: registered=$(Count-Audit $b 'compensation.registered'), esperaba 2" }
if ((Count-Audit $b 'boundary.error.fired') -ge 1) { OKMsg 'B: svc_fail disparo el boundary error' } else { Fail 'B: no se disparo boundary.error.fired' }
if ((Count-Audit $b 'compensation.executed') -eq 2) { OKMsg 'B: 2 compensaciones ejecutadas' } else { Fail "B: executed=$(Count-Audit $b 'compensation.executed'), esperaba 2" }
$ordB = Comp-Order $b
if ($ordB.Count -eq 2 -and $ordB[0] -eq 'comp_b' -and $ordB[1] -eq 'comp_a') { OKMsg "B: orden LIFO correcto [$($ordB -join ', ')]" } else { Fail "B: orden LIFO incorrecto [$($ordB -join ', ')]" }
if ((Count-Audit $b 'service_task.failed') -eq 0) { OKMsg 'B: NO service_task.failed (boundary lo capturo)' } else { Fail 'B: hubo service_task.failed' }
if ($b.lifecycle -eq 'completed') { OKMsg 'B: instance completed via end_compensate' } else { Fail "B: lifecycle=$($b.lifecycle), esperaba completed" }

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
