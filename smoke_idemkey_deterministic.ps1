# =============================================================================
# smoke_idemkey_deterministic.ps1 - 4.3b D-lite: idempotency-key determinística
# =============================================================================
# La idempotency-key ahora se deriva de (tokenId, elementId, encarnación) donde
# encarnación = # de incidents ya abiertos para ese (token, elemento). Propiedades:
#   - ESTABLE ante crash/restart de bpm (un crash no abre incident -> misma key ->
#     el receptor deduplica -> no doble efecto). [no smoke-able sin reiniciar bpm]
#   - FRESCA en incident-retry intencional (el fallo abrió incident -> encarnación+1
#     -> key nueva -> el receptor re-ejecuta). [ESTO es lo que validamos aca]
#
# Valida:
#   A) el service_task genera una idempotency-key con formato UUID.
#   B) un incident-retry re-dispatcha con una key DISTINTA (encarnación fresca) ->
#      el receptor NO la deduplica -> el retry efectivamente re-ejecuta.
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
function Invoked-Keys($inst){
    return @($inst.auditLog | Where-Object { $_.eventType -eq 'service_task.invoked' } | ForEach-Object { [string]$_.data.idempotencyKey })
}
function Open-Incidents($iid){
    $all=Invoke-RestMethod -Uri "$bpm/v1/bpm/incidents?lifecycle=open" -Headers $H
    return ,@($all | Where-Object { $_.processinstanceId -eq $iid })
}

Step 2 'Create failing processdef (bpm.test.fail, sin boundary -> incidente)'
$body=@{
    header=@{ code="idemdet_$run"; name='Idem det smoke'; description='tmp'; lifecycle='active' }
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
$pd=Invoke-RestMethod -Uri "$bpm/v1/bpm/admin/processdef" -Method POST -Headers $H -Body $body
$script:createdPdIds+=$pd.processdefId
OKMsg 'processdef creado'

Step 3 'A: dispatch original -> key con formato UUID'
$ia=Invoke-RestMethod -Uri "$bpm/v1/bpm/process/$($pd.processversionId)/start" -Method POST -Headers $H -Body '{}'
$script:startedInstances+=$ia.id
Start-Sleep -Milliseconds 500
$a=Get-Instance $ia.id
$keys1=Invoked-Keys $a
$key1=$keys1[0]
$uuidRe='^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
if ($key1 -match $uuidRe) { OKMsg "A: idempotencyKey formato UUID ($($key1.Substring(0,8))...)" } else { Fail "A: key mal formada ('$key1')" }

Step 4 'B: incident-retry -> key DISTINTA (encarnacion fresca -> re-ejecuta)'
$incs=Open-Incidents $ia.id
if ($incs.Count -ge 1) { OKMsg 'B: incidente inicial abierto' } else { Fail 'B: no abrio incidente' }
Invoke-RestMethod -Uri "$bpm/v1/bpm/incident/$($incs[0].id)/retry" -Method POST -Headers $H | Out-Null
Start-Sleep -Milliseconds 600
$b=Get-Instance $ia.id
$keys2=Invoked-Keys $b
if ($keys2.Count -ge 2) { OKMsg "B: service_task re-invocado ($($keys2.Count) invokes)" } else { Fail "B: invokes=$($keys2.Count), esperaba >=2" }
$key2=$keys2[1]
if ($key2 -match $uuidRe) { OKMsg "B: key del retry formato UUID ($($key2.Substring(0,8))...)" } else { Fail "B: key retry mal formada ('$key2')" }
if ($key1 -ne $key2) { OKMsg 'B: key del retry DISTINTA a la original (encarnacion fresca -> el receptor NO dedupea el retry)' } else { Fail 'B: la key del retry es IGUAL -> el receptor lo dedupearia (retry no re-ejecutaria!)' }

Step 5 'Cleanup'
foreach ($iid in $startedInstances){ try { Invoke-RestMethod -Uri "$bpm/v1/bpm/instance/$iid`?force=true" -Method DELETE -Headers $H | Out-Null } catch {} }
foreach ($pdid in $createdPdIds){ try { Invoke-RestMethod -Uri "$bpm/v1/bpm/admin/processdef/$pdid" -Method DELETE -Headers $H | Out-Null } catch {} }
OKMsg 'cleanup done'

Write-Host ''
Write-Host "PASSES=$script:passes  FAILURES=$script:failures" -ForegroundColor Cyan
