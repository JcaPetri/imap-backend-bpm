# =============================================================================
# smoke_idempotency_key.ps1 - 4.3 tier liviano: idempotency-key
# =============================================================================
# Valida que cada invocacion de service_task genera una idempotency-key (que
# viaja en el body + header 'Idempotency-Key' del dispatch remoto, para que el
# receptor deduplique). Se verifica via el audit 'service_task.invoked'.
#   A) la key esta presente y no vacia.
#   B) es UNICA por invocacion (dos instancias -> keys distintas).
#
# ASCII puro. Self-cleanup al final.
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
function Invoked-Key($inst){
    $ev=@($inst.auditLog | Where-Object { $_.eventType -eq 'service_task.invoked' })[0]
    if ($ev){ return [string]$ev.data.idempotencyKey } else { return '' }
}

Step 2 'Create processdef (start -> svc echo -> end)'
$body=@{
    header=@{ code="idemkey_$run"; name='Idem key smoke'; description='tmp'; lifecycle='active' }
    flowElements=@(
        @{ code='start'; name='Start'; type='start_event';  sortOrder=1 }
        @{ code='svc';   name='Svc';   type='service_task'; sortOrder=2; config=@{ serviceCode='bpm.test.echo' } }
        @{ code='end';   name='End';   type='end_event';    sortOrder=3 }
    )
    sequenceFlows=@(
        @{ sourceCode='start'; targetCode='svc'; sortOrder=1 }
        @{ sourceCode='svc';   targetCode='end'; sortOrder=2 }
    )
} | ConvertTo-Json -Depth 12
$pd=Invoke-RestMethod -Uri "$bpm/v1/bpm/admin/processdef" -Method POST -Headers $H -Body $body
$createdPdIds+=$pd.processdefId
OKMsg 'processdef creado'

Step 3 'A: la invocacion genera una idempotency-key'
$ia=Invoke-RestMethod -Uri "$bpm/v1/bpm/process/$($pd.processversionId)/start" -Method POST -Headers $H -Body '{}'
$startedInstances+=$ia.id
$a=Get-Instance $ia.id
$keyA=Invoked-Key $a
if ($keyA -and $keyA.Length -ge 30) { OKMsg "A: idempotencyKey presente ($($keyA.Substring(0,8))...)" } else { Fail "A: idempotencyKey vacia o corta ('$keyA')" }

Step 4 'B: la key es unica por invocacion'
$ib=Invoke-RestMethod -Uri "$bpm/v1/bpm/process/$($pd.processversionId)/start" -Method POST -Headers $H -Body '{}'
$startedInstances+=$ib.id
$b=Get-Instance $ib.id
$keyB=Invoked-Key $b
if ($keyB -and $keyB.Length -ge 30) { OKMsg "B: idempotencyKey presente ($($keyB.Substring(0,8))...)" } else { Fail "B: idempotencyKey vacia" }
if ($keyA -ne $keyB) { OKMsg 'B: keys distintas entre invocaciones (unica por invocacion)' } else { Fail 'B: la key se repitio entre invocaciones' }

Step 5 'Cleanup'
foreach ($iid in $startedInstances){ try { Invoke-RestMethod -Uri "$bpm/v1/bpm/instance/$iid`?force=true" -Method DELETE -Headers $H | Out-Null } catch {} }
foreach ($pdid in $createdPdIds){ try { Invoke-RestMethod -Uri "$bpm/v1/bpm/admin/processdef/$pdid" -Method DELETE -Headers $H | Out-Null } catch {} }
OKMsg 'cleanup done'

Write-Host ''
Write-Host "PASSES=$script:passes  FAILURES=$script:failures" -ForegroundColor Cyan
if ($script:failures -gt 0){ exit 1 } else { exit 0 }
