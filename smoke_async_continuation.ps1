# =============================================================================
# smoke_async_continuation.ps1 - 4.1 async continuation
# =============================================================================
#   A) config.async=true fuerza async en un service_task local -> corre en
#      continuation job (near-instant via immediate-kick) -> completa.
#   B) service_task local SIN async -> corre inline (sync) -> sin continuation.
#   C) service_task remoto (serviceCode sin handler local) -> async por default.
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
function Count-Audit($inst,$evt){ return (@($inst.auditLog | Where-Object { $_.eventType -eq $evt })).Count }
function Wait-Complete($id,$maxSec){
    for($i=0; $i -lt ($maxSec*2); $i++){
        $inst=Get-Instance $id
        if ($inst.lifecycle -ne 'active'){ return $inst }
        Start-Sleep -Milliseconds 500
    }
    return Get-Instance $id
}
function New-SvcPd($code,$svcConfig){
    $body=@{
        header=@{ code=$code; name="async $code"; description='tmp'; lifecycle='active' }
        flowElements=@(
            @{ code='start'; name='Start'; type='start_event';  sortOrder=1 }
            @{ code='svc';   name='Svc';   type='service_task'; sortOrder=2; config=$svcConfig }
            @{ code='end';   name='End';   type='end_event';    sortOrder=3 }
        )
        sequenceFlows=@(
            @{ sourceCode='start'; targetCode='svc'; sortOrder=1 }
            @{ sourceCode='svc';   targetCode='end'; sortOrder=2 }
        )
    } | ConvertTo-Json -Depth 12
    $resp=Invoke-RestMethod -Uri "$bpm/v1/bpm/admin/processdef" -Method POST -Headers $H -Body $body
    $script:createdPdIds+=$resp.processdefId
    return $resp
}
function Start-Inst($verId){
    $inst=Invoke-RestMethod -Uri "$bpm/v1/bpm/process/$verId/start" -Method POST -Headers $H -Body '{}'
    $script:startedInstances+=$inst.id
    return $inst
}

# =============================================================================
# A) config.async=true -> async (local echo)
# =============================================================================
Step 2 'A: config.async=true fuerza async en service_task local'
$pdA=New-SvcPd "async_on_$run" @{ serviceCode='bpm.test.echo'; async=$true }
$ia=Start-Inst $pdA.processversionId
$a=Wait-Complete $ia.id 12
if ((Count-Audit $a 'continuation.scheduled') -ge 1) { OKMsg 'A: continuation.scheduled (corre en su propia tx)' } else { Fail 'A: falta continuation.scheduled' }
if ((Count-Audit $a 'continuation.executed') -ge 1) { OKMsg 'A: continuation.executed (immediate-kick disparo)' } else { Fail 'A: falta continuation.executed' }
if ((Count-Audit $a 'service_task.completed') -ge 1) { OKMsg 'A: service_task corrio' } else { Fail 'A: el service_task no corrio' }
if ($a.lifecycle -eq 'completed') { OKMsg 'A: instance completed' } else { Fail "A: lifecycle=$($a.lifecycle)" }

# =============================================================================
# B) local sin async -> inline (sync)
# =============================================================================
Step 3 'B: service_task local sin async corre inline (sync)'
$pdB=New-SvcPd "async_off_$run" @{ serviceCode='bpm.test.echo' }
$ib=Start-Inst $pdB.processversionId
$b=Get-Instance $ib.id   # inline -> ya completo al volver del start
if ((Count-Audit $b 'continuation.scheduled') -eq 0) { OKMsg 'B: NO continuation.scheduled (corrio inline)' } else { Fail 'B: hubo continuation inesperada' }
if ((Count-Audit $b 'service_task.completed') -ge 1) { OKMsg 'B: service_task corrio' } else { Fail 'B: el service_task no corrio' }
if ($b.lifecycle -eq 'completed') { OKMsg 'B: instance completed (sincrono)' } else { Fail "B: lifecycle=$($b.lifecycle)" }

# =============================================================================
# C) remoto (serviceCode sin handler local) -> async por default
# =============================================================================
Step 4 'C: service_task remoto (sin handler local) -> async por default'
$pdC=New-SvcPd "async_remote_$run" @{ serviceCode='zz.remote.demo' }
$ic=Start-Inst $pdC.processversionId
$c=Wait-Complete $ic.id 12
if ((Count-Audit $c 'continuation.scheduled') -ge 1) { OKMsg 'C: continuation.scheduled (default-async en remoto)' } else { Fail 'C: el remoto no corrio async por default' }
if ($c.lifecycle -eq 'completed') { OKMsg 'C: instance completed' } else { Fail "C: lifecycle=$($c.lifecycle)" }

# =============================================================================
Step 5 'Cleanup'
foreach ($iid in $startedInstances){ try { Invoke-RestMethod -Uri "$bpm/v1/bpm/instance/$iid`?force=true" -Method DELETE -Headers $H | Out-Null } catch {} }
foreach ($pdid in $createdPdIds){ try { Invoke-RestMethod -Uri "$bpm/v1/bpm/admin/processdef/$pdid" -Method DELETE -Headers $H | Out-Null } catch {} }
OKMsg 'cleanup done'

Write-Host ''
Write-Host "PASSES=$script:passes  FAILURES=$script:failures" -ForegroundColor Cyan
if ($script:failures -gt 0){ exit 1 } else { exit 0 }
