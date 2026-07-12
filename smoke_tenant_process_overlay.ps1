# =============================================================================
# smoke_tenant_process_overlay.ps1 - overlay de procesos por tenant (gemelo plan de cuentas)
# =============================================================================
# Nivel 1 (habilitar): POST /tenant-process/{code}/enable + GET lista + DELETE (disable).
# Nivel 2 (parametrizar): PUT /tenant-process/{code}/config {premium} -> el motor inyecta
#   la variable 'config' al arrancar; un exclusive_gateway lee ${config.premium} y cambia
#   de rama SIN forkear el processdef (mismo def, distinto comportamiento por tenant).
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
$code="tprocess_$run"
function Get-Instance($id){ return Invoke-RestMethod -Uri "$bpm/v1/bpm/instance/$id" -Headers $H }
function Entered($inst,$elem){ return (@($inst.auditLog | Where-Object { $_.eventType -eq 'token.entered' -and $_.data.elementCode -eq $elem })).Count }
function New-Pd($body){ $pd=Invoke-RestMethod -Uri "$bpm/v1/bpm/admin/processdef" -Method POST -Headers $H -Body ($body|ConvertTo-Json -Depth 14); $script:createdPdIds+=$pd.processdefId; return $pd }
function Start-Inst($verId){ $i=Invoke-RestMethod -Uri "$bpm/v1/bpm/process/$verId/start" -Method POST -Headers $H -Body '{}'; $script:startedInstances+=$i.id; return $i }

Step 2 'Crear processdef: start -> XOR(config.premium) -> {premium | estandar} -> end'
$pd=New-Pd @{
    header=@{ code=$code; name='Tenant process overlay'; description='tmp'; lifecycle='active' }
    flowElements=@(
        @{ code='start';       name='Start';   type='start_event';       sortOrder=1 }
        @{ code='xor';         name='Xor';     type='exclusive_gateway'; sortOrder=2 }
        @{ code='svc_premium'; name='Premium'; type='service_task';      sortOrder=3; config=@{ serviceCode='bpm.test.echo' } }
        @{ code='svc_std';     name='Std';     type='service_task';      sortOrder=4; config=@{ serviceCode='bpm.test.echo' } }
        @{ code='end_p';       name='EndP';    type='end_event';         sortOrder=5 }
        @{ code='end_s';       name='EndS';    type='end_event';         sortOrder=6 }
    )
    sequenceFlows=@(
        @{ sourceCode='start'; targetCode='xor';         sortOrder=1 }
        @{ sourceCode='xor';   targetCode='svc_premium'; sortOrder=2; conditionExpr='${config.premium}' }
        @{ sourceCode='xor';   targetCode='svc_std';     sortOrder=3 }
        @{ sourceCode='svc_premium'; targetCode='end_p'; sortOrder=4 }
        @{ sourceCode='svc_std';     targetCode='end_s'; sortOrder=5 }
    )
}
OKMsg "processdef creado (code=$code)"

Step 3 'Nivel 1 - habilitar + listar'
Invoke-RestMethod -Uri "$bpm/v1/bpm/tenant-process/$code/enable" -Method POST -Headers $H | Out-Null
$lst=@(Invoke-RestMethod -Uri "$bpm/v1/bpm/tenant-process" -Headers $H)
$row=@($lst | Where-Object { $_.processdefCode -eq $code })[0]
if ($row -and $row.enabled -eq $true) { OKMsg 'N1: proceso habilitado y aparece en la lista' } else { Fail 'N1: no aparece habilitado' }

Step 4 'Nivel 2 - config premium=true -> el gateway toma la rama PREMIUM'
Invoke-RestMethod -Uri "$bpm/v1/bpm/tenant-process/$code/config" -Method PUT -Headers $H -Body (@{ premium=$true }|ConvertTo-Json) | Out-Null
$ia=Start-Inst $pd.processversionId
Start-Sleep -Milliseconds 500
$a=Get-Instance $ia.id
if ((Entered $a 'svc_premium') -ge 1 -and (Entered $a 'svc_std') -eq 0) { OKMsg 'N2: config.premium=true -> rama PREMIUM (sin forkear)' } else { Fail "N2: rama incorrecta (premium=$(Entered $a 'svc_premium'), std=$(Entered $a 'svc_std'))" }

Step 5 'Nivel 2 - config premium=false -> el gateway toma la rama ESTANDAR'
Invoke-RestMethod -Uri "$bpm/v1/bpm/tenant-process/$code/config" -Method PUT -Headers $H -Body (@{ premium=$false }|ConvertTo-Json) | Out-Null
$ib=Start-Inst $pd.processversionId
Start-Sleep -Milliseconds 500
$b=Get-Instance $ib.id
if ((Entered $b 'svc_std') -ge 1 -and (Entered $b 'svc_premium') -eq 0) { OKMsg 'N2: config.premium=false -> rama ESTANDAR (mismo def, distinto comportamiento)' } else { Fail "N2: rama incorrecta (premium=$(Entered $b 'svc_premium'), std=$(Entered $b 'svc_std'))" }

Step 6 'Nivel 1 - deshabilitar'
Invoke-RestMethod -Uri "$bpm/v1/bpm/tenant-process/$code" -Method DELETE -Headers $H | Out-Null
$lst2=@(Invoke-RestMethod -Uri "$bpm/v1/bpm/tenant-process" -Headers $H)
$row2=@($lst2 | Where-Object { $_.processdefCode -eq $code })[0]
if ($row2 -and $row2.enabled -eq $false) { OKMsg 'N1: deshabilitado (soft, conserva config)' } else { Fail 'N1: no quedo deshabilitado' }

Step 7 'Cleanup'
foreach ($iid in ($startedInstances | Select-Object -Unique)){ try { Invoke-RestMethod -Uri "$bpm/v1/bpm/instance/$iid`?force=true" -Method DELETE -Headers $H | Out-Null } catch {} }
foreach ($pdid in $createdPdIds){ try { Invoke-RestMethod -Uri "$bpm/v1/bpm/admin/processdef/$pdid" -Method DELETE -Headers $H | Out-Null } catch {} }
OKMsg 'cleanup done (nota: la fila de overlay queda como config, keyed por code unico)'

Write-Host ''
Write-Host "PASSES=$script:passes  FAILURES=$script:failures" -ForegroundColor Cyan
