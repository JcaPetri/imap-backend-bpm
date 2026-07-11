# =============================================================================
# smoke_dmn_drd.ps1 - Ola 7.1: DMN DRD chaining (decisiones encadenadas)
# =============================================================================
# drd_top REQUIERE drd_base. Al evaluar drd_top, el motor evalua drd_base primero
# (orden topologico) e inyecta su output (tier) como input de drd_top.
#
#   drd_base:  amount>=100 -> tier=gold ; else -> tier=silver
#   drd_top:   tier=gold   -> approved=yes ; else -> approved=no   (requires drd_base)
#
#   A) amount=150 -> base:gold  -> top:approved=yes  (chain gold)
#   B) amount=50  -> base:silver-> top:approved=no   (chain silver)
#   + valida drdChain=[drd_base, drd_top] en el audit.
#
# Usa el endpoint de autoria nuevo POST /v1/bpm/admin/decisiondef. ASCII puro.
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

$run=(Get-Date).ToString('yyyyMMddHHmmss'); $script:createdPdIds=@(); $script:startedInstances=@(); $script:createdDecisions=@()
$base="drd_base_$run"; $top="drd_top_$run"
function Get-Instance($id){ return Invoke-RestMethod -Uri "$bpm/v1/bpm/instance/$id" -Headers $H }
function First-Audit($inst,$evt){ return @($inst.auditLog | Where-Object { $_.eventType -eq $evt })[0] }
function New-Decision($body){ $r=Invoke-RestMethod -Uri "$bpm/v1/bpm/admin/decisiondef" -Method POST -Headers $H -Body ($body|ConvertTo-Json -Depth 14); $script:createdDecisions+=$body.code; return $r }
function New-Pd($body){ $pd=Invoke-RestMethod -Uri "$bpm/v1/bpm/admin/processdef" -Method POST -Headers $H -Body ($body|ConvertTo-Json -Depth 14); $script:createdPdIds+=$pd.processdefId; return $pd }
function Start-Inst($verId,$vars){ $i=Invoke-RestMethod -Uri "$bpm/v1/bpm/process/$verId/start" -Method POST -Headers $H -Body ($vars|ConvertTo-Json); $script:startedInstances+=$i.id; return $i }

Step 2 'Crear decision base (amount -> tier)'
$rb=New-Decision @{
    code=$base; name='DRD base'; hitPolicy='first'
    inputSchema=@(@{ var_name='amount'; type='number' })
    outputSchema=@(@{ var_name='tier'; type='string' })
    rules=@(
        @{ priority=1; inputs=@(@{ var_name='amount'; operator='gte'; value=100 }); outputs=@(@{ var_name='tier'; value='gold' }) }
        @{ priority=2; inputs=@(@{ var_name='amount'; operator='any' });            outputs=@(@{ var_name='tier'; value='silver' }) }
    )
}
if ($rb.rules -eq 2) { OKMsg "base creada ($($rb.rules) rules)" } else { Fail "base rules=$($rb.rules)" }

Step 3 'Crear decision top (REQUIERE base; tier -> approved)'
$rt=New-Decision @{
    code=$top; name='DRD top'; hitPolicy='first'
    requiredDecisions=@($base)
    inputSchema=@(@{ var_name='tier'; type='string' })
    outputSchema=@(@{ var_name='approved'; type='string' })
    rules=@(
        @{ priority=1; inputs=@(@{ var_name='tier'; operator='eq'; value='gold' }); outputs=@(@{ var_name='approved'; value='yes' }) }
        @{ priority=2; inputs=@(@{ var_name='tier'; operator='any' });              outputs=@(@{ var_name='approved'; value='no' }) }
    )
}
if ($rt.rules -eq 2) { OKMsg "top creada ($($rt.rules) rules, requires $base)" } else { Fail "top rules=$($rt.rules)" }

Step 4 'Processdef: start -> brt(drd_top) -> end'
$pd=New-Pd @{
    header=@{ code="drd_proc_$run"; name='DRD proc'; description='tmp'; lifecycle='active' }
    flowElements=@(
        @{ code='start'; name='Start'; type='start_event';        sortOrder=1 }
        @{ code='brt';   name='Brt';   type='business_rule_task'; sortOrder=2; config=@{ decisionRef=$top } }
        @{ code='end';   name='End';   type='end_event';          sortOrder=3 }
    )
    sequenceFlows=@(
        @{ sourceCode='start'; targetCode='brt'; sortOrder=1 }
        @{ sourceCode='brt';   targetCode='end'; sortOrder=2 }
    )
}
OKMsg 'processdef creado'

Step 5 'A: amount=150 -> chain gold -> approved=yes'
$ia=Start-Inst $pd.processversionId @{ amount=150 }
Start-Sleep -Milliseconds 600
$a=Get-Instance $ia.id
$deA=First-Audit $a 'decision.evaluated'
if ($deA -and $deA.data.outputs.approved -eq 'yes') { OKMsg 'A: approved=yes (base:gold encadeno a top)' } else { Fail "A: approved=$($deA.data.outputs.approved), esperaba yes" }
$chainA=@($deA.data.drdChain)
if ($chainA.Count -eq 2 -and $chainA[0] -eq $base -and $chainA[1] -eq $top) { OKMsg "A: drdChain=[$($chainA -join ', ')] (dep primero, top al final)" } else { Fail "A: drdChain inesperado ($($chainA -join ', '))" }
if ($a.lifecycle -eq 'completed') { OKMsg 'A: instance completed' } else { Fail "A: lifecycle=$($a.lifecycle)" }

Step 6 'B: amount=50 -> chain silver -> approved=no'
$ib=Start-Inst $pd.processversionId @{ amount=50 }
Start-Sleep -Milliseconds 600
$b=Get-Instance $ib.id
$deB=First-Audit $b 'decision.evaluated'
if ($deB -and $deB.data.outputs.approved -eq 'no') { OKMsg 'B: approved=no (base:silver encadeno a top)' } else { Fail "B: approved=$($deB.data.outputs.approved), esperaba no" }

Step 7 'Cleanup'
foreach ($iid in ($startedInstances | Select-Object -Unique)){ try { Invoke-RestMethod -Uri "$bpm/v1/bpm/instance/$iid`?force=true" -Method DELETE -Headers $H | Out-Null } catch {} }
foreach ($pdid in $createdPdIds){ try { Invoke-RestMethod -Uri "$bpm/v1/bpm/admin/processdef/$pdid" -Method DELETE -Headers $H | Out-Null } catch {} }
foreach ($dc in $createdDecisions){ try { Invoke-RestMethod -Uri "$bpm/v1/bpm/admin/decisiondef/$dc" -Method DELETE -Headers $H | Out-Null } catch {} }
OKMsg 'cleanup done'

Write-Host ''
Write-Host "PASSES=$script:passes  FAILURES=$script:failures" -ForegroundColor Cyan
