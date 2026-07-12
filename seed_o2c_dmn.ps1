# =============================================================================
# seed_o2c_dmn.ps1 - O2C B3: 4 tablas DMN de plataforma (SYSTEM) + verificacion
# =============================================================================
# Crea (idempotente: delete+create) las decisiones default de O2C bajo SYSTEM:
#   o2c_pricing (first)  o2c_credito (first)  o2c_percepciones (collect)  o2c_retenciones (first)
# Son DEFAULTS de plataforma; cada tenant puede overridearlas. Verifica pricing/credito
# evaluando por un processdef throwaway (business_rule_task). ASCII puro.
# =============================================================================
param(
    [string]$Email=$env:IMAP_EMAIL, [string]$Password=$env:IMAP_PASSWORD,
    [string]$TenantId=$env:IMAP_TENANT, [switch]$Prod
)
if ($Prod) { $iam='https://imaps.com.ar/imap/iam'; $bpm='https://imaps.com.ar/imap/bpm' }
else       { $iam='http://localhost:8091/imap/iam'; $bpm='http://localhost:8093/imap/bpm' }
if (-not $Email -or -not $Password) { Write-Error 'Faltan creds'; exit 1 }
$SYSTEM='00000000-0000-0000-0000-000000000001'
$script:passes=0; $script:failures=0
function Fail($m){ Write-Host "FAIL: $m" -ForegroundColor Red; $script:failures++ }
function OKMsg($m){ Write-Host "OK   $m" -ForegroundColor Green; $script:passes++ }
function Step($n,$m){ Write-Host ''; Write-Host "[$n] $m" -ForegroundColor Cyan }

Step 1 'Login (autoria bajo SYSTEM)'
$tok=(Invoke-RestMethod -Uri "$iam/v1/auth/login" -Method POST -ContentType 'application/json' -Body (@{email=$Email;password=$Password}|ConvertTo-Json)).accessToken
$H=@{Authorization="Bearer $tok";'X-Tenant-Id'=$SYSTEM;'Content-Type'='application/json'}
OKMsg 'logged in (SYSTEM)'

$run=(Get-Date).ToString('yyyyMMddHHmmss'); $script:createdPdIds=@(); $script:startedInstances=@()
function Reseed($body){
    try { Invoke-RestMethod -Uri "$bpm/v1/bpm/admin/decisiondef/$($body.code)" -Method DELETE -Headers $H | Out-Null } catch {}
    return Invoke-RestMethod -Uri "$bpm/v1/bpm/admin/decisiondef" -Method POST -Headers $H -Body ($body|ConvertTo-Json -Depth 14)
}
function New-Pd($body){ $pd=Invoke-RestMethod -Uri "$bpm/v1/bpm/admin/processdef" -Method POST -Headers $H -Body ($body|ConvertTo-Json -Depth 14); $script:createdPdIds+=$pd.processdefId; return $pd }
function Eval($decisionRef,$vars){
    $uniq=[guid]::NewGuid().ToString('N').Substring(0,8)
    $pd=New-Pd @{ header=@{ code="dmneval_${decisionRef}_${run}_$uniq"; name='eval'; description='tmp'; lifecycle='active' }
        flowElements=@(
            @{ code='start';name='S';type='start_event';sortOrder=1 },
            @{ code='brt';name='B';type='business_rule_task';sortOrder=2;config=@{decisionRef=$decisionRef} },
            @{ code='end';name='E';type='end_event';sortOrder=3 })
        sequenceFlows=@(
            @{sourceCode='start';targetCode='brt';sortOrder=1},
            @{sourceCode='brt';targetCode='end';sortOrder=2}) }
    $inst=Invoke-RestMethod -Uri "$bpm/v1/bpm/process/$($pd.processversionId)/start" -Method POST -Headers $H -Body ($vars|ConvertTo-Json)
    $script:startedInstances+=$inst.id
    $full=Invoke-RestMethod -Uri "$bpm/v1/bpm/instance/$($inst.id)" -Headers $H
    return (@($full.auditLog | Where-Object { $_.eventType -eq 'decision.evaluated' })[0]).data.outputs
}

Step 2 'Seed o2c_pricing (first)'
$p=Reseed @{ code='o2c_pricing'; name='O2C Pricing'; hitPolicy='first'
    inputSchema=@(
        @{var_name='cliente_tier';type='string'},
        @{var_name='producto_cat';type='string'},
        @{var_name='cantidad';type='number'},
        @{var_name='monto';type='number'})
    outputSchema=@(@{var_name='descuento_pct';type='number'})
    rules=@(
        @{priority=1;inputs=@(@{var_name='cliente_tier';operator='eq';value='gold'}, @{var_name='cantidad';operator='gte';value=100});outputs=@(@{var_name='descuento_pct';value=15})},
        @{priority=2;inputs=@(@{var_name='cliente_tier';operator='eq';value='gold'});outputs=@(@{var_name='descuento_pct';value=10})},
        @{priority=3;inputs=@(@{var_name='cliente_tier';operator='eq';value='silver'});outputs=@(@{var_name='descuento_pct';value=5})},
        @{priority=4;inputs=@(@{var_name='cliente_tier';operator='any'});outputs=@(@{var_name='descuento_pct';value=0})}) }
if ($p.rules -ge 4) { OKMsg "o2c_pricing ($($p.rules) rules)" } else { Fail "o2c_pricing rules=$($p.rules)" }

Step 3 'Seed o2c_credito (first) - input derivado credito_margen (=limite-saldo-monto)'
$c=Reseed @{ code='o2c_credito'; name='O2C Credito'; hitPolicy='first'
    inputSchema=@(@{var_name='credito_margen';type='number'})
    outputSchema=@(
        @{var_name='credito_ok';type='string'},
        @{var_name='requiere_sena';type='string'})
    rules=@(
        @{priority=1;inputs=@(@{var_name='credito_margen';operator='gte';value=0});outputs=@(@{var_name='credito_ok';value='true'}, @{var_name='requiere_sena';value='false'})},
        @{priority=2;inputs=@(@{var_name='credito_margen';operator='any'});outputs=@(@{var_name='credito_ok';value='false'}, @{var_name='requiere_sena';value='true'})}) }
if ($c.rules -ge 2) { OKMsg "o2c_credito ($($c.rules) rules)" } else { Fail "o2c_credito rules=$($c.rules)" }

Step 4 'Seed o2c_percepciones (collect) + o2c_retenciones (first) - defaults 0 (tenant override)'
$pe=Reseed @{ code='o2c_percepciones'; name='O2C Percepciones'; hitPolicy='collect'
    inputSchema=@(
        @{var_name='jurisdiccion';type='string'},
        @{var_name='cliente_condicion';type='string'},
        @{var_name='monto';type='number'})
    outputSchema=@(
        @{var_name='iva_perc_pct';type='number'},
        @{var_name='iibb_perc_pct';type='number'})
    rules=@(@{priority=1;inputs=@(@{var_name='jurisdiccion';operator='any'});outputs=@(@{var_name='iva_perc_pct';value=0}, @{var_name='iibb_perc_pct';value=0})}) }
if ($pe.rules -ge 1) { OKMsg "o2c_percepciones ($($pe.rules) rules)" } else { Fail "o2c_percepciones rules=$($pe.rules)" }
$re=Reseed @{ code='o2c_retenciones'; name='O2C Retenciones'; hitPolicy='first'
    inputSchema=@(
        @{var_name='medio_pago';type='string'},
        @{var_name='cliente_condicion';type='string'},
        @{var_name='monto';type='number'})
    outputSchema=@(@{var_name='retencion_pct';type='number'})
    rules=@(@{priority=1;inputs=@(@{var_name='medio_pago';operator='any'});outputs=@(@{var_name='retencion_pct';value=0})}) }
if ($re.rules -ge 1) { OKMsg "o2c_retenciones ($($re.rules) rules)" } else { Fail "o2c_retenciones rules=$($re.rules)" }

Step 5 'Verificar evaluacion (pricing + credito)'
$o1=Eval 'o2c_pricing' @{ cliente_tier='gold'; producto_cat='X'; cantidad=100; monto=5000 }
if ("$($o1.descuento_pct)" -eq '15') { OKMsg "pricing gold+100 -> descuento 15" } else { Fail "pricing -> $($o1.descuento_pct), esperaba 15" }
$o2=Eval 'o2c_pricing' @{ cliente_tier='silver'; producto_cat='X'; cantidad=1; monto=10 }
if ("$($o2.descuento_pct)" -eq '5') { OKMsg "pricing silver -> descuento 5" } else { Fail "pricing -> $($o2.descuento_pct), esperaba 5" }
$o3=Eval 'o2c_credito' @{ credito_margen=50 }
if ("$($o3.credito_ok)" -eq 'true') { OKMsg "credito margen>=0 -> ok=true" } else { Fail "credito -> ok=$($o3.credito_ok), esperaba true" }
$o4=Eval 'o2c_credito' @{ credito_margen=-10 }
if ("$($o4.credito_ok)" -eq 'false' -and "$($o4.requiere_sena)" -eq 'true') { OKMsg "credito margen<0 -> ok=false, sena=true" } else { Fail "credito -> ok=$($o4.credito_ok) sena=$($o4.requiere_sena)" }

Step 6 'Cleanup (solo los processdef throwaway; las 4 DMN quedan seedeadas)'
foreach ($iid in ($startedInstances|Select-Object -Unique)){ try { Invoke-RestMethod -Uri "$bpm/v1/bpm/instance/$iid`?force=true" -Method DELETE -Headers $H | Out-Null } catch {} }
foreach ($pdid in $createdPdIds){ try { Invoke-RestMethod -Uri "$bpm/v1/bpm/admin/processdef/$pdid" -Method DELETE -Headers $H | Out-Null } catch {} }
OKMsg 'cleanup done'

Write-Host ''
Write-Host "PASSES=$script:passes  FAILURES=$script:failures" -ForegroundColor Cyan
if ($script:failures -gt 0) { exit 1 }
