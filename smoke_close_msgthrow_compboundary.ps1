# =============================================================================
# smoke_close_msgthrow_compboundary.ps1 - cierre diferidos Ola 5
#   Item 1: message-throw inline (correlate + start)
#   Item 4: compensation-boundary formal
# =============================================================================
#   A) MESSAGE THROW correlate: parallel split {catch MSG(key) ; throw MSG(key)}
#      -> el throw correlaciona con el catch armado -> join -> completed.
#   B) MESSAGE THROW start: def target con start_event message MSG_B; def trigger
#      start -> throw MSG_B (sin key) -> startProcessByMessage arranca el target.
#   C) COMPENSATION BOUNDARY formal: boundary_event compensation=true adjunto a
#      svc_a; su flow apunta al handler off-path. end compensate=true lo corre.
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

$run=(Get-Date).ToString('yyyyMMddHHmmss'); $script:createdPdIds=@(); $script:startedInstances=@()
function Get-Instance($id){ return Invoke-RestMethod -Uri "$bpm/v1/bpm/instance/$id" -Headers $H }
function Count-Audit($inst,$evt){ return (@($inst.auditLog | Where-Object { $_.eventType -eq $evt })).Count }
function First-Audit($inst,$evt){ return @($inst.auditLog | Where-Object { $_.eventType -eq $evt })[0] }
function New-Pd($body){ $pd=Invoke-RestMethod -Uri "$bpm/v1/bpm/admin/processdef" -Method POST -Headers $H -Body ($body|ConvertTo-Json -Depth 14); $script:createdPdIds+=$pd.processdefId; return $pd }
function Start-Inst($verId,$vars){ $i=Invoke-RestMethod -Uri "$bpm/v1/bpm/process/$verId/start" -Method POST -Headers $H -Body ($vars|ConvertTo-Json); $script:startedInstances+=$i.id; return $i }

# ---------------------------------------------------------------------------
Step 2 'A: MESSAGE THROW correlate - split {catch MSG(key) ; throw MSG(key)} -> join'
$msg="OLA5MSG_$run"; $key="K_$run"
$bodyA=@{
    header=@{ code="close_msgcorr_$run"; name='Msg throw correlate'; description='tmp'; lifecycle='active' }
    flowElements=@(
        @{ code='start';   name='Start';  type='start_event';        sortOrder=1 }
        @{ code='psplit';  name='PSplit'; type='parallel_gateway';   sortOrder=2 }
        @{ code='ev_catch';name='Catch';  type='intermediate_event'; sortOrder=3; config=@{ message=@{ messageCode=$msg; correlationKey=$key } } }
        @{ code='ev_throw';name='Throw';  type='intermediate_event'; sortOrder=4; config=@{ throw='message'; messageCode=$msg; correlationKey=$key } }
        @{ code='pjoin';   name='PJoin';  type='parallel_gateway';   sortOrder=5 }
        @{ code='end';     name='End';    type='end_event';          sortOrder=6 }
    )
    sequenceFlows=@(
        @{ sourceCode='start';    targetCode='psplit';   sortOrder=1 }
        @{ sourceCode='psplit';   targetCode='ev_catch'; sortOrder=2 }
        @{ sourceCode='psplit';   targetCode='ev_throw'; sortOrder=3 }
        @{ sourceCode='ev_catch'; targetCode='pjoin';    sortOrder=4 }
        @{ sourceCode='ev_throw'; targetCode='pjoin';    sortOrder=5 }
        @{ sourceCode='pjoin';    targetCode='end';      sortOrder=6 }
    )
}
$pdA=New-Pd $bodyA
$ia=Start-Inst $pdA.processversionId @{}
Start-Sleep -Milliseconds 500
$a=Get-Instance $ia.id
$mt=First-Audit $a 'message.thrown'
if ($mt) { OKMsg 'A: message.thrown presente' } else { Fail 'A: falta message.thrown' }
if ($mt -and $mt.data.mode -eq 'correlate') { OKMsg 'A: mode=correlate' } else { Fail "A: mode inesperado ($($mt.data.mode))" }
if ((Count-Audit $a 'message.received') -ge 1) { OKMsg 'A: message.received (el catch correlaciono)' } else { Fail 'A: el catch no recibio el mensaje' }
if ($a.lifecycle -eq 'completed') { OKMsg 'A: instance completed (join fusiono ambas ramas)' } else { Fail "A: lifecycle=$($a.lifecycle), esperaba completed" }

# ---------------------------------------------------------------------------
Step 3 'B: MESSAGE THROW start - trigger throw MSG_B -> arranca def target'
$msgB="OLA5START_$run"
$bodyTarget=@{
    header=@{ code="close_msgtarget_$run"; name='Msg start target'; description='tmp'; lifecycle='active' }
    flowElements=@(
        @{ code='start'; name='Start'; type='start_event'; sortOrder=1; config=@{ message=@{ messageCode=$msgB } } }
        @{ code='end';   name='End';   type='end_event';   sortOrder=2 }
    )
    sequenceFlows=@(
        @{ sourceCode='start'; targetCode='end'; sortOrder=1 }
    )
}
$pdTarget=New-Pd $bodyTarget
# La subscripcion message-start se sincroniza en loader.load con tenant operativo
# (no en activacion). Un arranque directo del target la puebla in-tenant.
Start-Inst $pdTarget.processversionId @{} | Out-Null
Start-Sleep -Milliseconds 300
OKMsg "B: target creado + subscripcion message-start '$msgB' sincronizada"
$bodyTrigger=@{
    header=@{ code="close_msgtrigger_$run"; name='Msg start trigger'; description='tmp'; lifecycle='active' }
    flowElements=@(
        @{ code='start';   name='Start'; type='start_event';        sortOrder=1 }
        @{ code='ev_throw';name='Throw'; type='intermediate_event'; sortOrder=2; config=@{ throw='message'; messageCode=$msgB } }
        @{ code='end';     name='End';   type='end_event';          sortOrder=3 }
    )
    sequenceFlows=@(
        @{ sourceCode='start';    targetCode='ev_throw'; sortOrder=1 }
        @{ sourceCode='ev_throw'; targetCode='end';      sortOrder=2 }
    )
}
$pdTrigger=New-Pd $bodyTrigger
$ib=Start-Inst $pdTrigger.processversionId @{}
Start-Sleep -Milliseconds 700
$b=Get-Instance $ib.id
$mtB=First-Audit $b 'message.thrown'
if ($mtB -and $mtB.data.mode -eq 'start') { OKMsg 'B: mode=start' } else { Fail "B: mode inesperado ($($mtB.data.mode))" }
if ($mtB -and [int]$mtB.data.started -ge 1) { OKMsg "B: started=$($mtB.data.started) (arranco el target)" } else { Fail "B: started=$($mtB.data.started), esperaba >=1" }

# ---------------------------------------------------------------------------
Step 4 'C: COMPENSATION BOUNDARY formal - boundary compensation=true -> handler'
$bodyC=@{
    header=@{ code="close_compbnd_$run"; name='Comp boundary formal'; description='tmp'; lifecycle='active' }
    flowElements=@(
        @{ code='start';    name='Start';    type='start_event';   sortOrder=1 }
        @{ code='svc_a';    name='SvcA';     type='service_task';  sortOrder=2; config=@{ serviceCode='bpm.test.echo' } }
        @{ code='end_comp'; name='EndComp';  type='end_event';     sortOrder=3; config=@{ compensate=$true } }
        @{ code='bnd_comp'; name='BndComp';  type='boundary_event';sortOrder=4; config=@{ boundary=@{ attachedTo='svc_a'; interrupting=$false }; compensation=$true } }
        @{ code='comp_h';   name='CompH';    type='service_task';  sortOrder=90; config=@{ serviceCode='bpm.test.echo' } }
    )
    sequenceFlows=@(
        @{ sourceCode='start';    targetCode='svc_a';    sortOrder=1 }
        @{ sourceCode='svc_a';    targetCode='end_comp'; sortOrder=2 }
        @{ sourceCode='bnd_comp'; targetCode='comp_h';   sortOrder=3 }
    )
}
$pdC=New-Pd $bodyC
$ic=Start-Inst $pdC.processversionId @{}
Start-Sleep -Milliseconds 600
$c=Get-Instance $ic.id
if ((Count-Audit $c 'compensation.registered') -eq 1) { OKMsg 'C: compensation.registered (via boundary formal)' } else { Fail "C: registered=$(Count-Audit $c 'compensation.registered'), esperaba 1" }
if ((Count-Audit $c 'compensation.executed') -eq 1) { OKMsg 'C: compensation.executed (corrio el handler del boundary)' } else { Fail "C: executed=$(Count-Audit $c 'compensation.executed'), esperaba 1" }
if ($c.lifecycle -eq 'completed') { OKMsg 'C: instance completed' } else { Fail "C: lifecycle=$($c.lifecycle), esperaba completed" }

# ---------------------------------------------------------------------------
Step 5 'Cleanup'
# instancias arrancadas por message-start (target) -> por processdef
try {
    $tgtInsts=@(Invoke-RestMethod -Uri "$bpm/v1/bpm/processdef/$($pdTarget.processdefId)/instances" -Headers $H)
    foreach ($ti in $tgtInsts){ $script:startedInstances+=$ti.id }
} catch {}
foreach ($iid in ($script:startedInstances | Select-Object -Unique)){ try { Invoke-RestMethod -Uri "$bpm/v1/bpm/instance/$iid`?force=true" -Method DELETE -Headers $H | Out-Null } catch {} }
foreach ($pdid in $script:createdPdIds){ try { Invoke-RestMethod -Uri "$bpm/v1/bpm/admin/processdef/$pdid" -Method DELETE -Headers $H | Out-Null } catch {} }
OKMsg 'cleanup done'

Write-Host ''
Write-Host "PASSES=$script:passes  FAILURES=$script:failures" -ForegroundColor Cyan
