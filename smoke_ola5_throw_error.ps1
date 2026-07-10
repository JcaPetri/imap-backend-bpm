# =============================================================================
# smoke_ola5_throw_error.ps1 - Ola 5: throw events + error propagation sub->parent
# =============================================================================
# Valida 3 features nuevas del motor BPM:
#   A) SIGNAL THROW (5.3): intermediate_event config.throw='signal' hace broadcast
#      de una senal que despierta a un catch armado en otra rama (parallel split).
#   B) COMPENSATE THROW (5.4): intermediate_event config.throw='compensate' corre
#      la compensacion LIFO de lo completado SIN terminar la instance (sigue el flujo).
#   C) ERROR PROPAGATION sub->parent (5.1/5.2): un child sub_process termina en un
#      end_event config.error.errorCode; el boundary error del sub_process en el
#      parent lo captura y rutea a la rama de excepcion.
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
function Count-Audit($inst,$evt){ return (@($inst.auditLog | Where-Object { $_.eventType -eq $evt })).Count }
function First-Audit($inst,$evt){ return @($inst.auditLog | Where-Object { $_.eventType -eq $evt })[0] }
function New-Pd($body){ $pd=Invoke-RestMethod -Uri "$bpm/v1/bpm/admin/processdef" -Method POST -Headers $H -Body ($body|ConvertTo-Json -Depth 14); $script:createdPdIds+=$pd.processdefId; return $pd }
function Start-Inst($verId,$vars){ $i=Invoke-RestMethod -Uri "$bpm/v1/bpm/process/$verId/start" -Method POST -Headers $H -Body ($vars|ConvertTo-Json); $script:startedInstances+=$i.id; return $i }

# ---------------------------------------------------------------------------
Step 2 'A: SIGNAL THROW - parallel split {catch S ; throw S} -> join'
$sig="OLA5SIG_$run"
$bodyA=@{
    header=@{ code="ola5_sig_$run"; name='Ola5 signal throw'; description='tmp'; lifecycle='active' }
    flowElements=@(
        @{ code='start';   name='Start';  type='start_event';      sortOrder=1 }
        @{ code='psplit';  name='PSplit'; type='parallel_gateway'; sortOrder=2 }
        @{ code='ev_catch';name='Catch';  type='intermediate_event'; sortOrder=3; config=@{ signal=@{ signalCode=$sig } } }
        @{ code='ev_throw';name='Throw';  type='intermediate_event'; sortOrder=4; config=@{ throw='signal'; signalCode=$sig } }
        @{ code='pjoin';   name='PJoin';  type='parallel_gateway'; sortOrder=5 }
        @{ code='end';     name='End';    type='end_event';        sortOrder=6 }
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
if ((Count-Audit $a 'signal.thrown') -ge 1) { OKMsg 'A: signal.thrown presente' } else { Fail 'A: falta signal.thrown' }
if ((Count-Audit $a 'signal.received') -ge 1) { OKMsg 'A: signal.received (el catch se desperto)' } else { Fail 'A: el catch no recibio la senal' }
if ($a.lifecycle -eq 'completed') { OKMsg 'A: instance completed (join fusiono ambas ramas)' } else { Fail "A: lifecycle=$($a.lifecycle), esperaba completed" }

# ---------------------------------------------------------------------------
Step 3 'B: COMPENSATE THROW - svc_a(comp) -> throw compensate -> end (sigue el flujo)'
$bodyB=@{
    header=@{ code="ola5_comp_$run"; name='Ola5 compensate throw'; description='tmp'; lifecycle='active' }
    flowElements=@(
        @{ code='start';   name='Start';  type='start_event';       sortOrder=1 }
        @{ code='svc_a';   name='SvcA';   type='service_task';      sortOrder=2; config=@{ serviceCode='bpm.test.echo' } }
        @{ code='ev_comp'; name='Comp';   type='intermediate_event';sortOrder=3; config=@{ throw='compensate' } }
        @{ code='end';     name='End';    type='end_event';         sortOrder=4 }
        @{ code='comp_a';  name='CompA';  type='service_task';      sortOrder=90; config=@{ serviceCode='bpm.test.echo'; compensationFor='svc_a' } }
    )
    sequenceFlows=@(
        @{ sourceCode='start';   targetCode='svc_a';   sortOrder=1 }
        @{ sourceCode='svc_a';   targetCode='ev_comp'; sortOrder=2 }
        @{ sourceCode='ev_comp'; targetCode='end';     sortOrder=3 }
    )
}
$pdB=New-Pd $bodyB
$ib=Start-Inst $pdB.processversionId @{}
Start-Sleep -Milliseconds 500
$b=Get-Instance $ib.id
if ((Count-Audit $b 'compensation.registered') -eq 1) { OKMsg 'B: compensation.registered (svc_a)' } else { Fail "B: registered=$(Count-Audit $b 'compensation.registered'), esperaba 1" }
if ((Count-Audit $b 'compensate.thrown') -ge 1) { OKMsg 'B: compensate.thrown presente' } else { Fail 'B: falta compensate.thrown' }
if ((Count-Audit $b 'compensation.executed') -eq 1) { OKMsg 'B: compensation.executed (LIFO corrio comp_a)' } else { Fail "B: executed=$(Count-Audit $b 'compensation.executed'), esperaba 1" }
if ($b.lifecycle -eq 'completed') { OKMsg 'B: instance completed (el throw NO termino la instance, siguio a end)' } else { Fail "B: lifecycle=$($b.lifecycle), esperaba completed" }

# ---------------------------------------------------------------------------
Step 4 'C: ERROR PROPAGATION - child error end -> parent sub_process boundary'
# child: start -> err_end(error CHILD_ERR)
$bodyChild=@{
    header=@{ code="ola5_child_$run"; name='Ola5 child error'; description='tmp'; lifecycle='active' }
    flowElements=@(
        @{ code='start';   name='Start'; type='start_event'; sortOrder=1 }
        @{ code='err_end'; name='ErrEnd';type='end_event';   sortOrder=2; config=@{ error=@{ errorCode='CHILD_ERR' } } }
    )
    sequenceFlows=@(
        @{ sourceCode='start'; targetCode='err_end'; sortOrder=1 }
    )
}
$pdChild=New-Pd $bodyChild
OKMsg "C: child processdef creado (ver=$($pdChild.processversionId))"

# parent: start -> sub(child, wait) -> end_ok ; boundary error(CHILD_ERR) on sub -> end_handled
$bodyParent=@{
    header=@{ code="ola5_parent_$run"; name='Ola5 parent'; description='tmp'; lifecycle='active' }
    flowElements=@(
        @{ code='start';       name='Start';   type='start_event';   sortOrder=1 }
        @{ code='sub';         name='Sub';      type='sub_process';   sortOrder=2; config=@{ callActivity=@{ calledProcessversionId="$($pdChild.processversionId)"; waitForCompletion=$true } } }
        @{ code='end_ok';      name='EndOk';    type='end_event';     sortOrder=3 }
        @{ code='bnd';         name='Bnd';      type='boundary_event';sortOrder=4; config=@{ boundary=@{ attachedTo='sub'; interrupting=$true }; error=@{ errorCode='CHILD_ERR' } } }
        @{ code='end_handled'; name='EndHandled';type='end_event';    sortOrder=5 }
    )
    sequenceFlows=@(
        @{ sourceCode='start'; targetCode='sub';         sortOrder=1 }
        @{ sourceCode='sub';   targetCode='end_ok';      sortOrder=2 }
        @{ sourceCode='bnd';   targetCode='end_handled'; sortOrder=3 }
    )
}
$pdParent=New-Pd $bodyParent
$ic=Start-Inst $pdParent.processversionId @{}
Start-Sleep -Milliseconds 800
$c=Get-Instance $ic.id
if ((Count-Audit $c 'subprocess.error.propagated') -ge 1) { OKMsg 'C: subprocess.error.propagated presente' } else { Fail 'C: falta subprocess.error.propagated' }
$bef=First-Audit $c 'boundary.error.fired'
if ($bef -and $bef.data.source -eq 'sub_process') { OKMsg 'C: boundary.error.fired source=sub_process' } else { Fail "C: source != sub_process (fue $($bef.data.source))" }
if ($bef -and $bef.data.errorCode -eq 'CHILD_ERR') { OKMsg 'C: errorCode=CHILD_ERR propagado' } else { Fail "C: errorCode inesperado ($($bef.data.errorCode))" }
if ($c.lifecycle -eq 'completed') { OKMsg 'C: parent completed via rama de excepcion' } else { Fail "C: lifecycle=$($c.lifecycle), esperaba completed" }

# ---------------------------------------------------------------------------
Step 5 'Cleanup'
foreach ($iid in $startedInstances){ try { Invoke-RestMethod -Uri "$bpm/v1/bpm/instance/$iid`?force=true" -Method DELETE -Headers $H | Out-Null } catch {} }
foreach ($pdid in $createdPdIds){ try { Invoke-RestMethod -Uri "$bpm/v1/bpm/admin/processdef/$pdid" -Method DELETE -Headers $H | Out-Null } catch {} }
OKMsg 'cleanup done'

Write-Host ''
Write-Host "PASSES=$script:passes  FAILURES=$script:failures" -ForegroundColor Cyan
