package com.imap.bpm.domain.engine;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.imap.bpm.infrastructure.entity.*;
import com.imap.bpm.infrastructure.repository.*;
import com.imap.bpm.infrastructure.tenant.TenantContextHolder;
import com.imap.eav.engine.context.EavTenantSession;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import org.apache.commons.jexl3.JexlBuilder;
import org.apache.commons.jexl3.JexlContext;
import org.apache.commons.jexl3.JexlEngine;
import org.apache.commons.jexl3.MapContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.util.*;

/**
 * Core state machine del motor BPMN.
 *
 * MVP scope (todo lo necesario para correr el proceso Login):
 *   - start_event       → avanza
 *   - end_event         → consume token, si último cerro instance
 *   - user_task         → token.waiting + crea TaskInstance + detiene
 *   - service_task      → MVP: audita + avanza al siguiente (sin invocar service real)
 *   - exclusive_gateway → evalúa conditions JEXL, avanza el primero que matchea
 *   - parallel_gateway  → SPLIT (1-in N-out: emite N child tokens) +
 *                         JOIN  (M-in 1-out: espera todos siblings con mismo
 *                                parent_token_id antes de avanzar al outgoing)
 *   - intermediate_event (timer/message/signal): A2 — timer programa job
 *                         vía JobExecutor; message/signal subscribe correlations
 *                         que reactivan tokens al recibir POST a /messages/
 *                         correlate o /signals/broadcast.
 *   - sub_process       (B1 — call activity): spawnea child ProcessInstance
 *                         con parent_instance_id + parent_token_id; al
 *                         completar el child, copia returnVariables al parent
 *                         y reactiva el token waiting en el sub_process.
 *   - boundary_event    (B2 — interrupting timer sobre user_task): adosado
 *                         a una activity vía config.boundary.attachedTo;
 *                         schedula JobExecutor al crear la activity; al
 *                         disparar, cancela la task + consume token + crea
 *                         nuevo token en outgoing del boundary.
 *   - business_rule_task (B3 — DMN): config.decisionRef apunta a un
 *                         bpm_dmn_decisiondef por code. DmnEvaluator
 *                         aplica operadores declarativos + hit policy,
 *                         outputs van a variables del processinstance.
 *
 * NO soportado todavía (backlog Paso 07+):
 *   - boundary_event sobre sub_process (requiere cancel-cascade child)
 *   - boundary_event type=error/escalation (solo timer hoy)
 *   - boundary_event no-interrupting (mantiene activity + token paralelo)
 *   - compensation
 *   - DMN hit policies priority/collect/any/output-order
 *   - DMN FEEL expression language (MVP usa operadores declarativos)
 *   - sub_process fire-and-forget (waitForCompletion=false)
 */
@Service
public class ProcessEngine {

    private static final Logger log = LoggerFactory.getLogger(ProcessEngine.class);

    /** UUID de sys_state_tbl WHERE code='active'. Hardcoded — el ciclo de
     *  vida BPM usa el lifecycle field, no state_id, así que cualquier UUID
     *  válido del sys_state alcanza. Si pinta error podemos parametrizarlo. */
    private static final UUID DEFAULT_STATE_ACTIVE =
        UUID.fromString("019dc6f0-aa83-7333-8888-000000000001");

    private final ProcessDefinitionLoader loader;
    private final ProcessInstanceRepository instanceRepo;
    private final TokenRepository tokenRepo;
    private final TaskInstanceRepository taskRepo;
    private final VariableRepository varRepo;
    private final AuditLogRepository auditRepo;
    private final JobExecutorRepository jobRepo;
    private final MessageCorrelationRepository msgCorrRepo;
    private final EavTenantSession tenantSession;
    private final DecisionDefinitionLoader decisionLoader;
    private final DmnEvaluator dmnEvaluator;
    private final MeterRegistry meterRegistry;
    private final ObjectMapper jackson;
    private final JexlEngine jexl;

    public ProcessEngine(ProcessDefinitionLoader loader,
                         ProcessInstanceRepository instanceRepo,
                         TokenRepository tokenRepo,
                         TaskInstanceRepository taskRepo,
                         VariableRepository varRepo,
                         AuditLogRepository auditRepo,
                         JobExecutorRepository jobRepo,
                         MessageCorrelationRepository msgCorrRepo,
                         EavTenantSession tenantSession,
                         DecisionDefinitionLoader decisionLoader,
                         DmnEvaluator dmnEvaluator,
                         MeterRegistry meterRegistry,
                         ObjectMapper jackson) {
        this.loader = loader;
        this.instanceRepo = instanceRepo;
        this.tokenRepo = tokenRepo;
        this.taskRepo = taskRepo;
        this.varRepo = varRepo;
        this.auditRepo = auditRepo;
        this.jobRepo = jobRepo;
        this.msgCorrRepo = msgCorrRepo;
        this.tenantSession = tenantSession;
        this.decisionLoader = decisionLoader;
        this.dmnEvaluator = dmnEvaluator;
        this.meterRegistry = meterRegistry;
        this.jackson = jackson;
        this.jexl = new JexlBuilder().silent(true).strict(false).create();
    }

    // ─── Métricas Micrometer (C3) — helper para counters tag-by-processdef ──

    private void metricInc(String name, ProcessDefinition def) {
        String code = def == null ? "unknown" : def.processdefCode();
        meterRegistry.counter(name, Tags.of("processdef", code == null ? "unknown" : code)).increment();
    }
    private void metricInc(String name, String processdefCode) {
        meterRegistry.counter(name, Tags.of("processdef", processdefCode == null ? "unknown" : processdefCode)).increment();
    }
    private void metricInc(String name) {
        meterRegistry.counter(name).increment();
    }

    // ════════════════════════════════════════════════════════════════════════
    // Public API
    // ════════════════════════════════════════════════════════════════════════

    /**
     * Arranca una nueva instancia del proceso identificado por processVersionId.
     * Avanza tokens hasta el primer wait state.
     *
     * @param processVersionId UUID de la processversion (snapshot inmutable)
     * @param payload          variables iniciales del proceso
     * @param bearerToken      JWT actual (para propagar a la call a system)
     * @param tenantId         tenant_id activo
     * @param userId           UUID del user que dispara, puede ser null (anonymous)
     */
    @Transactional
    public ProcessInstance startProcess(UUID processVersionId,
                                        Map<String, Object> payload,
                                        String bearerToken,
                                        UUID tenantId,
                                        UUID userId) {
        tenantSession.applyToCurrentTransaction();
        return startProcessInternal(processVersionId, payload, bearerToken, tenantId, userId,
            null, null);
    }

    /**
     * Variante para spawn de sub_process (B1 — call activity).
     * Si parentInstanceId / parentTokenId son non-null, queda registrada como
     * child en bpm_pro_processinstance_tbl. Al completar (handleEndEvent),
     * el motor notifica al parent y avanza el token waiting.
     */
    private ProcessInstance startProcessInternal(UUID processVersionId,
                                                  Map<String, Object> payload,
                                                  String bearerToken,
                                                  UUID tenantId,
                                                  UUID userId,
                                                  UUID parentInstanceId,
                                                  UUID parentTokenId) {
        ProcessDefinition def = loader.load(processVersionId, bearerToken, tenantId);
        log.info("Starting process {} (v{}) for user {} parent={}",
            def.processdefCode(), def.version(), userId, parentInstanceId);

        // 1. Find start_event
        ProcessDefinition.FlowElement start = def.flowElements().stream()
            .filter(fe -> "start_event".equals(fe.type()))
            .findFirst()
            .orElseThrow(() -> new IllegalStateException(
                "No start_event in processVersion " + processVersionId));

        // 2. Crear ProcessInstance (con parent refs si es sub_process)
        ProcessInstance instance = newInstance(def, tenantId, userId);
        instance.setParentInstanceId(parentInstanceId);
        instance.setParentTokenId(parentTokenId);
        instanceRepo.save(instance);
        Map<String, Object> startedAuditData = new LinkedHashMap<>();
        startedAuditData.put("processCode", def.processdefCode());
        startedAuditData.put("version", def.version());
        if (parentInstanceId != null) startedAuditData.put("parentInstanceId", parentInstanceId.toString());
        if (parentTokenId != null)    startedAuditData.put("parentTokenId", parentTokenId.toString());
        audit(instance, "instance.started", null, null, userId, startedAuditData);

        // 3. Persistir variables iniciales del payload
        if (payload != null) {
            for (Map.Entry<String, Object> e : payload.entrySet()) {
                setVariable(instance, e.getKey(), e.getValue());
            }
        }

        // 4. Token inicial en start_event
        Token token = newToken(instance, start.id(), null);
        tokenRepo.save(token);
        audit(instance, "token.entered", start.id(), token.getId(), userId, Map.of(
            "elementCode", start.code(),
            "elementType", start.type()
        ));

        // 5. Avanza hasta wait state o end
        advanceToken(instance, token, def, userId);

        metricInc("bpm.instance.started", def);

        // Reload instance (puede haberse marcado completed durante advance)
        return instanceRepo.findById(instance.getId()).orElse(instance);
    }

    /**
     * Completa una TaskInstance: merge outputData en variables + avanza el token.
     */
    @Transactional
    public TaskInstance completeTask(UUID taskInstanceId,
                                     Map<String, Object> outputData,
                                     String bearerToken,
                                     UUID userId) {
        tenantSession.applyToCurrentTransaction();
        TaskInstance task = taskRepo.findById(taskInstanceId)
            .orElseThrow(() -> new IllegalArgumentException("Task not found: " + taskInstanceId));
        if (!List.of("created", "assigned", "in_progress").contains(task.getLifecycle())) {
            throw new IllegalStateException("Task " + taskInstanceId
                + " is not active (lifecycle=" + task.getLifecycle() + ")");
        }

        OffsetDateTime now = OffsetDateTime.now();
        task.setOutputData(outputData);
        task.setCompletedAt(now);
        task.setLifecycle("completed");
        task.setUpdatedAt(now);
        task.setUpdatedById(userId);
        taskRepo.save(task);

        ProcessInstance instance = instanceRepo.findById(task.getProcessinstanceId())
            .orElseThrow(() -> new IllegalStateException(
                "ProcessInstance not found for task: " + taskInstanceId));

        // Merge outputData en variables del processinstance
        if (outputData != null) {
            for (Map.Entry<String, Object> e : outputData.entrySet()) {
                setVariable(instance, e.getKey(), e.getValue());
            }
        }
        audit(instance, "task.completed", task.getFlowelementId(), task.getTokenId(), userId,
            Map.of("taskInstanceId", task.getId().toString()));
        metricInc("bpm.task.completed");

        // B2 — cancelar boundary timers del token antes de avanzar (sino
        // disparan después de que la task ya está completa = bug).
        if (task.getTokenId() != null) {
            int cancelled = cancelBoundaryJobsForToken(task.getTokenId());
            if (cancelled > 0) {
                audit(instance, "boundary.cancelled", task.getFlowelementId(), task.getTokenId(), userId,
                    Map.of("reason", "task.completed", "cancelledJobs", cancelled));
            }
        }

        // Avanza el token: el current element es el user_task que se acaba de
        // completar. Reactivamos el token (estaba 'waiting') y CONSUMIMOS el
        // current element pasando al siguiente — sino advanceToken vería el
        // user_task otra vez y crearía OTRA TaskInstance (bug catched 2026-05-17).
        if (task.getTokenId() != null) {
            Token token = tokenRepo.findById(task.getTokenId()).orElse(null);
            if (token != null && "waiting".equals(token.getLifecycle())) {
                token.setLifecycle("active");
                token.setUpdatedAt(now);
                tokenRepo.save(token);
                ProcessDefinition def = loader.load(instance.getProcessversionId(), bearerToken, instance.getTenantId());
                ProcessDefinition.FlowElement current = def.findElementById(token.getCurrentElementId());
                if (current != null) {
                    consumeAndMoveToNext(instance, token, current, def, userId);
                }
            }
        }
        return task;
    }

    // ════════════════════════════════════════════════════════════════════════
    // State machine — advanceToken (recursivo hasta wait state)
    // ════════════════════════════════════════════════════════════════════════

    private void advanceToken(ProcessInstance instance, Token token,
                              ProcessDefinition def, UUID userId) {
        ProcessDefinition.FlowElement current = def.findElementById(token.getCurrentElementId());
        if (current == null) {
            log.error("FlowElement not found: {}", token.getCurrentElementId());
            return;
        }

        switch (current.type()) {
            case "start_event":
                consumeAndMoveToNext(instance, token, current, def, userId);
                break;

            case "end_event":
                handleEndEvent(instance, token, current, userId);
                break;

            case "user_task":
                handleUserTask(instance, token, current, def, userId);
                break;

            case "service_task":
                // MVP: log + avanza. En iter siguiente: invoca handler real (Kafka/HTTP).
                log.info("service_task '{}' invoked — MVP just logs and advances", current.code());
                audit(instance, "service_task.invoked", current.id(), token.getId(), userId,
                    Map.of("elementCode", current.code()));
                consumeAndMoveToNext(instance, token, current, def, userId);
                break;

            case "exclusive_gateway":
                handleExclusiveGateway(instance, token, current, def, userId);
                break;

            case "parallel_gateway":
                handleParallelGateway(instance, token, current, def, userId);
                break;

            case "intermediate_event":
                handleIntermediateEvent(instance, token, current, def, userId);
                break;

            case "sub_process":
                handleSubProcess(instance, token, current, def, userId);
                break;

            case "business_rule_task":
                handleBusinessRuleTask(instance, token, current, def, userId);
                break;

            case "boundary_event":
                // B2 — un boundary_event NO recibe tokens entrantes normales
                // (se dispara por interrupción de su activity adjunta). Si
                // un token llega acá, lo tratamos como passthrough con warn.
                log.warn("Token entered boundary_event '{}' directly — unusual, treating as passthrough",
                    current.code());
                audit(instance, "boundary_event.unexpected_entry", current.id(), token.getId(), userId,
                    Map.of("elementCode", current.code()));
                consumeAndMoveToNext(instance, token, current, def, userId);
                break;

            default:
                log.warn("Unsupported flow_element type '{}' — token blocked at {}",
                    current.type(), current.code());
                token.setLifecycle("waiting");
                tokenRepo.save(token);
        }
    }

    // ─── consume + move ─────────────────────────────────────────────────────

    private void consumeAndMoveToNext(ProcessInstance instance, Token token,
                                       ProcessDefinition.FlowElement current,
                                       ProcessDefinition def, UUID userId) {
        List<ProcessDefinition.SequenceFlow> outgoing = def.outgoingFlows(current.id());
        if (outgoing.isEmpty()) {
            log.warn("No outgoing flow from {} — token waiting", current.code());
            token.setLifecycle("waiting");
            tokenRepo.save(token);
            return;
        }
        // Para non-gateway elements: tomamos el primero (debería ser único).
        ProcessDefinition.SequenceFlow next = outgoing.get(0);
        moveTokenToElement(instance, token, next.targetId(), def, userId);
    }

    private void moveTokenToElement(ProcessInstance instance, Token token,
                                     UUID nextElementId, ProcessDefinition def, UUID userId) {
        token.setCurrentElementId(nextElementId);
        token.setEnteredAt(OffsetDateTime.now());
        token.setUpdatedAt(OffsetDateTime.now());
        tokenRepo.save(token);

        ProcessDefinition.FlowElement next = def.findElementById(nextElementId);
        audit(instance, "token.entered", nextElementId, token.getId(), userId,
            Map.of("elementCode", next != null ? next.code() : "?",
                   "elementType", next != null ? next.type() : "?"));

        // Recursión hasta wait state
        advanceToken(instance, token, def, userId);
    }

    // ─── end_event ──────────────────────────────────────────────────────────

    private void handleEndEvent(ProcessInstance instance, Token token,
                                 ProcessDefinition.FlowElement endEvent, UUID userId) {
        token.setLifecycle("consumed");
        token.setUpdatedAt(OffsetDateTime.now());
        tokenRepo.save(token);
        audit(instance, "token.consumed", endEvent.id(), token.getId(), userId,
            Map.of("elementCode", endEvent.code()));

        // Si NO hay más tokens activos en la instance → mark completed
        List<Token> stillActive = tokenRepo.findByProcessinstanceIdAndLifecycleIn(
            instance.getId(), List.of("active", "waiting"));
        if (stillActive.isEmpty()) {
            instance.setLifecycle("completed");
            instance.setEndedAt(OffsetDateTime.now());
            instance.setUpdatedAt(OffsetDateTime.now());
            instanceRepo.save(instance);
            audit(instance, "instance.ended", null, null, userId,
                Map.of("result", endEvent.config() == null ? "default"
                       : endEvent.config().getOrDefault("result", "default"),
                       "endEventCode", endEvent.code()));
            log.info("ProcessInstance {} completed via {}", instance.getId(), endEvent.code());
            metricInc("bpm.instance.ended");

            // B1 — si es child de un sub_process, notificar al parent para
            // que avance el token waiting en el sub_process flow_element.
            if (instance.getParentInstanceId() != null && instance.getParentTokenId() != null) {
                notifyParentOfSubprocessCompletion(instance, userId);
            }
        }
    }

    // ─── user_task ──────────────────────────────────────────────────────────

    private void handleUserTask(ProcessInstance instance, Token token,
                                 ProcessDefinition.FlowElement userTask,
                                 ProcessDefinition def, UUID userId) {
        // Token queda waiting hasta que se complete la task
        token.setLifecycle("waiting");
        token.setUpdatedAt(OffsetDateTime.now());
        tokenRepo.save(token);

        // Snapshot de variables current para el input_data de la task
        Map<String, Object> inputData = currentVariablesAsMap(instance);

        TaskInstance task = new TaskInstance();
        OffsetDateTime now = OffsetDateTime.now();
        task.setId(UUID.randomUUID());
        task.setTenantId(instance.getTenantId());
        task.setProcessinstanceId(instance.getId());
        task.setFlowelementId(userTask.id());
        task.setTokenId(token.getId());
        task.setLifecycle("created");
        task.setPriority(50);
        task.setInputData(inputData);
        // A3 — AssignmentRule MVP: assignedUser default = quien arrancó el
        // proceso. Cuando llegue Iter 5 AssignmentRule, acá se consulta
        // bpm_hum_assignmentrule del flowelement (user / role / group / expr).
        task.setAssignedUserId(instance.getStartedById());
        task.setAssignedAt(now);
        task.setStateId(DEFAULT_STATE_ACTIVE);
        task.setCreatedAt(now);
        task.setUpdatedAt(now);
        taskRepo.save(task);

        ProcessDefinition.TaskForm form = def.findTaskFormByFlowElement(userTask.id());

        // B2 — programar boundary timers adjuntos (si hay)
        int boundariesScheduled = scheduleBoundaryTimers(instance, token, userTask, task.getId(), def, userId);

        audit(instance, "task.created", userTask.id(), token.getId(), userId, Map.of(
            "taskInstanceId", task.getId().toString(),
            "elementCode", userTask.code(),
            "assignedUserId", task.getAssignedUserId() != null
                ? task.getAssignedUserId().toString() : "(none)",
            "formEntityDefCode", form != null ? form.entityDefCode() : "(none)",
            "boundariesScheduled", boundariesScheduled
        ));
        log.info("UserTask '{}' created → task {} waiting (form={}, assignedTo={}, boundaries={})",
            userTask.code(), task.getId(),
            form != null ? form.entityDefCode() : "none",
            task.getAssignedUserId(),
            boundariesScheduled);
        metricInc("bpm.task.created");
    }

    // ─── boundary_event (B2 — interrupting timer sobre user_task) ───────────

    /**
     * Para cada boundary_event con type=timer adjunto al activity, schedula
     * un JobExecutor con fire_at = now + delaySeconds. El job lleva en su
     * config_jsonb el marker `boundary: true` + `boundaryElementId` +
     * `attachedTaskInstanceId` (si user_task) para que fireTimerJob pueda
     * discriminar boundary vs intermediate timer.
     *
     * Si no hay boundaries para esta activity → devuelve 0 (no-op).
     *
     * NOTA: scope MVP B2 → solo timer; error/escalation pendiente.
     */
    @SuppressWarnings("unchecked")
    private int scheduleBoundaryTimers(ProcessInstance instance, Token activityToken,
                                        ProcessDefinition.FlowElement activity,
                                        UUID attachedTaskInstanceId,
                                        ProcessDefinition def, UUID userId) {
        List<ProcessDefinition.FlowElement> boundaries = def.findBoundariesFor(activity.code());
        if (boundaries.isEmpty()) return 0;

        int scheduled = 0;
        OffsetDateTime now = OffsetDateTime.now();
        for (ProcessDefinition.FlowElement b : boundaries) {
            Map<String, Object> cfg = b.config();
            Map<String, Object> timerCfg = cfg == null ? null
                : (Map<String, Object>) cfg.get("timer");
            if (timerCfg == null) {
                log.warn("boundary_event '{}' attached to '{}' has no timer config — skip (MVP only supports timer)",
                    b.code(), activity.code());
                continue;
            }
            long delaySeconds;
            Object delayObj = timerCfg.get("delaySeconds");
            try {
                delaySeconds = delayObj instanceof Number n ? n.longValue()
                    : Long.parseLong(String.valueOf(delayObj));
            } catch (NumberFormatException e) {
                log.error("boundary_event '{}' invalid delaySeconds: {}", b.code(), delayObj);
                continue;
            }

            Map<String, Object> boundaryCfg = cfg == null ? Map.of()
                : (Map<String, Object>) cfg.getOrDefault("boundary", Map.of());
            boolean interrupting = !(Boolean.FALSE.equals(boundaryCfg.get("interrupting")));

            JobExecutor job = new JobExecutor();
            job.setId(UUID.randomUUID());
            job.setTenantId(instance.getTenantId());
            job.setProcessinstanceId(instance.getId());
            job.setTokenId(activityToken.getId());
            job.setJobType("timer");
            job.setFireAt(now.plusSeconds(delaySeconds));
            Map<String, Object> jobCfg = new LinkedHashMap<>();
            jobCfg.put("boundary", true);
            jobCfg.put("boundaryElementId", b.id().toString());
            jobCfg.put("boundaryElementCode", b.code());
            jobCfg.put("attachedToElementId", activity.id().toString());
            jobCfg.put("attachedToElementCode", activity.code());
            if (attachedTaskInstanceId != null)
                jobCfg.put("attachedTaskInstanceId", attachedTaskInstanceId.toString());
            jobCfg.put("interrupting", interrupting);
            jobCfg.put("delaySeconds", delaySeconds);
            job.setConfig(jobCfg);
            job.setLifecycle("scheduled");
            job.setRetries(0);
            job.setMaxRetries(3);
            job.setStateId(DEFAULT_STATE_ACTIVE);
            job.setCreatedAt(now);
            job.setUpdatedAt(now);
            jobRepo.save(job);

            audit(instance, "boundary.scheduled", b.id(), activityToken.getId(), userId, Map.of(
                "boundaryCode", b.code(),
                "attachedToCode", activity.code(),
                "jobId", job.getId().toString(),
                "fireAt", job.getFireAt().toString(),
                "interrupting", interrupting
            ));
            scheduled++;
        }
        return scheduled;
    }

    /**
     * Cancela todos los boundary jobs `scheduled` del token de la activity.
     * Llamado cuando la activity completa normalmente (completeTask, etc).
     * Idempotente: si no hay jobs o ya están cancelled/fired, no hace nada.
     */
    private int cancelBoundaryJobsForToken(UUID tokenId) {
        if (tokenId == null) return 0;
        List<JobExecutor> jobs = jobRepo.findByTokenIdAndLifecycle(tokenId, "scheduled");
        if (jobs.isEmpty()) return 0;
        OffsetDateTime now = OffsetDateTime.now();
        int cancelled = 0;
        for (JobExecutor j : jobs) {
            // Solo cancelar boundary jobs — no afectar timer events normales
            // (que también usan tokenId pero con boundary=false en config).
            Object boundaryFlag = j.getConfig() == null ? null : j.getConfig().get("boundary");
            if (!Boolean.TRUE.equals(boundaryFlag)) continue;
            j.setLifecycle("cancelled");
            j.setUpdatedAt(now);
            jobRepo.save(j);
            cancelled++;
        }
        return cancelled;
    }

    // ─── exclusive_gateway ──────────────────────────────────────────────────

    private void handleExclusiveGateway(ProcessInstance instance, Token token,
                                         ProcessDefinition.FlowElement gateway,
                                         ProcessDefinition def, UUID userId) {
        Map<String, Object> vars = currentVariablesAsMap(instance);
        List<ProcessDefinition.SequenceFlow> outgoing = def.outgoingFlows(gateway.id());

        ProcessDefinition.SequenceFlow chosen = null;
        ProcessDefinition.SequenceFlow defaultFlow = null;
        for (ProcessDefinition.SequenceFlow sf : outgoing) {
            if (sf.conditionExpr() == null || sf.conditionExpr().isBlank()) {
                if (defaultFlow == null) defaultFlow = sf;
                continue;
            }
            if (evaluateCondition(sf.conditionExpr(), vars)) {
                chosen = sf;
                break;
            }
        }
        if (chosen == null) chosen = defaultFlow;
        if (chosen == null) {
            log.error("No outgoing flow matched + no default in gateway {} — token waiting",
                gateway.code());
            token.setLifecycle("waiting");
            tokenRepo.save(token);
            audit(instance, "gateway.no_match", gateway.id(), token.getId(), userId,
                Map.of("gatewayCode", gateway.code(), "vars", vars));
            return;
        }
        audit(instance, "gateway.evaluated", gateway.id(), token.getId(), userId, Map.of(
            "gatewayCode", gateway.code(),
            "chosenFlowId", chosen.id().toString(),
            "targetCode", chosen.targetCode(),
            "conditionExpr", chosen.conditionExpr() == null ? "(default)" : chosen.conditionExpr()
        ));
        metricInc("bpm.gateway.exclusive");
        moveTokenToElement(instance, token, chosen.targetId(), def, userId);
    }

    // ─── cancel instance (C1 — soft cancel preservando audit) ────────────────

    /**
     * Cancela una instance activa: marca instance.lifecycle='cancelled',
     * mata tokens vivos (active/waiting), cancela tasks vivas (created/
     * assigned/in_progress), cancela jobs scheduled (boundaries + timers),
     * marca correlations waiting como cancelled, audita el evento con
     * reason opcional. Preserva audit log para compliance.
     *
     * Devuelve counts de qué actualizó. Lanza IllegalStateException si la
     * instance no está en estado active (idempotencia).
     */
    @Transactional
    public Map<String, Object> cancelInstance(UUID instanceId, String reason, UUID userId) {
        tenantSession.applyToCurrentTransaction();
        ProcessInstance instance = instanceRepo.findById(instanceId)
            .orElseThrow(() -> new IllegalArgumentException("Instance not found: " + instanceId));
        if (!"active".equals(instance.getLifecycle())) {
            throw new IllegalStateException("Cannot cancel instance " + instanceId
                + " — lifecycle is '" + instance.getLifecycle() + "' (must be 'active')");
        }

        OffsetDateTime now = OffsetDateTime.now();
        int tokensCancelled = 0;
        int tasksCancelled = 0;
        int jobsCancelled = 0;
        int corrsCancelled = 0;

        // Tokens vivos → consumed (consideramos cancelled como subtipo de consumed)
        for (Token t : tokenRepo.findByProcessinstanceIdAndLifecycleIn(
                instance.getId(), List.of("active", "waiting"))) {
            t.setLifecycle("consumed");
            t.setUpdatedAt(now);
            tokenRepo.save(t);
            tokensCancelled++;
        }

        // Tasks vivas → cancelled
        for (TaskInstance task : taskRepo.findByProcessinstanceId(instance.getId())) {
            if (List.of("created", "assigned", "in_progress").contains(task.getLifecycle())) {
                task.setLifecycle("cancelled");
                task.setUpdatedAt(now);
                taskRepo.save(task);
                tasksCancelled++;
            }
        }

        // Jobs scheduled → cancelled (incluye boundary + intermediate timers)
        for (JobExecutor job : jobRepo.findByProcessinstanceIdAndLifecycle(instance.getId(), "scheduled")) {
            job.setLifecycle("cancelled");
            job.setUpdatedAt(now);
            jobRepo.save(job);
            jobsCancelled++;
        }

        // Correlations waiting → cancelled
        for (MessageCorrelation mc : msgCorrRepo.findByProcessinstanceIdAndLifecycle(instance.getId(), "waiting")) {
            mc.setLifecycle("cancelled");
            mc.setUpdatedAt(now);
            msgCorrRepo.save(mc);
            corrsCancelled++;
        }

        instance.setLifecycle("cancelled");
        instance.setEndedAt(now);
        instance.setUpdatedAt(now);
        instanceRepo.save(instance);

        Map<String, Object> auditData = new LinkedHashMap<>();
        auditData.put("reason", reason == null ? "(no reason given)" : reason);
        auditData.put("tokensCancelled", tokensCancelled);
        auditData.put("tasksCancelled", tasksCancelled);
        auditData.put("jobsCancelled", jobsCancelled);
        auditData.put("correlationsCancelled", corrsCancelled);
        audit(instance, "instance.cancelled", null, null, userId, auditData);
        log.info("Instance {} cancelled (reason={}) — tokens={}, tasks={}, jobs={}, corrs={}",
            instanceId, reason, tokensCancelled, tasksCancelled, jobsCancelled, corrsCancelled);
        metricInc("bpm.instance.cancelled");

        return auditData;
    }

    // ─── parallel_gateway (A1 — AND-split / AND-join) ────────────────────────

    /**
     * Maneja parallel_gateway según su "forma" topológica en el grafo:
     *
     *   1 in / N out  → SPLIT: consume el token actual, emite N child tokens
     *                   (uno por outgoing), cada uno con parent_token_id = token
     *                   actual. Avanza recursivamente cada child.
     *   M in / 1 out  → JOIN : marca el token como waiting en el gateway. Si
     *                   YA llegaron M tokens hermanos (mismo parent_token_id)
     *                   esperando, consume todos y emite 1 token en el outgoing
     *                   con parent_token_id = "grandparent" (el padre del nivel
     *                   superior, restaurando el nivel de paralelismo).
     *   1 in / 1 out  → passthrough degenerado (avanza como un nodo normal).
     *   M in / N out  → no soportado: el modelado lo desaconseja. Token waiting
     *                   + warning en log.
     *
     * Garantía: en SPLIT, todos los siblings comparten parentTokenId == el id
     * del token que se splitió. En JOIN, esa relación se usa para identificar
     * "mi grupo" y no confundirme con tokens de otros splits concurrentes.
     */
    private void handleParallelGateway(ProcessInstance instance, Token token,
                                        ProcessDefinition.FlowElement gateway,
                                        ProcessDefinition def, UUID userId) {
        List<ProcessDefinition.SequenceFlow> outgoing = def.outgoingFlows(gateway.id());
        List<ProcessDefinition.SequenceFlow> incoming = def.incomingFlows(gateway.id());
        OffsetDateTime now = OffsetDateTime.now();

        if (outgoing.size() > 1 && incoming.size() <= 1) {
            // ── SPLIT ────────────────────────────────────────────────────────
            token.setLifecycle("consumed");
            token.setUpdatedAt(now);
            tokenRepo.save(token);
            audit(instance, "gateway.split", gateway.id(), token.getId(), userId, Map.of(
                "gatewayCode", gateway.code(),
                "branches", outgoing.size()
            ));
            metricInc("bpm.gateway.split");
            for (ProcessDefinition.SequenceFlow sf : outgoing) {
                Token child = newToken(instance, sf.targetId(), token.getId());
                tokenRepo.save(child);
                ProcessDefinition.FlowElement target = def.findElementById(sf.targetId());
                audit(instance, "token.entered", sf.targetId(), child.getId(), userId, Map.of(
                    "elementCode", target != null ? target.code() : "?",
                    "elementType", target != null ? target.type() : "?",
                    "fromSplit", gateway.code()
                ));
                advanceToken(instance, child, def, userId);
            }
            return;
        }

        if (incoming.size() > 1 && outgoing.size() == 1) {
            // ── JOIN ─────────────────────────────────────────────────────────
            token.setLifecycle("waiting");
            token.setUpdatedAt(now);
            tokenRepo.save(token);
            audit(instance, "gateway.join.arrived", gateway.id(), token.getId(), userId, Map.of(
                "gatewayCode", gateway.code()
            ));

            UUID myParent = token.getParentTokenId();
            List<Token> arrived = tokenRepo.findByProcessinstanceIdAndCurrentElementIdAndLifecycle(
                instance.getId(), gateway.id(), "waiting");
            List<Token> siblings = arrived.stream()
                .filter(t -> Objects.equals(t.getParentTokenId(), myParent))
                .toList();

            if (siblings.size() < incoming.size()) {
                log.debug("parallel_gateway JOIN {} — waiting siblings: {}/{}",
                    gateway.code(), siblings.size(), incoming.size());
                return; // esperar más
            }

            // Todos llegaron — consume siblings + emit nuevo token en outgoing
            for (Token sib : siblings) {
                sib.setLifecycle("consumed");
                sib.setUpdatedAt(now);
                tokenRepo.save(sib);
            }

            // grandparent restaura el nivel de paralelismo anterior al SPLIT
            UUID grandparent = myParent == null ? null
                : tokenRepo.findById(myParent).map(Token::getParentTokenId).orElse(null);

            audit(instance, "gateway.join.completed", gateway.id(), null, userId, Map.of(
                "gatewayCode", gateway.code(),
                "branchesJoined", siblings.size()
            ));
            metricInc("bpm.gateway.join");

            ProcessDefinition.SequenceFlow out = outgoing.get(0);
            Token outToken = newToken(instance, out.targetId(), grandparent);
            tokenRepo.save(outToken);
            ProcessDefinition.FlowElement target = def.findElementById(out.targetId());
            audit(instance, "token.entered", out.targetId(), outToken.getId(), userId, Map.of(
                "elementCode", target != null ? target.code() : "?",
                "elementType", target != null ? target.type() : "?",
                "fromJoin", gateway.code()
            ));
            advanceToken(instance, outToken, def, userId);
            return;
        }

        if (outgoing.size() == 1 && incoming.size() <= 1) {
            // ── passthrough degenerado ───────────────────────────────────────
            log.debug("parallel_gateway {} is 1-in 1-out (passthrough)", gateway.code());
            consumeAndMoveToNext(instance, token, gateway, def, userId);
            return;
        }

        // M-in N-out: no soportado en MVP — bloqueamos el token
        log.warn("parallel_gateway {} has unsupported shape in={} out={} — token blocked",
            gateway.code(), incoming.size(), outgoing.size());
        token.setLifecycle("waiting");
        tokenRepo.save(token);
        audit(instance, "gateway.unsupported_shape", gateway.id(), token.getId(), userId, Map.of(
            "gatewayCode", gateway.code(),
            "incoming", incoming.size(),
            "outgoing", outgoing.size()
        ));
    }

    // ─── intermediate_event (A2 — timer + message + signal) ─────────────────

    /**
     * Maneja intermediate_event. Dispatch por subtype en config:
     *   { "timer":   { "delaySeconds": 5 } }                    → A2 timer
     *   { "message": { "messageCode": "x", "correlationKey": "${var}" } } → A2 message
     *   { "signal":  { "signalCode": "y" } }                    → A2 signal
     *
     * Si falta cualquier subtype reconocido → passthrough (avanza como nodo
     * normal) para no bloquear procesos con placeholder events.
     *
     * El correlationKey de message es opcional: puede ser literal (ej "X42")
     * o JEXL expr `${varName}` que se evalúa contra las variables de la
     * processinstance al momento de crear la correlation.
     */
    @SuppressWarnings("unchecked")
    private void handleIntermediateEvent(ProcessInstance instance, Token token,
                                          ProcessDefinition.FlowElement event,
                                          ProcessDefinition def, UUID userId) {
        Map<String, Object> cfg = event.config();
        if (cfg != null && cfg.get("timer") instanceof Map) {
            handleTimerEvent(instance, token, event, (Map<String, Object>) cfg.get("timer"), userId);
            return;
        }
        if (cfg != null && cfg.get("message") instanceof Map) {
            handleMessageEvent(instance, token, event, (Map<String, Object>) cfg.get("message"), userId);
            return;
        }
        if (cfg != null && cfg.get("signal") instanceof Map) {
            handleSignalEvent(instance, token, event, (Map<String, Object>) cfg.get("signal"), userId);
            return;
        }

        log.warn("intermediate_event '{}' has no recognized subtype config — passthrough",
            event.code());
        audit(instance, "intermediate_event.passthrough", event.id(), token.getId(), userId,
            Map.of("elementCode", event.code()));
        consumeAndMoveToNext(instance, token, event, def, userId);
    }

    private void handleTimerEvent(ProcessInstance instance, Token token,
                                   ProcessDefinition.FlowElement event,
                                   Map<String, Object> timerCfg, UUID userId) {
        long delaySeconds;
        Object delayObj = timerCfg.get("delaySeconds");
        if (delayObj instanceof Number n) {
            delaySeconds = n.longValue();
        } else {
            try {
                delaySeconds = Long.parseLong(String.valueOf(delayObj));
            } catch (NumberFormatException e) {
                log.error("Invalid delaySeconds in timer config of {}: {}", event.code(), delayObj);
                token.setLifecycle("waiting");
                tokenRepo.save(token);
                audit(instance, "intermediate_event.config_error", event.id(), token.getId(), userId,
                    Map.of("elementCode", event.code(), "delayRaw", String.valueOf(delayObj)));
                return;
            }
        }

        OffsetDateTime now = OffsetDateTime.now();
        OffsetDateTime fireAt = now.plusSeconds(delaySeconds);

        token.setLifecycle("waiting");
        token.setUpdatedAt(now);
        tokenRepo.save(token);

        JobExecutor job = new JobExecutor();
        job.setId(UUID.randomUUID());
        job.setTenantId(instance.getTenantId());
        job.setProcessinstanceId(instance.getId());
        job.setTokenId(token.getId());
        job.setJobType("timer");
        job.setFireAt(fireAt);
        job.setConfig(Map.of(
            "elementId", event.id().toString(),
            "elementCode", event.code(),
            "delaySeconds", delaySeconds
        ));
        job.setLifecycle("scheduled");
        job.setRetries(0);
        job.setMaxRetries(3);
        job.setStateId(DEFAULT_STATE_ACTIVE);
        job.setCreatedAt(now);
        job.setUpdatedAt(now);
        jobRepo.save(job);

        audit(instance, "timer.scheduled", event.id(), token.getId(), userId, Map.of(
            "elementCode", event.code(),
            "jobId", job.getId().toString(),
            "fireAt", fireAt.toString(),
            "delaySeconds", delaySeconds
        ));
        log.info("intermediate_event '{}' scheduled timer job {} fireAt={}",
            event.code(), job.getId(), fireAt);
        metricInc("bpm.timer.scheduled");
    }

    private void handleMessageEvent(ProcessInstance instance, Token token,
                                     ProcessDefinition.FlowElement event,
                                     Map<String, Object> msgCfg, UUID userId) {
        String messageCode = stringOr(msgCfg.get("messageCode"), null);
        if (messageCode == null || messageCode.isBlank()) {
            log.error("intermediate_event '{}' message config without messageCode", event.code());
            token.setLifecycle("waiting");
            tokenRepo.save(token);
            audit(instance, "intermediate_event.config_error", event.id(), token.getId(), userId,
                Map.of("elementCode", event.code(), "reason", "missing messageCode"));
            return;
        }

        // correlationKey opcional: literal string o `${varName}` JEXL expr.
        String rawKey = stringOr(msgCfg.get("correlationKey"), "");
        String resolvedKey = resolveExpression(rawKey, currentVariablesAsMap(instance));
        if (resolvedKey == null || resolvedKey.isBlank()) {
            // sin correlationKey, queda como wildcard "*" — primer correlate matcheará
            resolvedKey = "*";
        }

        OffsetDateTime now = OffsetDateTime.now();
        token.setLifecycle("waiting");
        token.setUpdatedAt(now);
        tokenRepo.save(token);

        MessageCorrelation mc = new MessageCorrelation();
        mc.setId(UUID.randomUUID());
        mc.setTenantId(instance.getTenantId());
        mc.setProcessinstanceId(instance.getId());
        mc.setTokenId(token.getId());
        mc.setMessagedefId(MessageCorrelation.messageRefId(messageCode));
        mc.setCorrelationKey(resolvedKey);
        mc.setLifecycle("waiting");
        mc.setStateId(DEFAULT_STATE_ACTIVE);
        mc.setCreatedAt(now);
        mc.setUpdatedAt(now);
        msgCorrRepo.save(mc);

        audit(instance, "message.subscribed", event.id(), token.getId(), userId, Map.of(
            "elementCode", event.code(),
            "messageCode", messageCode,
            "correlationKey", resolvedKey,
            "correlationId", mc.getId().toString()
        ));
        log.info("intermediate_event '{}' waiting for message '{}' (key={})",
            event.code(), messageCode, resolvedKey);
        metricInc("bpm.message.subscribed");
    }

    private void handleSignalEvent(ProcessInstance instance, Token token,
                                    ProcessDefinition.FlowElement event,
                                    Map<String, Object> sigCfg, UUID userId) {
        String signalCode = stringOr(sigCfg.get("signalCode"), null);
        if (signalCode == null || signalCode.isBlank()) {
            log.error("intermediate_event '{}' signal config without signalCode", event.code());
            token.setLifecycle("waiting");
            tokenRepo.save(token);
            audit(instance, "intermediate_event.config_error", event.id(), token.getId(), userId,
                Map.of("elementCode", event.code(), "reason", "missing signalCode"));
            return;
        }

        OffsetDateTime now = OffsetDateTime.now();
        token.setLifecycle("waiting");
        token.setUpdatedAt(now);
        tokenRepo.save(token);

        MessageCorrelation mc = new MessageCorrelation();
        mc.setId(UUID.randomUUID());
        mc.setTenantId(instance.getTenantId());
        mc.setProcessinstanceId(instance.getId());
        mc.setTokenId(token.getId());
        mc.setMessagedefId(MessageCorrelation.signalRefId(signalCode));
        mc.setCorrelationKey(MessageCorrelation.BROADCAST_KEY);
        mc.setLifecycle("waiting");
        mc.setStateId(DEFAULT_STATE_ACTIVE);
        mc.setCreatedAt(now);
        mc.setUpdatedAt(now);
        msgCorrRepo.save(mc);

        audit(instance, "signal.subscribed", event.id(), token.getId(), userId, Map.of(
            "elementCode", event.code(),
            "signalCode", signalCode,
            "correlationId", mc.getId().toString()
        ));
        log.info("intermediate_event '{}' subscribed to signal '{}'",
            event.code(), signalCode);
        metricInc("bpm.signal.subscribed");
    }

    // ─── sub_process (B1 — call activity) ──────────────────────────────────

    /**
     * Maneja un flow_element `sub_process` (call activity).
     *
     * Config esperada (en bpm_pro_flowelement_config como JSON):
     *   {
     *     "callActivity": {
     *       "calledProcessversionId": "<uuid>",          // MVP: requerido
     *       "passVariables":   ["userId", "email"],       // opcional
     *       "returnVariables": ["validationResult"]       // opcional
     *     }
     *   }
     *
     * Flujo:
     *   1. Token del parent → waiting
     *   2. Construir payload con las `passVariables` snapshotted del parent
     *   3. Spawn child ProcessInstance via startProcessInternal con parent refs
     *   4. Audit `subprocess.spawned`
     *
     * El child puede completarse instantáneamente dentro de la misma transacción
     * (procesos de solo service_tasks+gateways) o quedar en wait state. En
     * ambos casos el comportamiento es correcto: si completa instantáneo,
     * handleEndEvent del child llama notifyParentOfSubprocessCompletion que
     * reactiva el token del parent ya marcado waiting (sin race condition).
     */
    @SuppressWarnings("unchecked")
    private void handleSubProcess(ProcessInstance instance, Token token,
                                   ProcessDefinition.FlowElement subProcess,
                                   ProcessDefinition def, UUID userId) {
        Map<String, Object> cfg = subProcess.config();
        Map<String, Object> callCfg = cfg == null ? null
            : (Map<String, Object>) cfg.get("callActivity");

        if (callCfg == null) {
            log.warn("sub_process '{}' has no callActivity config — passthrough",
                subProcess.code());
            audit(instance, "subprocess.passthrough", subProcess.id(), token.getId(), userId,
                Map.of("elementCode", subProcess.code()));
            consumeAndMoveToNext(instance, token, subProcess, def, userId);
            return;
        }

        String calledVerId = stringOr(callCfg.get("calledProcessversionId"), null);
        if (calledVerId == null || calledVerId.isBlank()) {
            log.error("sub_process '{}' callActivity missing calledProcessversionId",
                subProcess.code());
            token.setLifecycle("waiting");
            tokenRepo.save(token);
            audit(instance, "subprocess.config_error", subProcess.id(), token.getId(), userId,
                Map.of("elementCode", subProcess.code(),
                       "reason", "missing calledProcessversionId"));
            return;
        }

        UUID calledProcessVersionId;
        try {
            calledProcessVersionId = UUID.fromString(calledVerId);
        } catch (IllegalArgumentException e) {
            log.error("sub_process '{}' calledProcessversionId not a UUID: {}",
                subProcess.code(), calledVerId);
            token.setLifecycle("waiting");
            tokenRepo.save(token);
            audit(instance, "subprocess.config_error", subProcess.id(), token.getId(), userId,
                Map.of("elementCode", subProcess.code(),
                       "calledProcessversionId", calledVerId,
                       "reason", "not a valid UUID"));
            return;
        }

        // Snapshot vars del parent → payload para el child
        Map<String, Object> parentVars = currentVariablesAsMap(instance);
        Map<String, Object> childPayload = new LinkedHashMap<>();
        Object passVarsObj = callCfg.get("passVariables");
        if (passVarsObj instanceof List<?> list) {
            for (Object v : list) {
                String name = String.valueOf(v);
                if (parentVars.containsKey(name)) {
                    childPayload.put(name, parentVars.get(name));
                }
            }
        }

        // Token del parent: waiting hasta que el child complete
        OffsetDateTime now = OffsetDateTime.now();
        token.setLifecycle("waiting");
        token.setUpdatedAt(now);
        tokenRepo.save(token);

        audit(instance, "subprocess.spawning", subProcess.id(), token.getId(), userId, Map.of(
            "elementCode", subProcess.code(),
            "calledProcessversionId", calledProcessVersionId.toString(),
            "passVariables", childPayload.keySet()
        ));

        // Spawn child (puede completarse instantáneo dentro de esta transaction)
        ProcessInstance child;
        try {
            child = startProcessInternal(calledProcessVersionId, childPayload,
                null /*bearer*/, instance.getTenantId(), userId,
                instance.getId(), token.getId());
        } catch (Exception ex) {
            log.error("sub_process '{}' spawn failed", subProcess.code(), ex);
            // Reactivar token y pasar al siguiente igual (fallback graceful)
            token.setLifecycle("active");
            token.setUpdatedAt(OffsetDateTime.now());
            tokenRepo.save(token);
            audit(instance, "subprocess.spawn_failed", subProcess.id(), token.getId(), userId,
                Map.of("elementCode", subProcess.code(), "error", ex.getMessage()));
            consumeAndMoveToNext(instance, token, subProcess, def, userId);
            return;
        }

        audit(instance, "subprocess.spawned", subProcess.id(), token.getId(), userId, Map.of(
            "elementCode", subProcess.code(),
            "childInstanceId", child.getId().toString(),
            "childLifecycle", child.getLifecycle()
        ));
        log.info("sub_process '{}' spawned child {} (lifecycle={})",
            subProcess.code(), child.getId(), child.getLifecycle());
        metricInc("bpm.subprocess.spawned");
    }

    /**
     * Llamado desde handleEndEvent del child cuando termina. Copia las
     * `returnVariables` declaradas en el sub_process flow_element del parent
     * desde el child al parent, reactiva el token del parent y avanza.
     */
    @SuppressWarnings("unchecked")
    private void notifyParentOfSubprocessCompletion(ProcessInstance child, UUID userId) {
        UUID parentInstanceId = child.getParentInstanceId();
        UUID parentTokenId    = child.getParentTokenId();
        ProcessInstance parent = instanceRepo.findById(parentInstanceId).orElse(null);
        if (parent == null) {
            log.warn("notifyParent: parent instance {} not found", parentInstanceId);
            return;
        }
        if (!"active".equals(parent.getLifecycle())) {
            log.info("notifyParent: parent {} no longer active ({})",
                parent.getId(), parent.getLifecycle());
            return;
        }
        Token parentToken = tokenRepo.findById(parentTokenId).orElse(null);
        if (parentToken == null || !"waiting".equals(parentToken.getLifecycle())) {
            log.warn("notifyParent: parent token {} gone or not waiting", parentTokenId);
            return;
        }

        ProcessDefinition parentDef = loader.load(parent.getProcessversionId(),
            null, parent.getTenantId());
        ProcessDefinition.FlowElement subProcessElement =
            parentDef.findElementById(parentToken.getCurrentElementId());
        if (subProcessElement == null) {
            log.error("notifyParent: parent sub_process element gone");
            return;
        }

        // Copia returnVariables del child al parent
        Map<String, Object> cfg = subProcessElement.config();
        Map<String, Object> callCfg = cfg == null ? null
            : (Map<String, Object>) cfg.get("callActivity");
        if (callCfg != null && callCfg.get("returnVariables") instanceof List<?> retList) {
            Map<String, Object> childVars = currentVariablesAsMap(child);
            int copied = 0;
            for (Object v : retList) {
                String name = String.valueOf(v);
                if (childVars.containsKey(name)) {
                    setVariable(parent, name, childVars.get(name));
                    copied++;
                }
            }
            log.debug("notifyParent: copied {} returnVariables from child {} to parent {}",
                copied, child.getId(), parent.getId());
        }

        OffsetDateTime now = OffsetDateTime.now();
        parentToken.setLifecycle("active");
        parentToken.setUpdatedAt(now);
        tokenRepo.save(parentToken);
        audit(parent, "subprocess.completed", subProcessElement.id(), parentToken.getId(), userId,
            Map.of(
                "elementCode", subProcessElement.code(),
                "childInstanceId", child.getId().toString(),
                "childLifecycle", child.getLifecycle()
            ));

        consumeAndMoveToNext(parent, parentToken, subProcessElement, parentDef, userId);
    }

    // ─── business_rule_task (B3 — DMN) ───────────────────────────────────────

    /**
     * Maneja un flow_element `business_rule_task` invocando el DmnEvaluator
     * sobre la decisiondef referenciada por config.decisionRef.
     *
     * Convención de config (en bpm_pro_flowelement_config):
     *   { "decisionRef": "credit_approval" }
     *
     * Las vars del processinstance se pasan directo como inputs (matcheo
     * por var_name). Los outputs de la rule ganadora se setean directo
     * como vars del processinstance (sobreescribiendo si ya existían).
     *
     * Si decisionRef falta → fallback passthrough (avanza sin evaluar)
     * + audit `business_rule.passthrough`.
     *
     * Si la decisión no matchea ninguna rule → audit `decision.no_match`
     * y avanza (no bloquea — el processdef puede tener un default).
     */
    @SuppressWarnings("unchecked")
    private void handleBusinessRuleTask(ProcessInstance instance, Token token,
                                         ProcessDefinition.FlowElement brt,
                                         ProcessDefinition def, UUID userId) {
        Map<String, Object> cfg = brt.config();
        String decisionRef = cfg == null ? null : stringOr(cfg.get("decisionRef"), null);
        if (decisionRef == null || decisionRef.isBlank()) {
            log.warn("business_rule_task '{}' has no decisionRef — passthrough", brt.code());
            audit(instance, "business_rule.passthrough", brt.id(), token.getId(), userId,
                Map.of("elementCode", brt.code(), "reason", "missing decisionRef"));
            consumeAndMoveToNext(instance, token, brt, def, userId);
            return;
        }

        DecisionDefinition decision;
        try {
            decision = decisionLoader.load(decisionRef, null, instance.getTenantId());
        } catch (Exception e) {
            log.error("business_rule_task '{}': could not load decisionRef='{}'",
                brt.code(), decisionRef, e);
            audit(instance, "business_rule.load_error", brt.id(), token.getId(), userId,
                Map.of("elementCode", brt.code(), "decisionRef", decisionRef, "error", e.getMessage()));
            // Token waiting → admin debe intervenir
            token.setLifecycle("waiting");
            tokenRepo.save(token);
            return;
        }

        Map<String, Object> inputs = currentVariablesAsMap(instance);
        DmnEvaluator.EvaluationResult result;
        try {
            result = dmnEvaluator.evaluate(decision, inputs);
        } catch (IllegalStateException e) {
            // Hit policy violation (ej unique matcheó >1)
            log.error("business_rule_task '{}': decision evaluation failed: {}",
                brt.code(), e.getMessage());
            audit(instance, "decision.error", brt.id(), token.getId(), userId, Map.of(
                "elementCode", brt.code(),
                "decisionRef", decisionRef,
                "hitPolicy", decision.hitPolicy(),
                "error", e.getMessage()
            ));
            token.setLifecycle("waiting");
            tokenRepo.save(token);
            return;
        }

        if (result.matchedRule() == null) {
            audit(instance, "decision.no_match", brt.id(), token.getId(), userId, Map.of(
                "elementCode", brt.code(),
                "decisionRef", decisionRef,
                "hitPolicy", decision.hitPolicy(),
                "rulesEvaluated", decision.rules().size(),
                "inputs", inputs
            ));
            log.info("business_rule_task '{}' decision '{}' had no match", brt.code(), decisionRef);
            consumeAndMoveToNext(instance, token, brt, def, userId);
            return;
        }

        // Mergear outputs como vars del processinstance
        for (Map.Entry<String, Object> e : result.outputs().entrySet()) {
            setVariable(instance, e.getKey(), e.getValue());
        }

        audit(instance, "decision.evaluated", brt.id(), token.getId(), userId, Map.of(
            "elementCode", brt.code(),
            "decisionRef", decisionRef,
            "hitPolicy", decision.hitPolicy(),
            "matchedRulePriority", result.matchedRule().priority(),
            "totalMatched", result.totalMatched(),
            "inputs", inputs,
            "outputs", result.outputs()
        ));
        log.info("business_rule_task '{}' decision '{}' → rule priority={} outputs={}",
            brt.code(), decisionRef, result.matchedRule().priority(), result.outputs());
        meterRegistry.counter("bpm.decision.evaluated",
            Tags.of("decision", decisionRef)).increment();

        consumeAndMoveToNext(instance, token, brt, def, userId);
    }

    /**
     * Correlate un mensaje entrante con el primer token waiting que matchee
     * (messagedef + correlationKey). Devuelve el número de tokens reactivados.
     *
     * Si la lista de matching correlations es vacía, devuelve 0 — el caller
     * (controller) puede decidir si es 404 o 200 con counter=0.
     *
     * Payload se merge en variables del processinstance antes de avanzar.
     */
    @Transactional
    public int correlateMessage(String messageCode, String correlationKey,
                                 Map<String, Object> payload) {
        if (messageCode == null || messageCode.isBlank()) return 0;
        tenantSession.applyToCurrentTransaction();
        UUID msgRefId = MessageCorrelation.messageRefId(messageCode);

        List<MessageCorrelation> matches = msgCorrRepo
            .findByMessagedefIdAndCorrelationKeyAndLifecycle(msgRefId, correlationKey, "waiting");

        // Fallback: si no hubo match exacto, buscamos correlations con wildcard "*"
        if (matches.isEmpty() && !"*".equals(correlationKey)) {
            matches = msgCorrRepo
                .findByMessagedefIdAndCorrelationKeyAndLifecycle(msgRefId, "*", "waiting");
        }

        if (matches.isEmpty()) {
            log.info("correlateMessage: no match for messageCode={} correlationKey={}",
                messageCode, correlationKey);
            return 0;
        }

        // Reactivar SOLO el primero (semántica point-to-point típica)
        MessageCorrelation mc = matches.get(0);
        return advanceFromCorrelation(mc, payload, "message.received") ? 1 : 0;
    }

    /**
     * Broadcast un signal a TODOS los tokens waiting por ese signalCode.
     * Devuelve el número de tokens reactivados.
     */
    @Transactional
    public int broadcastSignal(String signalCode, Map<String, Object> payload) {
        if (signalCode == null || signalCode.isBlank()) return 0;
        tenantSession.applyToCurrentTransaction();
        UUID sigRefId = MessageCorrelation.signalRefId(signalCode);
        List<MessageCorrelation> matches = msgCorrRepo
            .findByMessagedefIdAndLifecycle(sigRefId, "waiting");
        if (matches.isEmpty()) {
            log.info("broadcastSignal: no listener for signalCode={}", signalCode);
            return 0;
        }
        int reactivated = 0;
        for (MessageCorrelation mc : matches) {
            if (advanceFromCorrelation(mc, payload, "signal.received")) reactivated++;
        }
        log.info("broadcastSignal: signalCode={} reactivated {}/{}",
            signalCode, reactivated, matches.size());
        return reactivated;
    }

    /**
     * Helper: marca correlation matched, merge payload en variables, reactiva
     * token y avanza al siguiente. Idempotente vía chequeo de lifecycle.
     */
    private boolean advanceFromCorrelation(MessageCorrelation mc,
                                            Map<String, Object> payload,
                                            String auditEvent) {
        OffsetDateTime now = OffsetDateTime.now();
        mc.setLifecycle("matched");
        mc.setMatchedAt(now);
        mc.setMatchedPayload(payload);
        mc.setUpdatedAt(now);
        msgCorrRepo.save(mc);

        ProcessInstance instance = instanceRepo.findById(mc.getProcessinstanceId()).orElse(null);
        if (instance == null) {
            log.warn("advanceFromCorrelation: instance gone for correlation {}", mc.getId());
            return false;
        }
        if (!"active".equals(instance.getLifecycle())) {
            log.info("advanceFromCorrelation: instance {} no longer active ({})",
                instance.getId(), instance.getLifecycle());
            return false;
        }
        Token token = tokenRepo.findById(mc.getTokenId()).orElse(null);
        if (token == null || !"waiting".equals(token.getLifecycle())) {
            log.warn("advanceFromCorrelation: token gone or not waiting (id={})", mc.getTokenId());
            return false;
        }

        // Merge payload en variables (si vino payload non-null)
        if (payload != null && !payload.isEmpty()) {
            for (Map.Entry<String, Object> e : payload.entrySet()) {
                setVariable(instance, e.getKey(), e.getValue());
            }
        }

        token.setLifecycle("active");
        token.setUpdatedAt(now);
        tokenRepo.save(token);

        ProcessDefinition def = loader.load(instance.getProcessversionId(), null, instance.getTenantId());
        ProcessDefinition.FlowElement current = def.findElementById(token.getCurrentElementId());
        if (current == null) {
            log.error("advanceFromCorrelation: current element gone for token {}", token.getId());
            return false;
        }

        audit(instance, auditEvent, current.id(), token.getId(), null, Map.of(
            "elementCode", current.code(),
            "correlationId", mc.getId().toString()
        ));
        consumeAndMoveToNext(instance, token, current, def, null);
        return true;
    }

    // Helpers internos al event handling
    private static String stringOr(Object o, String dflt) {
        return o == null ? dflt : String.valueOf(o);
    }

    /**
     * Resuelve una expresión `${var}` contra el mapa de variables. Si no
     * empieza por `${`, se devuelve tal cual (literal). Si la evaluación
     * falla, devuelve el string raw (no rompe el flow).
     */
    private String resolveExpression(String raw, Map<String, Object> vars) {
        if (raw == null) return null;
        if (!raw.startsWith("${") || !raw.endsWith("}")) return raw;
        String script = raw.substring(2, raw.length() - 1);
        try {
            JexlContext ctx = new MapContext();
            for (Map.Entry<String, Object> e : vars.entrySet()) {
                ctx.set(e.getKey(), e.getValue());
            }
            Object result = jexl.createScript(script).execute(ctx);
            return result == null ? null : String.valueOf(result);
        } catch (Exception e) {
            log.warn("resolveExpression failed for '{}': {}", raw, e.getMessage());
            return raw;
        }
    }

    /**
     * Dispara un job timer vencido. Llamado por JobExecutorWorker.
     *
     * Dispatch por el flag `boundary` en config_jsonb:
     *   - boundary=true → fireBoundaryTimer (B2 — interrumpe activity y
     *                     avanza por rama escapada)
     *   - boundary=false/null → flow normal: reactiva token + advance
     *                            (intermediate_event timer del A2)
     *
     * Idempotente vía el chequeo de lifecycle del job.
     */
    @Transactional
    public void fireTimerJob(UUID jobId) {
        JobExecutor job = jobRepo.findById(jobId).orElse(null);
        if (job == null) {
            log.warn("fireTimerJob: job {} not found", jobId);
            return;
        }
        if (!"scheduled".equals(job.getLifecycle())) {
            log.debug("fireTimerJob: job {} not scheduled (lifecycle={}) — skip",
                jobId, job.getLifecycle());
            return;
        }

        // El worker scheduled corre sin TenantContextHolder seteado. Cargamos
        // el tenant del job antes de SET LOCAL para que RLS funcione.
        TenantContextHolder.set(job.getTenantId());
        tenantSession.applyToCurrentTransaction();

        OffsetDateTime now = OffsetDateTime.now();
        job.setLifecycle("firing");
        job.setUpdatedAt(now);
        jobRepo.save(job);

        try {
            ProcessInstance instance = instanceRepo.findById(job.getProcessinstanceId()).orElse(null);
            if (instance == null) {
                throw new IllegalStateException("Process instance gone: " + job.getProcessinstanceId());
            }
            if (!"active".equals(instance.getLifecycle())) {
                log.info("fireTimerJob: instance {} no longer active ({}) — cancel job",
                    instance.getId(), instance.getLifecycle());
                job.setLifecycle("cancelled");
                job.setFiredAt(now);
                job.setUpdatedAt(now);
                jobRepo.save(job);
                return;
            }
            Token token = tokenRepo.findById(job.getTokenId()).orElse(null);
            if (token == null || !"waiting".equals(token.getLifecycle())) {
                log.warn("fireTimerJob: token {} gone or no longer waiting — cancel job",
                    job.getTokenId());
                job.setLifecycle("cancelled");
                job.setFiredAt(now);
                job.setUpdatedAt(now);
                jobRepo.save(job);
                return;
            }

            // B2 — dispatch: boundary vs intermediate_event timer
            boolean isBoundary = job.getConfig() != null
                && Boolean.TRUE.equals(job.getConfig().get("boundary"));

            if (isBoundary) {
                fireBoundaryTimer(instance, token, job);
            } else {
                fireIntermediateTimer(instance, token, job);
            }

            job.setLifecycle("fired");
            job.setFiredAt(OffsetDateTime.now());
            job.setUpdatedAt(OffsetDateTime.now());
            jobRepo.save(job);

        } catch (Exception ex) {
            log.error("fireTimerJob: failed for job {}", jobId, ex);
            job.setRetries(job.getRetries() + 1);
            if (job.getRetries() >= job.getMaxRetries()) {
                job.setLifecycle("failed");
                job.setLastError(ex.getMessage());
            } else {
                // Volver a 'scheduled' con backoff exponencial (60s * 2^retries)
                job.setLifecycle("scheduled");
                job.setFireAt(OffsetDateTime.now().plusSeconds(60L * (1L << job.getRetries())));
                job.setLastError(ex.getMessage());
            }
            job.setUpdatedAt(OffsetDateTime.now());
            jobRepo.save(job);
        } finally {
            // Limpiar el holder — el scheduler reusa threads, sino el próximo
            // tick podría arrancar con el tenant del job anterior pegado.
            TenantContextHolder.clear();
        }
    }

    /** Flow normal del timer event (A2 intermediate_event). */
    private void fireIntermediateTimer(ProcessInstance instance, Token token, JobExecutor job) {
        OffsetDateTime now = OffsetDateTime.now();
        token.setLifecycle("active");
        token.setUpdatedAt(now);
        tokenRepo.save(token);

        ProcessDefinition def = loader.load(instance.getProcessversionId(), null, instance.getTenantId());
        ProcessDefinition.FlowElement current = def.findElementById(token.getCurrentElementId());
        if (current == null) return;
        audit(instance, "timer.fired", current.id(), token.getId(), null, Map.of(
            "elementCode", current.code(),
            "jobId", job.getId().toString()
        ));
        metricInc("bpm.timer.fired");
        consumeAndMoveToNext(instance, token, current, def, null);
    }

    /**
     * B2 — dispara un boundary timer: interrumpe la activity y avanza un
     * nuevo token por el outgoing del boundary_event.
     *
     * Pasos (modo interrupting=true MVP):
     *   1. Cancelar OTRAS boundary jobs del mismo token (si había varias)
     *   2. Cancelar la TaskInstance asociada (si hay) → lifecycle=cancelled
     *   3. Consumir el token de la activity (sigue waiting → consumed)
     *   4. Crear NUEVO token activo en el outgoing del boundary_event
     *   5. Avanzar el nuevo token
     *
     * Si interrupting=false (no-interrupt) → no-op por ahora (queda para futuro).
     */
    @SuppressWarnings("unchecked")
    private void fireBoundaryTimer(ProcessInstance instance, Token activityToken, JobExecutor job) {
        Map<String, Object> cfg = job.getConfig();
        boolean interrupting = !(Boolean.FALSE.equals(cfg.get("interrupting")));
        UUID boundaryElementId = UUID.fromString((String) cfg.get("boundaryElementId"));
        String boundaryCode = (String) cfg.getOrDefault("boundaryElementCode", "?");
        String attachedToCode = (String) cfg.getOrDefault("attachedToElementCode", "?");
        UUID attachedTaskId = cfg.get("attachedTaskInstanceId") == null ? null
            : UUID.fromString((String) cfg.get("attachedTaskInstanceId"));

        if (!interrupting) {
            // Non-interrupting MVP: solo creamos token paralelo y NO consumimos el activity token
            log.warn("Non-interrupting boundary {} fired — not implemented in MVP, skipping", boundaryCode);
            audit(instance, "boundary.skipped_noninterrupt", boundaryElementId,
                activityToken.getId(), null, Map.of("boundaryCode", boundaryCode));
            return;
        }

        OffsetDateTime now = OffsetDateTime.now();

        // 1. Cancelar OTRAS boundary jobs del mismo token (mismo activity)
        List<JobExecutor> siblings = jobRepo.findByTokenIdAndLifecycle(activityToken.getId(), "scheduled");
        int cancelledSiblings = 0;
        for (JobExecutor sib : siblings) {
            if (Objects.equals(sib.getId(), job.getId())) continue;  // skip self (firing)
            Object boundaryFlag = sib.getConfig() == null ? null : sib.getConfig().get("boundary");
            if (!Boolean.TRUE.equals(boundaryFlag)) continue;
            sib.setLifecycle("cancelled");
            sib.setUpdatedAt(now);
            jobRepo.save(sib);
            cancelledSiblings++;
        }

        // 2. Cancelar TaskInstance asociada (si hay)
        if (attachedTaskId != null) {
            taskRepo.findById(attachedTaskId).ifPresent(t -> {
                if (List.of("created", "assigned", "in_progress").contains(t.getLifecycle())) {
                    t.setLifecycle("cancelled");
                    t.setUpdatedAt(now);
                    taskRepo.save(t);
                    audit(instance, "task.cancelled", t.getFlowelementId(), activityToken.getId(), null,
                        Map.of("taskInstanceId", t.getId().toString(), "reason", "boundary_timer_fired"));
                }
            });
        }

        // 3. Consumir activity token
        activityToken.setLifecycle("consumed");
        activityToken.setUpdatedAt(now);
        tokenRepo.save(activityToken);

        audit(instance, "boundary.fired", boundaryElementId, activityToken.getId(), null, Map.of(
            "boundaryCode", boundaryCode,
            "attachedToCode", attachedToCode,
            "interrupted", true,
            "cancelledSiblingBoundaries", cancelledSiblings,
            "cancelledTaskInstanceId", attachedTaskId != null ? attachedTaskId.toString() : "(none)"
        ));
        metricInc("bpm.boundary.fired");

        // 4. Crear nuevo token en outgoing del boundary
        ProcessDefinition def = loader.load(instance.getProcessversionId(), null, instance.getTenantId());
        List<ProcessDefinition.SequenceFlow> outgoing = def.outgoingFlows(boundaryElementId);
        if (outgoing.isEmpty()) {
            log.warn("boundary_event '{}' has no outgoing flow — instance may stall", boundaryCode);
            return;
        }

        // El boundary token hereda el parent_token_id del activity token (mantiene
        // el scope de paralelismo si el activity estaba dentro de un SPLIT).
        Token boundaryToken = newToken(instance, outgoing.get(0).targetId(), activityToken.getParentTokenId());
        tokenRepo.save(boundaryToken);
        ProcessDefinition.FlowElement target = def.findElementById(outgoing.get(0).targetId());
        audit(instance, "token.entered", outgoing.get(0).targetId(), boundaryToken.getId(), null, Map.of(
            "elementCode", target != null ? target.code() : "?",
            "elementType", target != null ? target.type() : "?",
            "fromBoundary", boundaryCode
        ));

        // 5. Avanzar
        advanceToken(instance, boundaryToken, def, null);
    }

    /** JEXL eval. Soporta `${motivo == 'UsuPte'}` o `motivo == 'UsuPte'` (sin wrap). */
    private boolean evaluateCondition(String expr, Map<String, Object> vars) {
        String script = expr.startsWith("${") && expr.endsWith("}")
            ? expr.substring(2, expr.length() - 1)
            : expr;
        try {
            JexlContext ctx = new MapContext();
            for (Map.Entry<String, Object> e : vars.entrySet()) {
                ctx.set(e.getKey(), e.getValue());
            }
            Object result = jexl.createScript(script).execute(ctx);
            return Boolean.TRUE.equals(result);
        } catch (Exception e) {
            log.warn("JEXL evaluation failed for '{}': {}", expr, e.getMessage());
            return false;
        }
    }

    // ════════════════════════════════════════════════════════════════════════
    // Helpers — variables, audit, factories
    // ════════════════════════════════════════════════════════════════════════

    private void setVariable(ProcessInstance instance, String name, Object value) {
        Variable var = varRepo.findByProcessinstanceIdAndVarName(instance.getId(), name)
            .orElseGet(Variable::new);
        OffsetDateTime now = OffsetDateTime.now();
        if (var.getId() == null) {
            var.setId(UUID.randomUUID());
            var.setTenantId(instance.getTenantId());
            var.setProcessinstanceId(instance.getId());
            var.setVarName(name);
            var.setStateId(DEFAULT_STATE_ACTIVE);
            var.setCreatedAt(now);
        }
        var.setVarValue(value == null ? null : value.toString());
        var.setVarType(inferVarType(value));
        var.setUpdatedAt(now);
        varRepo.save(var);
    }

    private String inferVarType(Object value) {
        if (value == null) return "string";
        if (value instanceof Boolean) return "boolean";
        if (value instanceof Number) return "number";
        if (value instanceof Map || value instanceof List) return "json";
        return "string";
    }

    private Map<String, Object> currentVariablesAsMap(ProcessInstance instance) {
        Map<String, Object> out = new LinkedHashMap<>();
        for (Variable v : varRepo.findByProcessinstanceId(instance.getId())) {
            out.put(v.getVarName(), parseVarValue(v.getVarValue(), v.getVarType()));
        }
        return out;
    }

    private Object parseVarValue(String value, String type) {
        if (value == null) return null;
        return switch (type) {
            case "boolean" -> Boolean.parseBoolean(value);
            case "number" -> {
                try { yield Long.parseLong(value); }
                catch (NumberFormatException e) {
                    try { yield Double.parseDouble(value); }
                    catch (NumberFormatException e2) { yield value; }
                }
            }
            case "json" -> {
                try { yield jackson.readValue(value, Object.class); }
                catch (Exception e) { yield value; }
            }
            default -> value;
        };
    }

    private void audit(ProcessInstance instance, String eventType, UUID flowElementId,
                       UUID tokenId, UUID userId, Map<String, Object> data) {
        AuditLog al = new AuditLog();
        OffsetDateTime now = OffsetDateTime.now();
        al.setId(UUID.randomUUID());
        al.setTenantId(instance.getTenantId());
        al.setProcessinstanceId(instance.getId());
        al.setEventType(eventType);
        al.setFlowelementId(flowElementId);
        al.setTokenId(tokenId);
        al.setUserId(userId);
        al.setOccurredAt(now);
        al.setData(data);
        al.setStateId(DEFAULT_STATE_ACTIVE);
        al.setCreatedAt(now);
        al.setUpdatedAt(now);
        auditRepo.save(al);
    }

    private ProcessInstance newInstance(ProcessDefinition def, UUID tenantId, UUID userId) {
        ProcessInstance instance = new ProcessInstance();
        OffsetDateTime now = OffsetDateTime.now();
        instance.setId(UUID.randomUUID());
        instance.setTenantId(tenantId);
        instance.setProcessdefId(def.processdefId());
        instance.setProcessversionId(def.processVersionId());
        instance.setLifecycle("active");
        instance.setStartedAt(now);
        instance.setStartedById(userId);
        instance.setStateId(DEFAULT_STATE_ACTIVE);
        instance.setCreatedAt(now);
        instance.setUpdatedAt(now);
        instance.setCreatedById(userId);
        return instance;
    }

    private Token newToken(ProcessInstance instance, UUID elementId, UUID parentTokenId) {
        Token token = new Token();
        OffsetDateTime now = OffsetDateTime.now();
        token.setId(UUID.randomUUID());
        token.setTenantId(instance.getTenantId());
        token.setProcessinstanceId(instance.getId());
        token.setCurrentElementId(elementId);
        token.setParentTokenId(parentTokenId);
        token.setLifecycle("active");
        token.setEnteredAt(now);
        token.setStateId(DEFAULT_STATE_ACTIVE);
        token.setCreatedAt(now);
        token.setUpdatedAt(now);
        return token;
    }
}
