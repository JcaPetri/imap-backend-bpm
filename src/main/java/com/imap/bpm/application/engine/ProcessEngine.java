// ─── GOLDEN-RULES:BEGIN (auto · golden-rules.json · no editar a mano) ───
// REGLAS DE ORO IMAP — cumplir SIEMPRE (ver IMAP_GUIA_DESARROLLO.md):
//  • HTTP-only entre servicios (+ s2s auth; no SQL cross-service; futuro Kafka)
//  • Names en inglés
//  • UUIDv7 en ids
//  • i18n: idioma del string, no de la fila; datos (UUID, field, idioma)
//  • VtR: único canal con el frontend (front solo ve virtual)
//  • Hexagonal estricto (domain no depende de infra)
//  • No secrets en código (.env en C:\Applications, nunca hardcodear)
//  • Idempotencia en operaciones de negocio (idempotency key)
//  • [bpm] Orquestador central (tipo Temporal/Step Functions)
//  • [bpm] processdef→processversion: snapshot inmutable al activar
//  • [bpm] Las defs de proceso viven acá (no en system)
//  • [bpm] Camunda 8 como norte; interim form-driven en prod
// ─── GOLDEN-RULES:END ───

package com.imap.bpm.application.engine;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.imap.bpm.infrastructure.entity.*;
import com.imap.bpm.infrastructure.repository.*;
import com.imap.platform.tenant.TenantContextHolder;
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
 *   - user_task         → token.waiting + crea TaskInstanceEntity + detiene
 *   - service_task      → MVP: audita + avanza al siguiente (sin invocar service real)
 *   - exclusive_gateway → evalúa conditions JEXL, avanza el primero que matchea
 *   - parallel_gateway  → SPLIT (1-in N-out: emite N child tokens) +
 *                         JOIN  (M-in 1-out: espera todos siblings con mismo
 *                                parent_token_id antes de avanzar al outgoing)
 *   - intermediate_event (timer/message/signal): A2 — timer programa job
 *                         vía JobExecutorEntity; message/signal subscribe correlations
 *                         que reactivan tokens al recibir POST a /messages/
 *                         correlate o /signals/broadcast.
 *   - sub_process       (B1 — call activity): spawnea child ProcessInstanceEntity
 *                         con parent_instance_id + parent_token_id; al
 *                         completar el child, copia returnVariables al parent
 *                         y reactiva el token waiting en el sub_process.
 *   - boundary_event    (B2 — interrupting timer sobre user_task): adosado
 *                         a una activity vía config.boundary.attachedTo;
 *                         schedula JobExecutorEntity al crear la activity; al
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
    private final com.imap.bpm.infrastructure.sse.SseEventBus sseBus;
    private final com.imap.bpm.application.engine.servicetask.ServiceTaskRunner serviceTaskRunner;
    private final com.imap.bpm.infrastructure.repository.MessageStartSubscriptionRepository msgStartSubRepo;
    private final com.imap.bpm.application.service.TaskAssignmentService taskAssignmentService;
    private final com.imap.bpm.infrastructure.repository.CompensationRepository compensationRepo;

    /**
     * Self-injection (lazy) para invocar métodos @Transactional desde otros métodos
     * de esta MISMA clase y que el proxy de Spring aplique correctamente.
     * Spring NO intercepta self-invocations directas (this.method()) porque va
     * por el bean directo, no por el CGLIB proxy. Inyectando this como bean
     * separado, this.self.method() SÍ atraviesa el proxy y la @Transactional
     * (incl. MANDATORY propagation) funciona.
     */
    @org.springframework.context.annotation.Lazy
    @org.springframework.beans.factory.annotation.Autowired
    private ProcessEngine self;

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
                         ObjectMapper jackson,
                         com.imap.bpm.infrastructure.sse.SseEventBus sseBus,
                         com.imap.bpm.application.engine.servicetask.ServiceTaskRunner serviceTaskRunner,
                         com.imap.bpm.infrastructure.repository.MessageStartSubscriptionRepository msgStartSubRepo,
                         com.imap.bpm.application.service.TaskAssignmentService taskAssignmentService,
                         com.imap.bpm.infrastructure.repository.CompensationRepository compensationRepo) {
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
        this.sseBus = sseBus;
        this.serviceTaskRunner = serviceTaskRunner;
        this.msgStartSubRepo = msgStartSubRepo;
        this.taskAssignmentService = taskAssignmentService;
        this.compensationRepo = compensationRepo;
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
    public ProcessInstanceEntity startProcess(UUID processVersionId,
                                        Map<String, Object> payload,
                                        String bearerToken,
                                        UUID tenantId,
                                        UUID userId) {
        tenantSession.applyToCurrentTransaction();
        return startProcessInternal(processVersionId, payload, bearerToken, tenantId, userId,
            null, null);
    }

    /**
     * Fase 3 Día 4: Arranca instances por message correlation (event-based start).
     *
     * Busca todas las subscriptions activas matching (tenantId, messageCode) en
     * bpm_pro_message_start_subscription_tbl y arranca un instance por cada match.
     * Permite broadcast: N processdefs distintos suscritos al mismo messageCode
     * disparan en paralelo.
     *
     * Si no hay subscriptions → devuelve lista vacía (NO error). Esto permite que
     * el caller (típicamente un microservicio externo emitiendo events) no se
     * acople a si hay processes suscritos o no.
     *
     * @param messageCode    El código del message (ej. "inventory.purchase_order.arriving")
     * @param payload        Variables iniciales que se mergean al processinstance
     * @param bearerToken    JWT del caller para propagar a s2s (puede ser null en service calls)
     * @param tenantId       Tenant_id del caller
     * @param userId         UUID del user que dispara (puede ser null para events sin user)
     * @return Lista de ProcessInstanceEntity creadas (puede estar vacía si no hay subscriptions)
     */
    @Transactional
    public List<ProcessInstanceEntity> startProcessByMessage(String messageCode,
                                                       Map<String, Object> payload,
                                                       String bearerToken,
                                                       UUID tenantId,
                                                       UUID userId) {
        if (messageCode == null || messageCode.isBlank()) {
            log.warn("startProcessByMessage called with null/blank messageCode — skipping");
            return Collections.emptyList();
        }
        if (tenantId == null) tenantId = TenantContextHolder.get();

        // RLS: tabla bpm_pro_message_start_subscription_tbl tiene RLS habilitada
        // (Fase 4 Día 0 fix — se había deshabilitado por error en Fase 3 Día 4).
        // Sin aplicar tenantSession, el query devuelve 0 rows aunque haya subscriptions.
        tenantSession.applyToCurrentTransaction();

        List<com.imap.bpm.infrastructure.entity.MessageStartSubscriptionEntity> subs =
            msgStartSubRepo.findActiveByTenantAndMessageCode(tenantId, messageCode);

        if (subs.isEmpty()) {
            log.info("No active message-start subscriptions for tenant {} message '{}' — no instances started",
                tenantId, messageCode);
            return Collections.emptyList();
        }

        log.info("Found {} active message-start subscriptions for tenant {} message '{}'",
            subs.size(), tenantId, messageCode);

        List<ProcessInstanceEntity> instances = new ArrayList<>(subs.size());
        for (com.imap.bpm.infrastructure.entity.MessageStartSubscriptionEntity sub : subs) {
            try {
                // Each call is its own transaction (via @Transactional on startProcess).
                // self.startProcess para que el proxy de Spring aplique la @Transactional
                // (sin self, la self-invocation no atraviesa el CGLIB proxy y la tx no abre,
                // rompiendo el applyToCurrentTransaction MANDATORY de tenantSession).
                ProcessInstanceEntity inst = self.startProcess(
                    sub.getProcessversionId(), payload, bearerToken, tenantId, userId);
                instances.add(inst);
                metricInc("bpm.instance.started_by_message", sub.getMessageCode());
                log.info("Started instance {} from message '{}' subscription {} (processVersion {})",
                    inst.getId(), messageCode, sub.getId(), sub.getProcessversionId());
            } catch (Exception e) {
                log.error("Failed to start instance from message '{}' subscription {} (processVersion {}): {}",
                    messageCode, sub.getId(), sub.getProcessversionId(), e.getMessage(), e);
                // Continue with other subscriptions — best-effort broadcast
            }
        }
        return instances;
    }

    /**
     * Variante para spawn de sub_process (B1 — call activity).
     * Si parentInstanceId / parentTokenId son non-null, queda registrada como
     * child en bpm_pro_processinstance_tbl. Al completar (handleEndEvent),
     * el motor notifica al parent y avanza el token waiting.
     */
    private ProcessInstanceEntity startProcessInternal(UUID processVersionId,
                                                  Map<String, Object> payload,
                                                  String bearerToken,
                                                  UUID tenantId,
                                                  UUID userId,
                                                  UUID parentInstanceId,
                                                  UUID parentTokenId) {
        // 3c.2 — cargar el processversion bajo el TENANT OPERATIVO (no hardcodear
        // SYSTEM). La RLS de system muestra a cada tenant sus filas + las de SYSTEM
        // (platform catalog), así que esto encuentra tanto processdefs propios del
        // tenant como los seedeados bajo SYSTEM. Antes, hardcodear SYSTEM rompía el
        // arranque de processdefs creados bajo un tenant operativo ("not found").
        ProcessDefinition def = loader.load(processVersionId, bearerToken, tenantId);
        log.info("Starting process {} (v{}) for user {} parent={}",
            def.processdefCode(), def.version(), userId, parentInstanceId);

        // 1. Find start_event
        ProcessDefinition.FlowElement start = def.flowElements().stream()
            .filter(fe -> "start_event".equals(fe.type()))
            .findFirst()
            .orElseThrow(() -> new IllegalStateException(
                "No start_event in processVersion " + processVersionId));

        // 2. Crear ProcessInstanceEntity (con parent refs si es sub_process)
        ProcessInstanceEntity instance = newInstance(def, tenantId, userId);
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

        // 4. TokenEntity inicial en start_event
        TokenEntity token = newToken(instance, start.id(), null);
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
     * Completa una TaskInstanceEntity: merge outputData en variables + avanza el token.
     */
    @Transactional
    public TaskInstanceEntity completeTask(UUID taskInstanceId,
                                     Map<String, Object> outputData,
                                     String bearerToken,
                                     UUID userId) {
        tenantSession.applyToCurrentTransaction();
        TaskInstanceEntity task = taskRepo.findById(taskInstanceId)
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

        ProcessInstanceEntity instance = instanceRepo.findById(task.getProcessinstanceId())
            .orElseThrow(() -> new IllegalStateException(
                "ProcessInstanceEntity not found for task: " + taskInstanceId));

        // ¿Es una task de una activity multi-instanciada? Su token es hijo de un
        // token ancla (parentTokenId) con mi_cardinality != null, ambos parados
        // en el mismo element. En ese caso NO mergeamos el output a las vars
        // globales (cada item es independiente) y desviamos al join por cardinalidad.
        TokenEntity token = task.getTokenId() != null
            ? tokenRepo.findById(task.getTokenId()).orElse(null) : null;
        TokenEntity miAnchor = miAnchorOf(token);

        // Merge outputData en variables del processinstance (solo path normal)
        if (miAnchor == null && outputData != null) {
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

        // Multi-instance: consumir el token hijo + join por cardinalidad.
        if (miAnchor != null) {
            completeMultiInstanceChild(instance, token, miAnchor, outputData, bearerToken, userId);
            return task;
        }

        // Avanza el token: el current element es el user_task que se acaba de
        // completar. Reactivamos el token (estaba 'waiting') y CONSUMIMOS el
        // current element pasando al siguiente — sino advanceToken vería el
        // user_task otra vez y crearía OTRA TaskInstanceEntity (bug catched 2026-05-17).
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
        return task;
    }

    // ════════════════════════════════════════════════════════════════════════
    // State machine — advanceToken (recursivo hasta wait state)
    // ════════════════════════════════════════════════════════════════════════

    private void advanceToken(ProcessInstanceEntity instance, TokenEntity token,
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
                if (current.hasMultiInstance()) {
                    handleMultiInstanceSplit(instance, token, current, def, userId);
                } else {
                    handleUserTask(instance, token, current, def, userId);
                }
                break;

            case "service_task":
                handleServiceTask(instance, token, current, def, userId);
                break;

            case "exclusive_gateway":
                handleExclusiveGateway(instance, token, current, def, userId);
                break;

            case "parallel_gateway":
                handleParallelGateway(instance, token, current, def, userId);
                break;

            case "event_based_gateway":
                handleEventBasedGateway(instance, token, current, def, userId);
                break;

            case "inclusive_gateway":
                handleInclusiveGateway(instance, token, current, def, userId);
                break;

            case "intermediate_event":
                handleIntermediateEvent(instance, token, current, def, userId);
                break;

            case "sub_process":
                if (current.hasMultiInstance()) {
                    handleMultiInstanceSplit(instance, token, current, def, userId);
                } else {
                    handleSubProcess(instance, token, current, def, userId);
                }
                break;

            case "business_rule_task":
                handleBusinessRuleTask(instance, token, current, def, userId);
                break;

            case "boundary_event":
                // B2 — un boundary_event NO recibe tokens entrantes normales
                // (se dispara por interrupción de su activity adjunta). Si
                // un token llega acá, lo tratamos como passthrough con warn.
                log.warn("TokenEntity entered boundary_event '{}' directly — unusual, treating as passthrough",
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

    // ════════════════════════════════════════════════════════════════════════
    // skipForMigration — Hito 3 'skip' action support
    // ════════════════════════════════════════════════════════════════════════

    /**
     * Skip = "saltar este flowElement y avanzar al siguiente" durante una
     * migration plan apply. Cancela tasks/boundary jobs asociados al token,
     * audita el evento, y dispara `consumeAndMoveToNext` para que el motor
     * avance al outgoing del `skipElement` en `targetDef` (v2).
     *
     * Llamado por MigrationApplyService cuando la regla tiene action='skip'.
     * El skipElement debe ser un flowElement de v2 (targetCode); el token ya
     * tiene currentElementId=skipElement.id seteado antes de llamar.
     *
     * Semántica: como si el user hubiera completado la activity sin output.
     */
    public void skipForMigration(ProcessInstanceEntity instance, TokenEntity token,
                                  ProcessDefinition.FlowElement skipElement,
                                  ProcessDefinition targetDef, UUID userId) {
        OffsetDateTime now = OffsetDateTime.now();

        // 1. Cancelar tasks vivas del token (asumimos que el skip implica
        //    cerrar la activity en curso). Si no hay tasks, no-op.
        List<TaskInstanceEntity> activeTasks = taskRepo.findByTokenIdAndLifecycleIn(
            token.getId(), List.of("created", "reserved", "assigned", "in_progress"));
        for (TaskInstanceEntity t : activeTasks) {
            t.setLifecycle("cancelled");
            t.setCompletedAt(now);
            t.setUpdatedAt(now);
            taskRepo.save(t);
        }

        // 2. Cancelar boundary jobs del token (B2) — evita que disparen
        //    timers después del skip.
        int cancelledJobs = cancelBoundaryJobsForToken(token.getId());

        // 3. Audit
        audit(instance, "token.skipped_for_migration", skipElement.id(), token.getId(), userId,
            Map.of("elementCode", skipElement.code(),
                   "tasksCancelled", activeTasks.size(),
                   "boundaryJobsCancelled", cancelledJobs));

        // 4. Avanzar al outgoing del skipElement en targetDef.
        //    Esto puede llegar a wait state inmediato (user_task → crear TaskInstanceEntity)
        //    o cascadear hasta end_event (todo service_task/gateway).
        consumeAndMoveToNext(instance, token, skipElement, targetDef, userId);
    }

    // ─── consume + move ─────────────────────────────────────────────────────

    private void consumeAndMoveToNext(ProcessInstanceEntity instance, TokenEntity token,
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

    private void moveTokenToElement(ProcessInstanceEntity instance, TokenEntity token,
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

    // ─── service_task (Fase 0.B.1 — ServiceTaskRegistry) ───────────────────

    /**
     * Dispatcha el service_task al handler correspondiente vía {@link com.imap.bpm.application.engine.servicetask.ServiceTaskRunner}.
     *
     * Lookup chain en el Registry:
     *   1. Local handler (@ServiceTask annotation en el classpath del BPM)
     *   2. Remote handler (POST a baseUrl según prefix → bpm.service-tasks.remotes.<prefix>)
     *   3. Fallback: log warning + advance (compat con V1 MVP donde todos eran log+advance)
     *
     * Resultado:
     *   - SUCCESS → mergea resultVariables al instance + advance al outgoing
     *   - FAILURE con boundaryErrorCode → dispara el boundary error event correspondiente
     *   - FAILURE sin boundaryErrorCode (tras retries) → token a lifecycle='failed' + audit
     *   - PENDING (V2) → no implementado, equivale a SUCCESS por ahora
     */
    private void handleServiceTask(ProcessInstanceEntity instance, TokenEntity token,
                                    ProcessDefinition.FlowElement current,
                                    ProcessDefinition def, UUID userId) {
        String serviceCode = current.config() == null ? null : (String) current.config().get("serviceCode");
        Map<String, Object> vars = currentVariablesAsMap(instance);

        // Bearer token para que remote handlers puedan hacer s2s en cascade
        String bearerToken = com.imap.platform.security.BearerTokenHolder.get();

        com.imap.bpm.application.engine.servicetask.ServiceTaskContext ctx =
            new com.imap.bpm.application.engine.servicetask.ServiceTaskContext(
                serviceCode, current, instance, token, userId, bearerToken, vars);

        audit(instance, "service_task.invoked", current.id(), token.getId(), userId,
            Map.of("elementCode", current.code(), "serviceCode", serviceCode == null ? "(none)" : serviceCode));

        com.imap.bpm.application.engine.servicetask.ServiceTaskResult result;
        try {
            result = serviceTaskRunner.runWithRetry(ctx);
        } catch (Exception e) {
            log.error("ServiceTaskRunner threw unexpected exception for serviceCode='{}'", serviceCode, e);
            result = com.imap.bpm.application.engine.servicetask.ServiceTaskResult.fail(
                "RUNNER_EXCEPTION", e.getClass().getSimpleName() + ": " + e.getMessage());
        }

        // ── SUCCESS o PENDING (PENDING en MVP se trata como SUCCESS) ──────────
        if (result.isSuccess() || result.isPending()) {
            // Mergear resultVariables al processinstance.variables
            if (result.resultVariables() != null && !result.resultVariables().isEmpty()) {
                for (Map.Entry<String, Object> e : result.resultVariables().entrySet()) {
                    setVariable(instance, e.getKey(), e.getValue());
                }
            }
            audit(instance, "service_task.completed", current.id(), token.getId(), userId,
                Map.of("elementCode", current.code(),
                       "serviceCode", serviceCode == null ? "(none)" : serviceCode,
                       "resultVarsCount", result.resultVariables() == null ? 0 : result.resultVariables().size()));
            // Saga: si este activity es compensable (tiene handler compensationFor),
            // registrar la compensacion con snapshot de vars (LIFO en el trigger).
            registerCompensationIfAny(instance, current, def, userId);
            consumeAndMoveToNext(instance, token, current, def, userId);
            return;
        }

        // ── FAILURE → intentar boundary error auto antes de marcar 'failed' ───
        // errorCode efectivo: el boundaryErrorCode explícito del handler tiene
        // prioridad; si no, el errorCode del resultado (timeout / 5xx / excepción).
        // Ambos matchean contra findErrorBoundariesFor (exact O catch-all `*`/null),
        // así que un boundary catch-all sobre el service_task captura CUALQUIER falla.
        String effectiveErrorCode = result.boundaryErrorCode() != null
            ? result.boundaryErrorCode() : result.errorCode();

        audit(instance, "service_task.error", current.id(), token.getId(), userId,
            Map.of("elementCode", current.code(),
                   "serviceCode", serviceCode == null ? "(none)" : serviceCode,
                   "errorCode", effectiveErrorCode == null ? "UNKNOWN" : effectiveErrorCode,
                   "errorMessage", result.errorMessage() == null ? "" : result.errorMessage(),
                   "explicitBoundaryCode", result.boundaryErrorCode() != null));

        boolean fired = tryFireServiceTaskBoundaryError(instance, token, current, def,
            effectiveErrorCode, userId);
        if (fired) return;

        // ── FAILURE definitivo (sin boundary matching) ───────────────────────
        token.setLifecycle("failed");
        token.setUpdatedAt(OffsetDateTime.now());
        tokenRepo.save(token);
        audit(instance, "service_task.failed", current.id(), token.getId(), userId,
            Map.of("elementCode", current.code(),
                   "serviceCode", serviceCode == null ? "(none)" : serviceCode,
                   "errorCode", result.errorCode() == null ? "UNKNOWN" : result.errorCode(),
                   "errorMessage", result.errorMessage() == null ? "" : result.errorMessage()));
        log.error("service_task '{}' (serviceCode={}) failed after retries — token {} marked as failed",
            current.code(), serviceCode, token.getId());
    }

    /**
     * Boundary error auto sobre service_task: al fallar (tras agotar retries),
     * busca boundary_event de error/escalation adjuntos al service_task que
     * matcheen el errorCode (exact, o catch-all `*`/null) y los dispara. Mismo
     * mecanismo que raiseTaskError (endpoint manual sobre user_task) pero SIN
     * task humana — el activityToken es el propio token del service_task.
     *
     * Habilita resiliencia de orquestacion: modelar un boundary error catch-all
     * sobre un service_task enruta CUALQUIER falla del handler HTTP a una rama
     * de excepcion (retry alternativo / notificacion / escalacion) en vez de
     * dejar el token 'failed' sin recuperacion.
     *
     * @return true si se disparo al menos un boundary (caller retorna sin marcar failed).
     */
    private boolean tryFireServiceTaskBoundaryError(ProcessInstanceEntity instance, TokenEntity token,
                                                     ProcessDefinition.FlowElement serviceTask,
                                                     ProcessDefinition def,
                                                     String errorCode, UUID userId) {
        List<ProcessDefinition.FlowElement> matches = def.findErrorBoundariesFor(serviceTask.code(), errorCode);
        if (matches.isEmpty()) {
            log.info("service_task '{}' error '{}' — sin boundary error matching, fall through a FAILURE",
                serviceTask.code(), errorCode);
            return false;
        }
        int fired = 0;
        for (ProcessDefinition.FlowElement boundary : matches) {
            @SuppressWarnings("unchecked")
            Map<String, Object> boundaryCfg = boundary.config() == null ? Map.of()
                : (Map<String, Object>) boundary.config().getOrDefault("boundary", Map.of());
            boolean interrupting = !(Boolean.FALSE.equals(boundaryCfg.get("interrupting")));

            Map<String, Object> extra = new LinkedHashMap<>();
            extra.put("trigger", "error");
            extra.put("errorCode", errorCode == null ? "(none)" : errorCode);
            extra.put("source", "service_task");

            fireBoundaryHandler(instance, token,
                boundary.id(), boundary.code(),
                serviceTask.code(), null /* service_task no tiene task humana */,
                interrupting, null,
                "boundary.error.fired", "service_task_error:" + errorCode,
                extra);
            fired++;
        }
        metricInc("bpm.service_task.boundary_fired", def);
        log.info("service_task '{}' error '{}' → disparo {} boundary(ies)",
            serviceTask.code(), errorCode, fired);
        return fired > 0;
    }

    // ════════════════════════════════════════════════════════════════════════
    // Compensation / Saga — rollback de negocio en orquestacion distribuida
    //
    // Un service_task compensable declara su undo con un handler OFF-PATH:
    // otro service_task con config.compensationFor = <codeDelCompensable>. Al
    // completar OK el compensable, se registra una fila en bpm_pro_compensation_tbl
    // con snapshot de variables. Un end_event con config.compensate=true dispara
    // la compensacion: corre los handlers en LIFO (inverso a la completacion) via
    // ServiceTaskRunner (mismo dispatch HTTP S2S que un service_task normal).
    //
    // Compone con el feature ② (boundary error): un service_task falla → boundary
    // error → rama → end_event compensate=true → deshace lo previo. Es el patron
    // Saga sin 2PC (que la Regla de Oro HTTP-only prohibe de facto).
    // ════════════════════════════════════════════════════════════════════════

    /** Registra la compensacion de un activity compensable recien completado (si tiene handler). */
    private void registerCompensationIfAny(ProcessInstanceEntity instance,
                                           ProcessDefinition.FlowElement activity,
                                           ProcessDefinition def, UUID userId) {
        ProcessDefinition.FlowElement handler = def.findCompensationHandlerFor(activity.code());
        if (handler == null) return;

        OffsetDateTime now = OffsetDateTime.now();
        CompensationEntity c = new CompensationEntity();
        c.setId(UUID.randomUUID());
        c.setTenantId(instance.getTenantId());
        c.setProcessinstanceId(instance.getId());
        c.setCompletedElementId(activity.id());
        c.setCompensationElementId(handler.id());
        c.setCompletionOrder(compensationRepo.countByProcessinstanceId(instance.getId()));
        c.setCompletionData(currentVariablesAsMap(instance));
        c.setLifecycle("registered");
        c.setStateId(DEFAULT_STATE_ACTIVE);
        c.setCreatedAt(now);
        c.setUpdatedAt(now);
        compensationRepo.save(c);

        audit(instance, "compensation.registered", activity.id(), null, userId, Map.of(
            "compensableCode", activity.code(),
            "handlerCode", handler.code(),
            "completionOrder", c.getCompletionOrder()
        ));
        metricInc("bpm.compensation.registered", def);
    }

    /**
     * Dispara la compensacion LIFO de la instance: ejecuta los handlers
     * registrados en orden inverso a la completacion (completion_order DESC).
     * Cada handler corre via ServiceTaskRunner con el snapshot de vars de cuando
     * el activity original se completo. Los fallos de un handler NO abortan el
     * resto (best-effort): se marca 'failed' y se sigue (rollback parcial > nada).
     */
    private void compensateInstance(ProcessInstanceEntity instance, UUID userId) {
        List<CompensationEntity> regs = compensationRepo
            .findByProcessinstanceIdAndLifecycleOrderByCompletionOrderDesc(instance.getId(), "registered");
        if (regs.isEmpty()) {
            audit(instance, "compensation.none", null, null, userId, Map.of());
            return;
        }

        ProcessDefinition def = loader.load(instance.getProcessversionId(), null, instance.getTenantId());
        String bearer = com.imap.platform.security.BearerTokenHolder.get();
        audit(instance, "compensation.triggered", null, null, userId, Map.of("count", regs.size()));
        metricInc("bpm.compensation.triggered", def);

        int done = 0, failed = 0;
        for (CompensationEntity c : regs) {
            OffsetDateTime now = OffsetDateTime.now();
            ProcessDefinition.FlowElement handler = def.findElementById(c.getCompensationElementId());
            String serviceCode = (handler == null || handler.config() == null)
                ? null : (String) handler.config().get("serviceCode");

            c.setLifecycle("compensating");
            c.setUpdatedAt(now);
            compensationRepo.save(c);

            com.imap.bpm.application.engine.servicetask.ServiceTaskResult r;
            if (handler == null || serviceCode == null) {
                r = com.imap.bpm.application.engine.servicetask.ServiceTaskResult.fail(
                    "COMP_NO_HANDLER", "compensation handler o serviceCode ausente");
            } else {
                com.imap.bpm.application.engine.servicetask.ServiceTaskContext ctx =
                    new com.imap.bpm.application.engine.servicetask.ServiceTaskContext(
                        serviceCode, handler, instance, null /* sin token vivo */, userId, bearer,
                        c.getCompletionData() == null ? Map.of() : c.getCompletionData());
                try {
                    r = serviceTaskRunner.runWithRetry(ctx);
                } catch (Exception e) {
                    r = com.imap.bpm.application.engine.servicetask.ServiceTaskResult.fail(
                        "COMP_EXCEPTION", e.getClass().getSimpleName() + ": " + e.getMessage());
                }
            }

            OffsetDateTime end = OffsetDateTime.now();
            if (r.isSuccess() || r.isPending()) {
                c.setLifecycle("compensated");
                c.setCompensatedAt(end);
                c.setUpdatedAt(end);
                compensationRepo.save(c);
                audit(instance, "compensation.executed", c.getCompletedElementId(), null, userId, Map.of(
                    "handlerCode", handler != null ? handler.code() : "?",
                    "serviceCode", serviceCode == null ? "(none)" : serviceCode,
                    "completionOrder", c.getCompletionOrder()
                ));
                done++;
            } else {
                c.setLifecycle("failed");
                c.setUpdatedAt(end);
                compensationRepo.save(c);
                audit(instance, "compensation.failed", c.getCompletedElementId(), null, userId, Map.of(
                    "handlerCode", handler != null ? handler.code() : "?",
                    "serviceCode", serviceCode == null ? "(none)" : serviceCode,
                    "errorCode", r.errorCode() == null ? "UNKNOWN" : r.errorCode(),
                    "errorMessage", r.errorMessage() == null ? "" : r.errorMessage()
                ));
                failed++;
            }
        }
        audit(instance, "compensation.completed", null, null, userId, Map.of(
            "compensated", done, "failed", failed, "total", regs.size()
        ));
        log.info("compensateInstance {} → {} compensated, {} failed (LIFO)",
            instance.getId(), done, failed);
    }

    // ─── end_event ──────────────────────────────────────────────────────────

    private void handleEndEvent(ProcessInstanceEntity instance, TokenEntity token,
                                 ProcessDefinition.FlowElement endEvent, UUID userId) {
        // Saga trigger: un end_event con config.compensate=true corre la
        // compensacion LIFO de lo ya completado ANTES de terminar la instance.
        if (endEvent.config() != null && Boolean.TRUE.equals(endEvent.config().get("compensate"))) {
            compensateInstance(instance, userId);
        }

        // Terminate end event: config.terminate=true mata TODOS los tokens vivos
        // de la instance (aborta ramas paralelas en curso) y la completa ya. Se
        // resuelve aca y se retorna (no sigue el flujo normal de fin).
        if (endEvent.config() != null && Boolean.TRUE.equals(endEvent.config().get("terminate"))) {
            terminateInstance(instance, endEvent, token, userId);
            return;
        }

        token.setLifecycle("consumed");
        token.setUpdatedAt(OffsetDateTime.now());
        tokenRepo.save(token);
        audit(instance, "token.consumed", endEvent.id(), token.getId(), userId,
            Map.of("elementCode", endEvent.code()));

        // Si NO hay más tokens activos en la instance → mark completed
        List<TokenEntity> stillActive = tokenRepo.findByProcessinstanceIdAndLifecycleIn(
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
            log.info("ProcessInstanceEntity {} completed via {}", instance.getId(), endEvent.code());
            metricInc("bpm.instance.ended", instance.getProcessdefId().toString());

            // B1 — si es child de un sub_process, notificar al parent.
            // Si fire-and-forget (parentTokenId=null) solo auditamos en el
            // parent sin avanzar nada — el parent ya pasó al siguiente flow_element.
            if (instance.getParentInstanceId() != null) {
                if (instance.getParentTokenId() != null) {
                    notifyParentOfSubprocessCompletion(instance, userId);
                } else {
                    auditFireAndForgetChildCompletion(instance, userId);
                }
            }
        }
    }

    /**
     * Terminate end event: aborta la instance. Consume TODOS los tokens vivos
     * (active + waiting), des-arma sus jobs scheduled y correlations waiting,
     * cancela las tasks vivas, y marca la instance completed inmediatamente.
     * Uso: un rechazo fatal en una rama mata las ramas paralelas en curso.
     */
    private void terminateInstance(ProcessInstanceEntity instance, ProcessDefinition.FlowElement endEvent,
                                   TokenEntity endToken, UUID userId) {
        OffsetDateTime now = OffsetDateTime.now();
        int killed = 0;
        for (TokenEntity t : tokenRepo.findByProcessinstanceIdAndLifecycleIn(
                instance.getId(), List.of("active", "waiting"))) {
            for (JobExecutorEntity job : jobRepo.findByTokenIdAndLifecycle(t.getId(), "scheduled")) {
                job.setLifecycle("cancelled");
                job.setUpdatedAt(now);
                jobRepo.save(job);
            }
            for (MessageCorrelationEntity mc : msgCorrRepo.findByTokenIdAndLifecycle(t.getId(), "waiting")) {
                mc.setLifecycle("cancelled");
                mc.setUpdatedAt(now);
                msgCorrRepo.save(mc);
            }
            for (TaskInstanceEntity task : taskRepo.findByTokenIdAndLifecycleIn(
                    t.getId(), List.of("created", "assigned", "in_progress"))) {
                task.setLifecycle("cancelled");
                task.setUpdatedAt(now);
                taskRepo.save(task);
            }
            t.setLifecycle("consumed");
            t.setUpdatedAt(now);
            tokenRepo.save(t);
            killed++;
        }

        instance.setLifecycle("completed");
        instance.setEndedAt(now);
        instance.setUpdatedAt(now);
        instanceRepo.save(instance);
        audit(instance, "instance.terminated", endEvent.id(), endToken.getId(), userId, Map.of(
            "endEventCode", endEvent.code(),
            "killedTokens", killed
        ));
        metricInc("bpm.instance.terminated", instance.getProcessdefId().toString());
        log.info("ProcessInstanceEntity {} TERMINATED via {} (killed {} tokens)",
            instance.getId(), endEvent.code(), killed);

        // Si es child de un sub_process, notificar/auditar al parent igual que un fin normal.
        if (instance.getParentInstanceId() != null) {
            if (instance.getParentTokenId() != null) {
                notifyParentOfSubprocessCompletion(instance, userId);
            } else {
                auditFireAndForgetChildCompletion(instance, userId);
            }
        }
    }

    // ─── user_task ──────────────────────────────────────────────────────────

    private void handleUserTask(ProcessInstanceEntity instance, TokenEntity token,
                                 ProcessDefinition.FlowElement userTask,
                                 ProcessDefinition def, UUID userId) {
        // TokenEntity queda waiting hasta que se complete la task
        token.setLifecycle("waiting");
        token.setUpdatedAt(OffsetDateTime.now());
        tokenRepo.save(token);

        // Snapshot de variables current para el input_data de la task
        Map<String, Object> inputData = currentVariablesAsMap(instance);

        TaskInstanceEntity task = new TaskInstanceEntity();
        OffsetDateTime now = OffsetDateTime.now();
        task.setId(UUID.randomUUID());
        task.setTenantId(instance.getTenantId());
        task.setProcessinstanceId(instance.getId());
        task.setFlowelementId(userTask.id());
        task.setTokenId(token.getId());
        task.setLifecycle("created");
        task.setPriority(50);
        task.setInputData(inputData);
        // A3 — AssignmentRule via TaskAssignmentService (tech-debt sprint D2 P1):
        // V1 hardcoded — si starter es service account → fallback admin.
        // Cuando llegue Iter 5 AssignmentRule real, acá se consulta
        // bpm_hum_assignmentrule del flowelement (user / role / group / expr).
        // WorkHub 3b — routing del candidate group (modelo A + DMN, §6.2):
        //   3b.2 DMN: config.candidateGroupDecision → tabla DMN multi-condición.
        //   3b.1 estático/expr: config.candidateGroup ("deposito_ba" o "${var}").
        // Si resuelve → tarea DE COLA (assignee null, claimable por quien tenga el
        // permiso bpm.queue.<x>). Si no → asignación directa (actual).
        String candidateGroup = taskAssignmentService.resolveCandidateGroup(
            userTask.config(), inputData, instance.getTenantId());
        if (candidateGroup != null) {
            task.setAssignedRole(candidateGroup);   // assignee + assigned_at quedan null hasta el claim
        } else {
            UUID assignee = taskAssignmentService.resolveAssignee(
                def.processdefCode(), userTask.code(),
                instance.getStartedById(), instance.getTenantId());
            task.setAssignedUserId(assignee);
            task.setAssignedAt(now);
        }
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
            "formEntityDefCode", (form != null && form.entityDefCode() != null) ? form.entityDefCode() : "(none)",
            "boundariesScheduled", boundariesScheduled
        ));
        log.info("UserTask '{}' created → task {} waiting (form={}, assignedTo={}, boundaries={})",
            userTask.code(), task.getId(),
            form != null ? form.entityDefCode() : "none",
            task.getAssignedUserId(),
            boundariesScheduled);
        metricInc("bpm.task.created", def);
    }

    // ─── boundary_event (B2 — interrupting timer sobre user_task) ───────────

    /**
     * Para cada boundary_event con type=timer adjunto al activity, schedula
     * un JobExecutorEntity con fire_at = now + delaySeconds. El job lleva en su
     * config_jsonb el marker `boundary: true` + `boundaryElementId` +
     * `attachedTaskInstanceId` (si user_task) para que fireTimerJob pueda
     * discriminar boundary vs intermediate timer.
     *
     * Si no hay boundaries para esta activity → devuelve 0 (no-op).
     *
     * NOTA: scope MVP B2 → solo timer; error/escalation pendiente.
     */
    @SuppressWarnings("unchecked")
    private int scheduleBoundaryTimers(ProcessInstanceEntity instance, TokenEntity activityToken,
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

            JobExecutorEntity job = new JobExecutorEntity();
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
            // Timer ciclico (solo non-interrupting): repeatEverySeconds + maxRepeats.
            // Al disparar, reprograma el proximo hasta agotar maxRepeats (o hasta que
            // la activity se complete → cancelBoundaryJobsForToken cancela el pendiente).
            if (!interrupting && timerCfg.get("repeatEverySeconds") != null) {
                jobCfg.put("repeatEverySeconds", ((Number) timerCfg.get("repeatEverySeconds")).longValue());
                Object maxR = timerCfg.get("maxRepeats");
                jobCfg.put("maxRepeats", maxR instanceof Number mn ? mn.intValue() : 10);
                jobCfg.put("repeatCount", 1);
            }
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
        List<JobExecutorEntity> jobs = jobRepo.findByTokenIdAndLifecycle(tokenId, "scheduled");
        if (jobs.isEmpty()) return 0;
        OffsetDateTime now = OffsetDateTime.now();
        int cancelled = 0;
        for (JobExecutorEntity j : jobs) {
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

    private void handleExclusiveGateway(ProcessInstanceEntity instance, TokenEntity token,
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
        metricInc("bpm.gateway.exclusive", def);
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
        ProcessInstanceEntity instance = instanceRepo.findById(instanceId)
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
        for (TokenEntity t : tokenRepo.findByProcessinstanceIdAndLifecycleIn(
                instance.getId(), List.of("active", "waiting"))) {
            t.setLifecycle("consumed");
            t.setUpdatedAt(now);
            tokenRepo.save(t);
            tokensCancelled++;
        }

        // Tasks vivas → cancelled
        for (TaskInstanceEntity task : taskRepo.findByProcessinstanceId(instance.getId())) {
            if (List.of("created", "assigned", "in_progress").contains(task.getLifecycle())) {
                task.setLifecycle("cancelled");
                task.setUpdatedAt(now);
                taskRepo.save(task);
                tasksCancelled++;
            }
        }

        // Jobs scheduled → cancelled (incluye boundary + intermediate timers)
        for (JobExecutorEntity job : jobRepo.findByProcessinstanceIdAndLifecycle(instance.getId(), "scheduled")) {
            job.setLifecycle("cancelled");
            job.setUpdatedAt(now);
            jobRepo.save(job);
            jobsCancelled++;
        }

        // Correlations waiting → cancelled
        for (MessageCorrelationEntity mc : msgCorrRepo.findByProcessinstanceIdAndLifecycle(instance.getId(), "waiting")) {
            mc.setLifecycle("cancelled");
            mc.setUpdatedAt(now);
            msgCorrRepo.save(mc);
            corrsCancelled++;
        }

        instance.setLifecycle("cancelled");
        instance.setEndedAt(now);
        instance.setUpdatedAt(now);
        instanceRepo.save(instance);

        // Cancel-cascade: si la instance tiene children sub_process activos,
        // cancelarlos recursivamente. La recursión es seguro porque
        // findByParentInstanceId solo devuelve directos; los nietos se cancelan
        // en la siguiente iteración recursiva al cancelar el child.
        int childrenCascaded = 0;
        for (ProcessInstanceEntity child : instanceRepo.findByParentInstanceId(instance.getId())) {
            if ("active".equals(child.getLifecycle())) {
                try {
                    cancelInstance(child.getId(),
                        "parent_cancelled:" + (reason == null ? "-" : reason),
                        userId);
                    childrenCascaded++;
                } catch (Exception ex) {
                    log.warn("cancel-cascade: child {} failed: {}", child.getId(), ex.getMessage());
                }
            }
        }

        Map<String, Object> auditData = new LinkedHashMap<>();
        auditData.put("reason", reason == null ? "(no reason given)" : reason);
        auditData.put("tokensCancelled", tokensCancelled);
        auditData.put("tasksCancelled", tasksCancelled);
        auditData.put("jobsCancelled", jobsCancelled);
        auditData.put("correlationsCancelled", corrsCancelled);
        auditData.put("childrenCascaded", childrenCascaded);
        audit(instance, "instance.cancelled", null, null, userId, auditData);
        log.info("Instance {} cancelled (reason={}) — tokens={}, tasks={}, jobs={}, corrs={}, childrenCascaded={}",
            instanceId, reason, tokensCancelled, tasksCancelled, jobsCancelled, corrsCancelled, childrenCascaded);
        metricInc("bpm.instance.cancelled", instance.getProcessdefId().toString());

        return auditData;
    }

    // ─── raise error sobre activity (boundary error/escalation, advanced) ───

    /**
     * Dispara un error sobre una TaskInstanceEntity activa, buscando boundary_events
     * de tipo error/escalation adjuntos al activity que matcheen el errorCode.
     *
     * Si hay match → invoca el handler unificado de boundary (interrupting
     * o non-interrupting según config). Si NO hay match → lanza
     * IllegalStateException (el caller decide qué hacer: marcar task failed
     * manualmente, retry, etc).
     *
     * Para MVP solo se llama externamente vía POST /v1/bpm/task/{id}/raise-error.
     * Cuando service_task implemente invocación real, el motor también puede
     * invocar este path al capturar excepción.
     */
    @Transactional
    public Map<String, Object> raiseTaskError(UUID taskInstanceId, String errorCode,
                                              Map<String, Object> payload, UUID userId) {
        tenantSession.applyToCurrentTransaction();
        TaskInstanceEntity task = taskRepo.findById(taskInstanceId)
            .orElseThrow(() -> new IllegalArgumentException("Task not found: " + taskInstanceId));
        if (!List.of("created", "assigned", "in_progress").contains(task.getLifecycle())) {
            throw new IllegalStateException("Task " + taskInstanceId + " is not active (lifecycle="
                + task.getLifecycle() + ")");
        }
        ProcessInstanceEntity instance = instanceRepo.findById(task.getProcessinstanceId())
            .orElseThrow(() -> new IllegalStateException("Instance not found"));
        TokenEntity activityToken = task.getTokenId() == null ? null
            : tokenRepo.findById(task.getTokenId()).orElse(null);
        if (activityToken == null) {
            throw new IllegalStateException("Activity token not found for task " + taskInstanceId);
        }
        ProcessDefinition def = loader.load(instance.getProcessversionId(), null, instance.getTenantId());
        ProcessDefinition.FlowElement activity = def.findElementById(task.getFlowelementId());
        if (activity == null) throw new IllegalStateException("Activity element not found");

        List<ProcessDefinition.FlowElement> matches = def.findErrorBoundariesFor(activity.code(), errorCode);
        if (matches.isEmpty()) {
            throw new IllegalStateException("No error/escalation boundary on '" + activity.code()
                + "' matches errorCode '" + errorCode + "'");
        }

        // Disparamos TODOS los boundaries que matchearon (regla BPMN: multiple
        // catchers válido). Si alguno es interrupting, las siblings posteriores
        // pueden tener activity ya consumed — el handler igual emite token
        // en su outgoing (parallel branch).
        int firedCount = 0;
        for (ProcessDefinition.FlowElement boundary : matches) {
            @SuppressWarnings("unchecked")
            Map<String, Object> boundaryCfg = boundary.config() == null ? Map.of()
                : (Map<String, Object>) boundary.config().getOrDefault("boundary", Map.of());
            boolean interrupting = !(Boolean.FALSE.equals(boundaryCfg.get("interrupting")));

            Map<String, Object> extra = new LinkedHashMap<>();
            extra.put("trigger", "error");
            extra.put("errorCode", errorCode);
            if (payload != null && !payload.isEmpty()) extra.put("payload", payload);

            fireBoundaryHandler(instance, activityToken,
                boundary.id(), boundary.code(),
                activity.code(), task.getId(),
                interrupting, null,
                "boundary.error.fired", "boundary_error_raised:" + errorCode,
                extra);
            firedCount++;
        }
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("taskId", taskInstanceId.toString());
        out.put("activityCode", activity.code());
        out.put("errorCode", errorCode);
        out.put("boundariesFired", firedCount);
        return out;
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
     *   M in / N out  → no soportado: el modelado lo desaconseja. TokenEntity waiting
     *                   + warning en log.
     *
     * Garantía: en SPLIT, todos los siblings comparten parentTokenId == el id
     * del token que se splitió. En JOIN, esa relación se usa para identificar
     * "mi grupo" y no confundirme con tokens de otros splits concurrentes.
     */
    private void handleParallelGateway(ProcessInstanceEntity instance, TokenEntity token,
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
            metricInc("bpm.gateway.split", def);
            for (ProcessDefinition.SequenceFlow sf : outgoing) {
                TokenEntity child = newToken(instance, sf.targetId(), token.getId());
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
            List<TokenEntity> arrived = tokenRepo.findByProcessinstanceIdAndCurrentElementIdAndLifecycle(
                instance.getId(), gateway.id(), "waiting");
            List<TokenEntity> siblings = arrived.stream()
                .filter(t -> Objects.equals(t.getParentTokenId(), myParent))
                .toList();

            if (siblings.size() < incoming.size()) {
                log.debug("parallel_gateway JOIN {} — waiting siblings: {}/{}",
                    gateway.code(), siblings.size(), incoming.size());
                return; // esperar más
            }

            // Todos llegaron — consume siblings + emit nuevo token en outgoing
            for (TokenEntity sib : siblings) {
                sib.setLifecycle("consumed");
                sib.setUpdatedAt(now);
                tokenRepo.save(sib);
            }

            // grandparent restaura el nivel de paralelismo anterior al SPLIT
            UUID grandparent = myParent == null ? null
                : tokenRepo.findById(myParent).map(TokenEntity::getParentTokenId).orElse(null);

            audit(instance, "gateway.join.completed", gateway.id(), null, userId, Map.of(
                "gatewayCode", gateway.code(),
                "branchesJoined", siblings.size()
            ));
            metricInc("bpm.gateway.join", def);

            ProcessDefinition.SequenceFlow out = outgoing.get(0);
            TokenEntity outToken = newToken(instance, out.targetId(), grandparent);
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

        // ── M-in / N-out: JOIN-then-SPLIT combinado ──────────────────────────
        // Espera a los M hermanos (como el JOIN) y luego emite N tokens (como el
        // SPLIT). Sincroniza los flujos entrantes y re-abre el paralelismo.
        if (incoming.size() > 1 && outgoing.size() > 1) {
            token.setLifecycle("waiting");
            token.setUpdatedAt(now);
            tokenRepo.save(token);
            UUID myParent = token.getParentTokenId();
            List<TokenEntity> siblings = tokenRepo
                .findByProcessinstanceIdAndCurrentElementIdAndLifecycle(instance.getId(), gateway.id(), "waiting")
                .stream().filter(t -> Objects.equals(t.getParentTokenId(), myParent)).toList();
            if (siblings.size() < incoming.size()) {
                return;   // esperar mas ramas entrantes
            }
            for (TokenEntity sib : siblings) {
                sib.setLifecycle("consumed");
                sib.setUpdatedAt(now);
                tokenRepo.save(sib);
            }
            UUID grandparent = myParent == null ? null
                : tokenRepo.findById(myParent).map(TokenEntity::getParentTokenId).orElse(null);
            audit(instance, "gateway.joinsplit", gateway.id(), null, userId, Map.of(
                "gatewayCode", gateway.code(),
                "branchesJoined", siblings.size(),
                "branchesSplit", outgoing.size()
            ));
            metricInc("bpm.gateway.join", def);
            metricInc("bpm.gateway.split", def);
            // Fan-out N tokens, cada uno hijo del grandparent (nivel previo al join).
            for (ProcessDefinition.SequenceFlow sf : outgoing) {
                TokenEntity child = newToken(instance, sf.targetId(), grandparent);
                tokenRepo.save(child);
                ProcessDefinition.FlowElement target = def.findElementById(sf.targetId());
                audit(instance, "token.entered", sf.targetId(), child.getId(), userId, Map.of(
                    "elementCode", target != null ? target.code() : "?",
                    "elementType", target != null ? target.type() : "?",
                    "fromJoinSplit", gateway.code()
                ));
                advanceToken(instance, child, def, userId);
            }
            return;
        }

        // shape realmente inválido (ej 1-in/1-out ya cubierto; aca 0 flows)
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

    // ════════════════════════════════════════════════════════════════════════
    // inclusive_gateway (OR) — activa TODAS las ramas cuya condicion es true
    //
    // El gateway mas dificil del spec: en el SPLIT se activan N ramas (1..todas)
    // segun condiciones; el JOIN debe esperar EXACTAMENTE a las que se activaron
    // (no a incoming.size() como el parallel). Solucion: el token del SPLIT queda
    // como ANCLA (waiting) con mi_cardinality = ramas activadas; el JOIN espera esa
    // cardinalidad dinamica. Reusa el fan-out por parent_token_id del parallel.
    //
    // Limitacion MVP conocida: asume split-join estructurado (el 99% de los casos).
    // No hace el analisis de alcanzabilidad de tokens del spec completo.
    // Caso de uso: aprobacion que va a Finanzas si monto>X Y a Legal si contrato nuevo.
    // ════════════════════════════════════════════════════════════════════════

    private void handleInclusiveGateway(ProcessInstanceEntity instance, TokenEntity token,
                                        ProcessDefinition.FlowElement gateway,
                                        ProcessDefinition def, UUID userId) {
        List<ProcessDefinition.SequenceFlow> outgoing = def.outgoingFlows(gateway.id());
        List<ProcessDefinition.SequenceFlow> incoming = def.incomingFlows(gateway.id());
        OffsetDateTime now = OffsetDateTime.now();

        // ── SPLIT (1-in / N-out) ──────────────────────────────────────────────
        if (outgoing.size() > 1 && incoming.size() <= 1) {
            Map<String, Object> vars = currentVariablesAsMap(instance);
            List<ProcessDefinition.SequenceFlow> activated = new ArrayList<>();
            ProcessDefinition.SequenceFlow defaultFlow = null;
            for (ProcessDefinition.SequenceFlow sf : outgoing) {
                String cond = sf.conditionExpr();
                if (cond == null || cond.isBlank()) {
                    if (defaultFlow == null) defaultFlow = sf;   // primer flow sin condicion = default
                } else if (evaluateCondition(cond, vars)) {
                    activated.add(sf);
                }
            }
            // Ninguna condicion matcheo → tomar el default (si hay)
            if (activated.isEmpty() && defaultFlow != null) activated.add(defaultFlow);

            if (activated.isEmpty()) {
                log.warn("inclusive_gateway '{}' SPLIT: ninguna condicion matcheo y sin default — token blocked",
                    gateway.code());
                token.setLifecycle("waiting");
                tokenRepo.save(token);
                audit(instance, "inclusive.no_match", gateway.id(), token.getId(), userId,
                    Map.of("gatewayCode", gateway.code()));
                return;
            }

            // El token del split queda como ANCLA (waiting) con la cardinalidad
            // esperada = ramas activadas. El JOIN la usa para saber a cuantos esperar.
            token.setLifecycle("waiting");
            token.setMiCardinality(activated.size());
            token.setUpdatedAt(now);
            tokenRepo.save(token);
            audit(instance, "inclusive.split", gateway.id(), token.getId(), userId, Map.of(
                "gatewayCode", gateway.code(),
                "activated", activated.size(),
                "outgoingTotal", outgoing.size()
            ));
            metricInc("bpm.inclusive.split", def);

            for (ProcessDefinition.SequenceFlow sf : activated) {
                TokenEntity child = newToken(instance, sf.targetId(), token.getId());
                tokenRepo.save(child);
                ProcessDefinition.FlowElement target = def.findElementById(sf.targetId());
                audit(instance, "token.entered", sf.targetId(), child.getId(), userId, Map.of(
                    "elementCode", target != null ? target.code() : "?",
                    "elementType", target != null ? target.type() : "?",
                    "fromInclusiveSplit", gateway.code()
                ));
                advanceToken(instance, child, def, userId);
            }
            return;
        }

        // ── JOIN (M-in / 1-out) ───────────────────────────────────────────────
        if (incoming.size() > 1 && outgoing.size() == 1) {
            token.setLifecycle("waiting");
            token.setUpdatedAt(now);
            tokenRepo.save(token);
            audit(instance, "inclusive.join.arrived", gateway.id(), token.getId(), userId,
                Map.of("gatewayCode", gateway.code()));

            UUID myParent = token.getParentTokenId();
            // El ancla es el token del split (mismo parent_token_id). Lock pesimista
            // por si dos ramas async (user_task/timer) llegan concurrentes.
            TokenEntity anchor = myParent == null ? null
                : tokenRepo.findByIdForUpdate(myParent).orElse(null);
            int expected = (anchor != null && anchor.getMiCardinality() != null)
                ? anchor.getMiCardinality() : incoming.size();   // fallback defensivo

            List<TokenEntity> arrived = tokenRepo.findByProcessinstanceIdAndCurrentElementIdAndLifecycle(
                instance.getId(), gateway.id(), "waiting");
            List<TokenEntity> siblings = arrived.stream()
                .filter(t -> Objects.equals(t.getParentTokenId(), myParent))
                .toList();

            if (siblings.size() < expected) {
                audit(instance, "inclusive.join.progress", gateway.id(), token.getId(), userId,
                    Map.of("gatewayCode", gateway.code(), "arrived", siblings.size(), "expected", expected));
                return;   // esperar mas ramas activadas
            }

            for (TokenEntity sib : siblings) {
                sib.setLifecycle("consumed");
                sib.setUpdatedAt(now);
                tokenRepo.save(sib);
            }
            // Consume el ancla (split token) y restaura el nivel de paralelismo previo.
            UUID grandparent = anchor != null ? anchor.getParentTokenId() : null;
            if (anchor != null) {
                anchor.setLifecycle("consumed");
                anchor.setMiCardinality(null);
                anchor.setUpdatedAt(now);
                tokenRepo.save(anchor);
            }
            audit(instance, "inclusive.join.completed", gateway.id(), null, userId, Map.of(
                "gatewayCode", gateway.code(),
                "branchesJoined", siblings.size(),
                "expected", expected
            ));
            metricInc("bpm.inclusive.join", def);

            ProcessDefinition.SequenceFlow out = outgoing.get(0);
            TokenEntity outToken = newToken(instance, out.targetId(), grandparent);
            tokenRepo.save(outToken);
            ProcessDefinition.FlowElement target = def.findElementById(out.targetId());
            audit(instance, "token.entered", out.targetId(), outToken.getId(), userId, Map.of(
                "elementCode", target != null ? target.code() : "?",
                "elementType", target != null ? target.type() : "?",
                "fromInclusiveJoin", gateway.code()
            ));
            advanceToken(instance, outToken, def, userId);
            return;
        }

        // 1-in/1-out passthrough degenerado
        if (outgoing.size() == 1 && incoming.size() <= 1) {
            consumeAndMoveToNext(instance, token, gateway, def, userId);
            return;
        }

        log.warn("inclusive_gateway {} shape no soportado in={} out={} — token blocked",
            gateway.code(), incoming.size(), outgoing.size());
        token.setLifecycle("waiting");
        tokenRepo.save(token);
        audit(instance, "gateway.unsupported_shape", gateway.id(), token.getId(), userId, Map.of(
            "gatewayCode", gateway.code(), "incoming", incoming.size(), "outgoing", outgoing.size()));
    }

    // ════════════════════════════════════════════════════════════════════════
    // event_based_gateway — carrera de eventos ("esperá lo primero que pase")
    //
    // Al llegar el token, ARMA todas las ramas: cada outgoing apunta a un
    // intermediate_event (timer / message / signal) que se pone en waiting. El
    // PRIMERO que dispara gana; sus hermanos se cancelan (des-arma timers +
    // correlations + consume tokens). Reusa el fan-out por parent_token_id del
    // parallel_gateway y los primitivos de arme (handleIntermediateEvent) y de
    // disparo (fireIntermediateTimer / advanceFromCorrelation).
    //
    // Detección de rama-de-carrera 100% ESTRUCTURAL (sin schema nuevo): un
    // intermediate_event es rama de carrera si su source entrante es un
    // event_based_gateway. Los hermanos comparten parent_token_id = token del
    // gateway. Caso de uso: "esperá confirmación de pago (message) OR timeout 48h".
    // ════════════════════════════════════════════════════════════════════════

    private void handleEventBasedGateway(ProcessInstanceEntity instance, TokenEntity token,
                                         ProcessDefinition.FlowElement gateway,
                                         ProcessDefinition def, UUID userId) {
        List<ProcessDefinition.SequenceFlow> outgoing = def.outgoingFlows(gateway.id());
        if (outgoing.isEmpty()) {
            log.warn("event_based_gateway '{}' sin outgoing — token waiting", gateway.code());
            token.setLifecycle("waiting");
            tokenRepo.save(token);
            return;
        }

        OffsetDateTime now = OffsetDateTime.now();
        // Consume el token del gateway; cada rama es un token hijo (parent_token_id
        // = token del gateway) → agrupa a los hermanos para la cancelación.
        token.setLifecycle("consumed");
        token.setUpdatedAt(now);
        tokenRepo.save(token);
        audit(instance, "event_gateway.armed", gateway.id(), token.getId(), userId, Map.of(
            "gatewayCode", gateway.code(),
            "branches", outgoing.size()
        ));
        metricInc("bpm.event_gateway.armed", def);

        for (ProcessDefinition.SequenceFlow sf : outgoing) {
            TokenEntity child = newToken(instance, sf.targetId(), token.getId());
            tokenRepo.save(child);
            ProcessDefinition.FlowElement target = def.findElementById(sf.targetId());
            audit(instance, "token.entered", sf.targetId(), child.getId(), userId, Map.of(
                "elementCode", target != null ? target.code() : "?",
                "elementType", target != null ? target.type() : "?",
                "fromEventGateway", gateway.code()
            ));
            // Arma la rama (timer/message/signal → waiting). Cada intermediate_event
            // termina en 'waiting' hasta que su evento dispare.
            advanceToken(instance, child, def, userId);
        }
    }

    /**
     * Cancela las ramas perdedoras de un event_based_gateway cuando una gana.
     * Si `winnerEvent` es rama de carrera (su source es un event_based_gateway),
     * consume los tokens hermanos que siguen waiting en las otras ramas y
     * des-arma sus timers (jobs scheduled) y correlations (message/signal).
     * No-op si el evento no es rama de carrera.
     */
    private int cancelEventRaceSiblings(ProcessInstanceEntity instance, TokenEntity winnerToken,
                                        ProcessDefinition.FlowElement winnerEvent,
                                        ProcessDefinition def, UUID userId) {
        // ¿El source entrante del winner es un event_based_gateway?
        ProcessDefinition.FlowElement gateway = null;
        for (ProcessDefinition.SequenceFlow in : def.incomingFlows(winnerEvent.id())) {
            ProcessDefinition.FlowElement src = def.findElementById(in.sourceId());
            if (src != null && "event_based_gateway".equals(src.type())) { gateway = src; break; }
        }
        if (gateway == null) return 0;   // no es carrera

        UUID groupParent = winnerToken.getParentTokenId();
        OffsetDateTime now = OffsetDateTime.now();
        int cancelled = 0;

        for (ProcessDefinition.SequenceFlow sf : def.outgoingFlows(gateway.id())) {
            if (sf.targetId().equals(winnerEvent.id())) continue;   // la rama ganadora
            List<TokenEntity> waiters = tokenRepo.findByProcessinstanceIdAndCurrentElementIdAndLifecycle(
                instance.getId(), sf.targetId(), "waiting");
            for (TokenEntity loser : waiters) {
                // solo hermanos del mismo gateway (mismo parent_token_id)
                if (!Objects.equals(loser.getParentTokenId(), groupParent)) continue;

                // des-armar timers scheduled del loser
                for (JobExecutorEntity job : jobRepo.findByTokenIdAndLifecycle(loser.getId(), "scheduled")) {
                    job.setLifecycle("cancelled");
                    job.setUpdatedAt(now);
                    jobRepo.save(job);
                }
                // des-armar message/signal correlations waiting del loser
                for (MessageCorrelationEntity mc : msgCorrRepo.findByTokenIdAndLifecycle(loser.getId(), "waiting")) {
                    mc.setLifecycle("cancelled");
                    mc.setUpdatedAt(now);
                    msgCorrRepo.save(mc);
                }
                loser.setLifecycle("consumed");
                loser.setUpdatedAt(now);
                tokenRepo.save(loser);
                ProcessDefinition.FlowElement loserEl = def.findElementById(sf.targetId());
                audit(instance, "event_gateway.branch_cancelled", sf.targetId(), loser.getId(), userId, Map.of(
                    "elementCode", loserEl != null ? loserEl.code() : "?",
                    "gatewayCode", gateway.code(),
                    "winnerCode", winnerEvent.code()
                ));
                cancelled++;
            }
        }
        if (cancelled > 0) {
            audit(instance, "event_gateway.resolved", gateway.id(), winnerToken.getId(), userId, Map.of(
                "gatewayCode", gateway.code(),
                "winnerCode", winnerEvent.code(),
                "cancelledBranches", cancelled
            ));
            metricInc("bpm.event_gateway.resolved", def);
        }
        return cancelled;
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
    private void handleIntermediateEvent(ProcessInstanceEntity instance, TokenEntity token,
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

    private void handleTimerEvent(ProcessInstanceEntity instance, TokenEntity token,
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

        JobExecutorEntity job = new JobExecutorEntity();
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
        metricInc("bpm.timer.scheduled", instance.getProcessdefId().toString());
    }

    private void handleMessageEvent(ProcessInstanceEntity instance, TokenEntity token,
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

        MessageCorrelationEntity mc = new MessageCorrelationEntity();
        mc.setId(UUID.randomUUID());
        mc.setTenantId(instance.getTenantId());
        mc.setProcessinstanceId(instance.getId());
        mc.setTokenId(token.getId());
        mc.setMessagedefId(MessageCorrelationEntity.messageRefId(messageCode));
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
        metricInc("bpm.message.subscribed", instance.getProcessdefId().toString());
    }

    private void handleSignalEvent(ProcessInstanceEntity instance, TokenEntity token,
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

        MessageCorrelationEntity mc = new MessageCorrelationEntity();
        mc.setId(UUID.randomUUID());
        mc.setTenantId(instance.getTenantId());
        mc.setProcessinstanceId(instance.getId());
        mc.setTokenId(token.getId());
        mc.setMessagedefId(MessageCorrelationEntity.signalRefId(signalCode));
        mc.setCorrelationKey(MessageCorrelationEntity.BROADCAST_KEY);
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
        metricInc("bpm.signal.subscribed", instance.getProcessdefId().toString());
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
     *       "returnVariables": ["validationResult"],      // opcional
     *       "waitForCompletion": true                     // default true
     *     }
     *   }
     *
     * Flujos:
     *   - waitForCompletion=true (default):
     *       1. TokenEntity parent → waiting
     *       2. Spawn child con parent_instance_id + parent_token_id
     *       3. Cuando child completa, notifyParentOfSubprocessCompletion reactiva
     *          el token waiting del parent y avanza al siguiente flow_element.
     *   - waitForCompletion=false (fire-and-forget):
     *       1. Spawn child con parent_instance_id pero parent_token_id=null
     *          (no hay token waiting al cual volver)
     *       2. Avanza el token parent INMEDIATAMENTE al siguiente flow_element.
     *       3. Child sigue corriendo por su cuenta; al completar audita pero
     *          no notifica al parent (notifyParent skip por parent_token_id=null).
     *
     * El child puede completarse instantáneamente (procesos de solo service_tasks
     * + gateways) — no hay race condition porque el parent token ya está marcado
     * waiting (caso wait) o ya pasó al siguiente (caso fire-and-forget).
     */
    @SuppressWarnings("unchecked")
    private void handleSubProcess(ProcessInstanceEntity instance, TokenEntity token,
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

        // waitForCompletion: default true (parent espera). Si false, fire-and-forget.
        boolean waitForCompletion = !(Boolean.FALSE.equals(callCfg.get("waitForCompletion")));

        OffsetDateTime now = OffsetDateTime.now();
        if (waitForCompletion) {
            token.setLifecycle("waiting");
            token.setUpdatedAt(now);
            tokenRepo.save(token);
        }
        // En fire-and-forget el token queda active y avanza al final del método.

        audit(instance, "subprocess.spawning", subProcess.id(), token.getId(), userId, Map.of(
            "elementCode", subProcess.code(),
            "calledProcessversionId", calledProcessVersionId.toString(),
            "passVariables", new ArrayList<>(childPayload.keySet()),   // Set→List (Jackson)
            "waitForCompletion", waitForCompletion
        ));

        // Spawn child. Si fire-and-forget, parent_token_id=null para que
        // notifyParentOfSubprocessCompletion sepa que no hay token al cual volver.
        ProcessInstanceEntity child;
        try {
            child = startProcessInternal(calledProcessVersionId, childPayload,
                null /*bearer*/, instance.getTenantId(), userId,
                instance.getId(),
                waitForCompletion ? token.getId() : null);
        } catch (Exception ex) {
            log.error("sub_process '{}' spawn failed", subProcess.code(), ex);
            // Reactivar token (si estaba waiting) y pasar al siguiente
            if (waitForCompletion) {
                token.setLifecycle("active");
                token.setUpdatedAt(OffsetDateTime.now());
                tokenRepo.save(token);
            }
            audit(instance, "subprocess.spawn_failed", subProcess.id(), token.getId(), userId,
                Map.of("elementCode", subProcess.code(), "error", ex.getMessage()));
            consumeAndMoveToNext(instance, token, subProcess, def, userId);
            return;
        }

        audit(instance, "subprocess.spawned", subProcess.id(), token.getId(), userId, Map.of(
            "elementCode", subProcess.code(),
            "childInstanceId", child.getId().toString(),
            "childLifecycle", child.getLifecycle(),
            "waitForCompletion", waitForCompletion
        ));
        log.info("sub_process '{}' spawned child {} (lifecycle={}, wait={})",
            subProcess.code(), child.getId(), child.getLifecycle(), waitForCompletion);
        metricInc("bpm.subprocess.spawned", def);

        // Fire-and-forget: avanzar el parent INMEDIATAMENTE al siguiente flow_element.
        // El child sigue su curso; no hay return aquí.
        if (!waitForCompletion) {
            consumeAndMoveToNext(instance, token, subProcess, def, userId);
        }
    }

    /**
     * Llamado desde handleEndEvent del child cuando termina. Copia las
     * `returnVariables` declaradas en el sub_process flow_element del parent
     * desde el child al parent, reactiva el token del parent y avanza.
     */
    @SuppressWarnings("unchecked")
    private void notifyParentOfSubprocessCompletion(ProcessInstanceEntity child, UUID userId) {
        UUID parentInstanceId = child.getParentInstanceId();
        UUID parentTokenId    = child.getParentTokenId();
        ProcessInstanceEntity parent = instanceRepo.findById(parentInstanceId).orElse(null);
        if (parent == null) {
            log.warn("notifyParent: parent instance {} not found", parentInstanceId);
            return;
        }
        if (!"active".equals(parent.getLifecycle())) {
            log.info("notifyParent: parent {} no longer active ({})",
                parent.getId(), parent.getLifecycle());
            return;
        }
        TokenEntity parentToken = tokenRepo.findById(parentTokenId).orElse(null);
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

        // Multi-instance: si el parentToken es un hijo de MI (su parentTokenId es
        // el token ancla), no reactivamos+avanzamos — desviamos al join por
        // cardinalidad. El output del child = sus returnVariables (para outputCollection).
        TokenEntity miAnchor = miAnchorOf(parentToken);
        if (miAnchor != null) {
            Map<String, Object> childOutput = extractReturnVars(subProcessElement, child);
            completeMultiInstanceChild(parent, parentToken, miAnchor, childOutput, null, userId);
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

    // ════════════════════════════════════════════════════════════════════════
    // Multi-instance parallel (MVP) — marcador sobre user_task / sub_process
    //
    // Un token entra a la activity marcada con config.multiInstance. Se hace
    // fan-out de N ejecuciones (una por item de una coleccion de runtime): el
    // token que entro queda como ANCLA (mi_cardinality=N, lifecycle=waiting) y
    // se crean N tokens hijos que ejecutan el cuerpo de la activity. Cuando los
    // N terminan (join por cardinalidad, anti-race con lock del ancla), el ancla
    // avanza al unico outgoing.
    //
    // Reusa el modelo probado de parallel_gateway (fan-out por parent_token_id)
    // y sub_process (spawn hijo + notifyParent). El item viaja por el input_data
    // de la task (user_task) o el payload de la instancia hija (sub_process) — no
    // se introduce scope de variable global por token en el MVP.
    // Diferido (ver IMAP_MOTOR_BPM.md §8): sequential MI + completionCondition.
    // ════════════════════════════════════════════════════════════════════════

    @SuppressWarnings("unchecked")
    private void handleMultiInstanceSplit(ProcessInstanceEntity instance, TokenEntity anchor,
                                          ProcessDefinition.FlowElement activity,
                                          ProcessDefinition def, UUID userId) {
        Map<String, Object> mi = activity.multiInstance();
        String collectionExpr = stringOr(mi.get("collection"), null);
        String elementVar = stringOr(mi.get("elementVar"), "item");
        String mode = stringOr(mi.get("mode"), "parallel");
        boolean sequential = "sequential".equals(mode);   // parallel = default (validado en create)

        List<Object> items = resolveCollectionValue(instance, collectionExpr);
        int n = items.size();

        // Coleccion vacia → skip la activity (semantica BPMN: 0 instancias).
        if (n == 0) {
            audit(instance, "mi.empty", activity.id(), anchor.getId(), userId,
                Map.of("elementCode", activity.code(),
                       "collection", collectionExpr == null ? "(null)" : collectionExpr));
            consumeAndMoveToNext(instance, anchor, activity, def, userId);
            return;
        }

        // El ancla queda esperando; guarda N para el join por cardinalidad.
        OffsetDateTime now = OffsetDateTime.now();
        anchor.setMiCardinality(n);
        anchor.setLifecycle("waiting");
        anchor.setUpdatedAt(now);
        tokenRepo.save(anchor);

        audit(instance, "mi.split", activity.id(), anchor.getId(), userId, Map.of(
            "elementCode", activity.code(),
            "mode", mode,
            "cardinality", n,
            "elementVar", elementVar));
        metricInc("bpm.mi.split", def);
        log.info("multiInstance '{}' split → {} ejecuciones (mode={}, elementVar={})",
            activity.code(), n, mode, elementVar);

        if (sequential) {
            // Secuencial: solo el PRIMER item; los siguientes se crean al completar cada uno.
            spawnMiChild(instance, anchor, activity, def, items.get(0), 0, n, elementVar, userId);
        } else {
            // Parallel: fan-out de los N a la vez.
            for (int i = 0; i < n; i++) {
                spawnMiChild(instance, anchor, activity, def, items.get(i), i, n, elementVar, userId);
            }
        }
    }

    /** Crea 1 token hijo de MI + ejecuta el cuerpo de la activity para su item. */
    private void spawnMiChild(ProcessInstanceEntity instance, TokenEntity anchor,
                              ProcessDefinition.FlowElement activity, ProcessDefinition def,
                              Object item, int index, int total, String elementVar, UUID userId) {
        TokenEntity child = newToken(instance, activity.id(), anchor.getId());
        child.setLifecycle("waiting");   // espera la completacion del cuerpo
        tokenRepo.save(child);

        Map<String, Object> loopCtx = new LinkedHashMap<>();
        loopCtx.put(elementVar, item);
        loopCtx.put("loopCounter", index);
        loopCtx.put("nrOfInstances", total);

        if ("user_task".equals(activity.type())) {
            createMiUserTask(instance, child, activity, def, loopCtx, userId);
        } else { // sub_process
            spawnMiSubprocess(instance, child, activity, def, loopCtx, userId);
        }
        audit(instance, "mi.instance.created", activity.id(), child.getId(), userId,
            Map.of("elementCode", activity.code(), "index", index));
    }

    /** Crea la TaskInstanceEntity de UNA ejecucion MI (mirror de handleUserTask, con item en el input_data). */
    private void createMiUserTask(ProcessInstanceEntity instance, TokenEntity childToken,
                                  ProcessDefinition.FlowElement userTask, ProcessDefinition def,
                                  Map<String, Object> loopCtx, UUID userId) {
        Map<String, Object> inputData = currentVariablesAsMap(instance);
        inputData.putAll(loopCtx);   // item + loopCounter + nrOfInstances

        OffsetDateTime now = OffsetDateTime.now();
        TaskInstanceEntity task = new TaskInstanceEntity();
        task.setId(UUID.randomUUID());
        task.setTenantId(instance.getTenantId());
        task.setProcessinstanceId(instance.getId());
        task.setFlowelementId(userTask.id());
        task.setTokenId(childToken.getId());
        task.setLifecycle("created");
        task.setPriority(50);
        task.setInputData(inputData);
        String candidateGroup = taskAssignmentService.resolveCandidateGroup(
            userTask.config(), inputData, instance.getTenantId());
        if (candidateGroup != null) {
            task.setAssignedRole(candidateGroup);
        } else {
            UUID assignee = taskAssignmentService.resolveAssignee(
                def.processdefCode(), userTask.code(),
                instance.getStartedById(), instance.getTenantId());
            task.setAssignedUserId(assignee);
            task.setAssignedAt(now);
        }
        task.setStateId(DEFAULT_STATE_ACTIVE);
        task.setCreatedAt(now);
        task.setUpdatedAt(now);
        taskRepo.save(task);
        metricInc("bpm.task.created", def);
    }

    /** Spawnea la instancia hija de UNA ejecucion MI (mirror de handleSubProcess, con item en el payload). */
    @SuppressWarnings("unchecked")
    private void spawnMiSubprocess(ProcessInstanceEntity instance, TokenEntity childToken,
                                   ProcessDefinition.FlowElement subProcess, ProcessDefinition def,
                                   Map<String, Object> loopCtx, UUID userId) {
        Map<String, Object> cfg = subProcess.config();
        Map<String, Object> callCfg = cfg == null ? null : (Map<String, Object>) cfg.get("callActivity");
        if (callCfg == null) {
            log.warn("multiInstance sub_process '{}' sin callActivity — consumiendo token hijo", subProcess.code());
            childToken.setLifecycle("consumed");
            tokenRepo.save(childToken);
            return;
        }
        String calledVerId = stringOr(callCfg.get("calledProcessversionId"), null);
        UUID calledProcessVersionId;
        try {
            calledProcessVersionId = UUID.fromString(calledVerId);
        } catch (Exception e) {
            log.error("multiInstance sub_process '{}' calledProcessversionId invalido: {}",
                subProcess.code(), calledVerId);
            childToken.setLifecycle("consumed");
            tokenRepo.save(childToken);
            return;
        }
        Map<String, Object> parentVars = currentVariablesAsMap(instance);
        Map<String, Object> childPayload = new LinkedHashMap<>();
        if (callCfg.get("passVariables") instanceof List<?> list) {
            for (Object v : list) {
                String name = String.valueOf(v);
                if (parentVars.containsKey(name)) childPayload.put(name, parentVars.get(name));
            }
        }
        childPayload.putAll(loopCtx);   // item + loopCounter + nrOfInstances
        // parent_token_id = childToken.id → al terminar, notifyParent vuelve por acá.
        startProcessInternal(calledProcessVersionId, childPayload, null,
            instance.getTenantId(), userId, instance.getId(), childToken.getId());
    }

    /**
     * Join por cardinalidad de una activity multi-instanciada. Anti-race: lock
     * pesimista del ancla → exactamente una completacion cruza el umbral N y
     * avanza. Consume el token hijo, cuenta hermanos consumidos y, si llegaron
     * todos, reactiva el ancla y avanza al outgoing.
     */
    private void completeMultiInstanceChild(ProcessInstanceEntity instance, TokenEntity childToken,
                                            TokenEntity anchorArg, Map<String, Object> childOutput,
                                            String bearerToken, UUID userId) {
        OffsetDateTime now = OffsetDateTime.now();

        // 1. Consumir el token hijo (su cuerpo termino).
        childToken.setLifecycle("consumed");
        childToken.setUpdatedAt(now);
        tokenRepo.save(childToken);

        // 2. Lock del ancla — serializa las completaciones concurrentes.
        TokenEntity anchor = tokenRepo.findByIdForUpdate(anchorArg.getId()).orElse(anchorArg);
        UUID activityId = anchor.getCurrentElementId();
        ProcessDefinition def = loader.load(instance.getProcessversionId(), bearerToken, instance.getTenantId());
        ProcessDefinition.FlowElement activity = def.findElementById(activityId);

        // 3. Acumular output en outputCollection (opcional), bajo el lock → sin lost update.
        if (activity != null && childOutput != null && !childOutput.isEmpty()) {
            appendMiOutput(instance, activity, childOutput);
        }

        // 4. Estado del MI + config (mode / completionCondition).
        int completed = tokenRepo.countByParentTokenIdAndCurrentElementIdAndLifecycle(
            anchor.getId(), activityId, "consumed");
        int total = anchor.getMiCardinality() == null ? completed : anchor.getMiCardinality();
        Map<String, Object> mi = activity != null ? activity.multiInstance() : null;
        boolean sequential = mi != null && "sequential".equals(stringOr(mi.get("mode"), "parallel"));
        String completionCondition = mi == null ? null : stringOr(mi.get("completionCondition"), null);

        // Hijos aun vivos (waiting) de este MI — para nrOfActiveInstances + cancelacion.
        List<TokenEntity> activeChildren = tokenRepo
            .findByProcessinstanceIdAndCurrentElementIdAndLifecycle(instance.getId(), activityId, "waiting")
            .stream().filter(t -> Objects.equals(t.getParentTokenId(), anchor.getId())).toList();

        // completionCondition (quorum / fail-fast): se evalua tras cada hijo, con los counters
        // MI + el output del hijo recien completado. Si da true → corte anticipado.
        boolean conditionMet = false;
        if (completionCondition != null && !completionCondition.isBlank()) {
            Map<String, Object> ctx = currentVariablesAsMap(instance);
            if (childOutput != null) ctx.putAll(childOutput);
            ctx.put("nrOfInstances", total);
            ctx.put("nrOfCompletedInstances", completed);
            ctx.put("nrOfActiveInstances", activeChildren.size());
            conditionMet = evaluateCondition(completionCondition, ctx);
        }
        boolean allDone = completed >= total;

        if (conditionMet || allDone) {
            int cancelled = 0;
            if (conditionMet && !allDone) {   // corte anticipado → cancelar las que siguen vivas
                cancelled = cancelRemainingMiChildren(instance, activeChildren, activityId, userId);
            }
            anchor.setLifecycle("active");
            anchor.setMiCardinality(null);   // higiene: el ancla deja de ser MI al avanzar
            anchor.setUpdatedAt(now);
            tokenRepo.save(anchor);
            audit(instance, "mi.join.completed", activityId, anchor.getId(), userId, Map.of(
                "elementCode", activity != null ? activity.code() : "?",
                "cardinality", total, "completed", completed,
                "reason", (conditionMet && !allDone) ? "completionCondition" : "all",
                "cancelledRemaining", cancelled));
            metricInc("bpm.mi.completed", def);
            log.info("multiInstance '{}' join completo ({}/{}, reason={}) → avanzando",
                activity != null ? activity.code() : "?", completed, total,
                (conditionMet && !allDone) ? "condition" : "all");
            if (activity != null) consumeAndMoveToNext(instance, anchor, activity, def, userId);
            return;
        }

        // No termino: sequential → crear el siguiente item; parallel → seguir esperando.
        if (sequential && activity != null) {
            List<Object> items = resolveCollectionValue(instance, stringOr(mi.get("collection"), null));
            String elementVar = stringOr(mi.get("elementVar"), "item");
            if (completed < items.size()) {
                spawnMiChild(instance, anchor, activity, def, items.get(completed), completed, total, elementVar, userId);
                audit(instance, "mi.sequential.next", activityId, anchor.getId(), userId,
                    Map.of("index", completed, "total", total));
            }
        } else {
            audit(instance, "mi.join.progress", activityId, anchor.getId(), userId,
                Map.of("completed", completed, "total", total));
        }
    }

    /** completionCondition: cancela las ejecuciones MI que siguen vivas (tasks + tokens + child-instances). */
    private int cancelRemainingMiChildren(ProcessInstanceEntity instance, List<TokenEntity> children,
                                          UUID activityId, UUID userId) {
        OffsetDateTime now = OffsetDateTime.now();
        int cancelled = 0;
        for (TokenEntity c : children) {
            for (TaskInstanceEntity t : taskRepo.findByTokenIdAndLifecycleIn(
                    c.getId(), List.of("created", "assigned", "in_progress"))) {
                t.setLifecycle("cancelled");
                t.setUpdatedAt(now);
                taskRepo.save(t);
            }
            // Hijos de sub_process MI (si el body era sub_process) → cancelar la instancia hija.
            for (ProcessInstanceEntity child : instanceRepo.findByParentTokenId(c.getId())) {
                if ("active".equals(child.getLifecycle())) {
                    cancelInstance(child.getId(), "mi_completion_condition", userId);
                }
            }
            c.setLifecycle("consumed");
            c.setUpdatedAt(now);
            tokenRepo.save(c);
            audit(instance, "mi.instance.cancelled", activityId, c.getId(), userId,
                Map.of("reason", "completionCondition"));
            cancelled++;
        }
        return cancelled;
    }

    /** Acumula el output de una ejecucion MI en la var List config.multiInstance.outputCollection (si esta declarada). */
    private void appendMiOutput(ProcessInstanceEntity instance, ProcessDefinition.FlowElement activity,
                                Map<String, Object> item) {
        Map<String, Object> mi = activity.multiInstance();
        Object outColObj = mi == null ? null : mi.get("outputCollection");
        if (!(outColObj instanceof String outCol) || outCol.isBlank()) return;
        Object existing = currentVariablesAsMap(instance).get(outCol);
        List<Object> list = new ArrayList<>();
        if (existing instanceof List<?> l) list.addAll(l);
        list.add(item);
        setVariable(instance, outCol, list);
    }

    /** Extrae las returnVariables declaradas del child (para el outputCollection de un sub_process MI). */
    @SuppressWarnings("unchecked")
    private Map<String, Object> extractReturnVars(ProcessDefinition.FlowElement subProcessElement,
                                                  ProcessInstanceEntity child) {
        Map<String, Object> cfg = subProcessElement.config();
        Map<String, Object> callCfg = cfg == null ? null : (Map<String, Object>) cfg.get("callActivity");
        Map<String, Object> out = new LinkedHashMap<>();
        if (callCfg != null && callCfg.get("returnVariables") instanceof List<?> retList) {
            Map<String, Object> childVars = currentVariablesAsMap(child);
            for (Object v : retList) {
                String name = String.valueOf(v);
                if (childVars.containsKey(name)) out.put(name, childVars.get(name));
            }
        }
        return out;
    }

    /**
     * Devuelve el token ancla si `token` es un hijo de multi-instance, o null.
     * Discriminador vs parallel_gateway: el ancla tiene mi_cardinality != null y
     * esta parado en el MISMO element que el hijo (la activity multi-instanciada).
     */
    private TokenEntity miAnchorOf(TokenEntity token) {
        if (token == null || token.getParentTokenId() == null) return null;
        TokenEntity anchor = tokenRepo.findById(token.getParentTokenId()).orElse(null);
        if (anchor != null && anchor.getMiCardinality() != null
            && anchor.getCurrentElementId().equals(token.getCurrentElementId())) {
            return anchor;
        }
        return null;
    }

    /** Resuelve config.multiInstance.collection (JEXL ${..} o nombre de var) a una List. */
    @SuppressWarnings("unchecked")
    private List<Object> resolveCollectionValue(ProcessInstanceEntity instance, String expr) {
        if (expr == null || expr.isBlank()) return List.of();
        Map<String, Object> vars = currentVariablesAsMap(instance);
        Object raw;
        if (expr.startsWith("${") && expr.endsWith("}")) {
            String script = expr.substring(2, expr.length() - 1);
            try {
                JexlContext ctx = new MapContext();
                for (Map.Entry<String, Object> e : vars.entrySet()) ctx.set(e.getKey(), e.getValue());
                raw = jexl.createScript(script).execute(ctx);
            } catch (Exception e) {
                log.warn("multiInstance collection expr '{}' fallo: {}", expr, e.getMessage());
                return List.of();
            }
        } else {
            raw = vars.get(expr);
        }
        if (raw instanceof List<?> l) return new ArrayList<>((List<Object>) l);
        if (raw instanceof Collection<?> c) return new ArrayList<>((Collection<Object>) c);
        if (raw == null) return List.of();
        log.warn("multiInstance collection '{}' no resolvio a una List (fue {})",
            expr, raw.getClass().getSimpleName());
        return List.of();
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
    private void handleBusinessRuleTask(ProcessInstanceEntity instance, TokenEntity token,
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
            // TokenEntity waiting → admin debe intervenir
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

        // No-match: ninguna rule matcheó. NOTA: matchedRule==null también
        // ocurre en collect/rule-order/output-order cuando hubo matches
        // (multi-output sin "winner"). Discriminar por totalMatched.
        if (result.totalMatched() == 0) {
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

        Map<String, Object> auditData = new LinkedHashMap<>();
        auditData.put("elementCode", brt.code());
        auditData.put("decisionRef", decisionRef);
        auditData.put("hitPolicy", decision.hitPolicy());
        auditData.put("matchedRulePriority", result.matchedRule() != null
            ? result.matchedRule().priority() : -1);   // -1 cuando multi-output (collect/rule-order/output-order)
        auditData.put("totalMatched", result.totalMatched());
        auditData.put("inputs", inputs);
        auditData.put("outputs", result.outputs());
        audit(instance, "decision.evaluated", brt.id(), token.getId(), userId, auditData);
        log.info("business_rule_task '{}' decision '{}' → totalMatched={} outputs={}",
            brt.code(), decisionRef, result.totalMatched(), result.outputs());
        meterRegistry.counter("bpm.decision.evaluated",
            Tags.of("decision", decisionRef)).increment();

        consumeAndMoveToNext(instance, token, brt, def, userId);
    }

    /**
     * Audita en el parent que un child fire-and-forget terminó. No avanza
     * ningún token porque el parent ya pasó al siguiente flow_element cuando
     * spawnneó al child. Útil para observabilidad/debug.
     */
    private void auditFireAndForgetChildCompletion(ProcessInstanceEntity child, UUID userId) {
        ProcessInstanceEntity parent = instanceRepo.findById(child.getParentInstanceId()).orElse(null);
        if (parent == null) {
            log.debug("auditFireAndForget: parent {} gone", child.getParentInstanceId());
            return;
        }
        audit(parent, "subprocess.detached_completed", null, null, userId, Map.of(
            "childInstanceId", child.getId().toString(),
            "childLifecycle", child.getLifecycle()
        ));
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
        UUID msgRefId = MessageCorrelationEntity.messageRefId(messageCode);

        List<MessageCorrelationEntity> matches = msgCorrRepo
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
        MessageCorrelationEntity mc = matches.get(0);
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
        UUID sigRefId = MessageCorrelationEntity.signalRefId(signalCode);
        List<MessageCorrelationEntity> matches = msgCorrRepo
            .findByMessagedefIdAndLifecycle(sigRefId, "waiting");
        if (matches.isEmpty()) {
            log.info("broadcastSignal: no listener for signalCode={}", signalCode);
            return 0;
        }
        int reactivated = 0;
        for (MessageCorrelationEntity mc : matches) {
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
    private boolean advanceFromCorrelation(MessageCorrelationEntity mc,
                                            Map<String, Object> payload,
                                            String auditEvent) {
        OffsetDateTime now = OffsetDateTime.now();
        mc.setLifecycle("matched");
        mc.setMatchedAt(now);
        mc.setMatchedPayload(payload);
        mc.setUpdatedAt(now);
        msgCorrRepo.save(mc);

        ProcessInstanceEntity instance = instanceRepo.findById(mc.getProcessinstanceId()).orElse(null);
        if (instance == null) {
            log.warn("advanceFromCorrelation: instance gone for correlation {}", mc.getId());
            return false;
        }
        if (!"active".equals(instance.getLifecycle())) {
            log.info("advanceFromCorrelation: instance {} no longer active ({})",
                instance.getId(), instance.getLifecycle());
            return false;
        }
        TokenEntity token = tokenRepo.findById(mc.getTokenId()).orElse(null);
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
        // Event-based gateway: si este evento era una rama de carrera, cancelar
        // las ramas perdedoras (des-armar sus timers/correlations + consumir tokens).
        cancelEventRaceSiblings(instance, token, current, def, null);
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
    // NOTA: el overload fireTimerJob(jobId, tenantId) fue removido porque
    // la llamada interna `this.fireTimerJob(jobId)` bypasea el proxy CGLIB
    // de Spring → @Transactional no se aplica → MANDATORY falla. El caller
    // (JobExecutorWorker) debe pre-setear TenantContextHolder + BearerTokenHolder
    // ANTES de invocar fireTimerJob(jobId).

    @Transactional
    public void fireTimerJob(UUID jobId) {
        // Aplicar tenant (si el caller lo preseteó) ANTES de findById, sino
        // RLS filtra el job y devuelve null pese a que existe.
        tenantSession.applyToCurrentTransaction();

        JobExecutorEntity job = jobRepo.findById(jobId).orElse(null);
        if (job == null) {
            log.warn("fireTimerJob: job {} not found", jobId);
            return;
        }
        // Aceptar tanto 'firing' (claimed por el worker scan) como 'scheduled'
        // (legacy/test path). Si está en otro estado (cancelled/fired), skip.
        if (!"firing".equals(job.getLifecycle()) && !"scheduled".equals(job.getLifecycle())) {
            log.debug("fireTimerJob: job {} not active (lifecycle={}) — skip",
                jobId, job.getLifecycle());
            return;
        }

        OffsetDateTime now = OffsetDateTime.now();
        if (!"firing".equals(job.getLifecycle())) {
            job.setLifecycle("firing");
            job.setUpdatedAt(now);
            jobRepo.save(job);
        }

        try {
            ProcessInstanceEntity instance = instanceRepo.findById(job.getProcessinstanceId()).orElse(null);
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
            TokenEntity token = tokenRepo.findById(job.getTokenId()).orElse(null);
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
    private void fireIntermediateTimer(ProcessInstanceEntity instance, TokenEntity token, JobExecutorEntity job) {
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
        metricInc("bpm.timer.fired", instance.getProcessdefId().toString());
        // Event-based gateway: si este timer era una rama de carrera, cancelar
        // las ramas perdedoras antes de avanzar el ganador.
        cancelEventRaceSiblings(instance, token, current, def, null);
        consumeAndMoveToNext(instance, token, current, def, null);
    }

    /**
     * B2 — dispara un boundary timer: avanza un nuevo token por el outgoing
     * del boundary_event. Si interrupting=true, además cancela la activity.
     *
     * Pasos:
     *   1. Cancelar OTRAS boundary jobs del mismo token (siempre, para no
     *      duplicar disparos si había varios timers adjuntos al mismo activity)
     *   2. Si interrupting=true: cancelar TaskInstanceEntity + consumir activity token
     *   3. Crear NUEVO token activo en el outgoing del boundary
     *   4. Avanzar el nuevo token
     *
     * Non-interrupting (interrupting=false): la activity sigue activa,
     * solo se emite un token paralelo en el outgoing. El job queda fired
     * (no re-dispara). Para que vuelva a dispararse en la misma activity
     * habría que modelar el boundary como timer cíclico (futuro).
     */
    @SuppressWarnings("unchecked")
    private void fireBoundaryTimer(ProcessInstanceEntity instance, TokenEntity activityToken, JobExecutorEntity job) {
        Map<String, Object> cfg = job.getConfig();
        boolean interrupting = !(Boolean.FALSE.equals(cfg.get("interrupting")));
        UUID boundaryElementId = UUID.fromString((String) cfg.get("boundaryElementId"));
        String boundaryCode = (String) cfg.getOrDefault("boundaryElementCode", "?");
        String attachedToCode = (String) cfg.getOrDefault("attachedToElementCode", "?");
        UUID attachedTaskId = cfg.get("attachedTaskInstanceId") == null ? null
            : UUID.fromString((String) cfg.get("attachedTaskInstanceId"));

        fireBoundaryHandler(instance, activityToken, boundaryElementId, boundaryCode,
            attachedToCode, attachedTaskId, interrupting,
            job.getId(),  // skip self cuando cancela siblings
            "boundary.fired", "boundary_timer_fired",
            Map.of("trigger", "timer"));

        // Timer ciclico: si es non-interrupting y quedan repeticiones, reprograma
        // el proximo. El activityToken sigue waiting (non-interrupting no lo consume),
        // asi que el nuevo job es valido. Cuando la activity se completa,
        // cancelBoundaryJobsForToken cancela el job scheduled pendiente.
        if (!interrupting && cfg.get("repeatEverySeconds") != null) {
            int repeatCount = cfg.get("repeatCount") instanceof Number rc ? rc.intValue() : 1;
            int maxRepeats  = cfg.get("maxRepeats") instanceof Number mr ? mr.intValue() : 10;
            if (repeatCount < maxRepeats) {
                long every = ((Number) cfg.get("repeatEverySeconds")).longValue();
                OffsetDateTime now = OffsetDateTime.now();
                JobExecutorEntity next = new JobExecutorEntity();
                next.setId(UUID.randomUUID());
                next.setTenantId(instance.getTenantId());
                next.setProcessinstanceId(instance.getId());
                next.setTokenId(activityToken.getId());
                next.setJobType("timer");
                next.setFireAt(now.plusSeconds(every));
                Map<String, Object> nextCfg = new LinkedHashMap<>(cfg);
                nextCfg.put("repeatCount", repeatCount + 1);
                next.setConfig(nextCfg);
                next.setLifecycle("scheduled");
                next.setRetries(0);
                next.setMaxRetries(3);
                next.setStateId(DEFAULT_STATE_ACTIVE);
                next.setCreatedAt(now);
                next.setUpdatedAt(now);
                jobRepo.save(next);
                audit(instance, "boundary.timer.rescheduled", boundaryElementId, activityToken.getId(), null, Map.of(
                    "boundaryCode", boundaryCode,
                    "repeatCount", repeatCount + 1,
                    "maxRepeats", maxRepeats,
                    "fireAt", next.getFireAt().toString()
                ));
            }
        }
    }

    /**
     * Lógica unificada del disparo de un boundary_event (timer OR error/escalation).
     * - Cancela siblings boundary jobs del activity token (para que no dupliquen)
     * - Si interrupting: cancela TaskInstanceEntity + consumes activity token
     * - Crea nuevo token en outgoing del boundary y lo avanza
     *
     * @param selfJobIdToSkip id del job que dispara actualmente (null si no aplica,
     *                         como en error boundary que no viene de un job).
     */
    private void fireBoundaryHandler(ProcessInstanceEntity instance, TokenEntity activityToken,
                                      UUID boundaryElementId, String boundaryCode,
                                      String attachedToCode, UUID attachedTaskId,
                                      boolean interrupting, UUID selfJobIdToSkip,
                                      String auditEvent, String cancelReason,
                                      Map<String, Object> extraAuditData) {
        OffsetDateTime now = OffsetDateTime.now();

        // 1. Cancelar OTROS boundary jobs scheduled del mismo token. Sólo
        //    aplica si interrupting=true (sino la activity sigue y los otros
        //    boundaries deben seguir armados).
        int cancelledSiblings = 0;
        if (interrupting) {
            List<JobExecutorEntity> siblings = jobRepo.findByTokenIdAndLifecycle(activityToken.getId(), "scheduled");
            for (JobExecutorEntity sib : siblings) {
                if (selfJobIdToSkip != null && Objects.equals(sib.getId(), selfJobIdToSkip)) continue;
                Object boundaryFlag = sib.getConfig() == null ? null : sib.getConfig().get("boundary");
                if (!Boolean.TRUE.equals(boundaryFlag)) continue;
                sib.setLifecycle("cancelled");
                sib.setUpdatedAt(now);
                jobRepo.save(sib);
                cancelledSiblings++;
            }
        }

        // 2. Si interrupting: cancelar TaskInstanceEntity + consumir activity token
        if (interrupting) {
            if (attachedTaskId != null) {
                taskRepo.findById(attachedTaskId).ifPresent(t -> {
                    if (List.of("created", "assigned", "in_progress").contains(t.getLifecycle())) {
                        t.setLifecycle("cancelled");
                        t.setUpdatedAt(now);
                        taskRepo.save(t);
                        audit(instance, "task.cancelled", t.getFlowelementId(), activityToken.getId(), null,
                            Map.of("taskInstanceId", t.getId().toString(), "reason", cancelReason));
                    }
                });
            }
            activityToken.setLifecycle("consumed");
            activityToken.setUpdatedAt(now);
            tokenRepo.save(activityToken);
        }

        Map<String, Object> auditData = new LinkedHashMap<>();
        auditData.put("boundaryCode", boundaryCode);
        auditData.put("attachedToCode", attachedToCode);
        auditData.put("interrupted", interrupting);
        auditData.put("cancelledSiblingBoundaries", cancelledSiblings);
        auditData.put("cancelledTaskInstanceId",
            (interrupting && attachedTaskId != null) ? attachedTaskId.toString() : "(none)");
        if (extraAuditData != null) auditData.putAll(extraAuditData);
        audit(instance, auditEvent, boundaryElementId, activityToken.getId(), null, auditData);
        metricInc("bpm.boundary.fired", instance.getProcessdefId().toString());

        // 3. Crear nuevo token en outgoing del boundary y avanzar
        ProcessDefinition def = loader.load(instance.getProcessversionId(), null, instance.getTenantId());
        List<ProcessDefinition.SequenceFlow> outgoing = def.outgoingFlows(boundaryElementId);
        if (outgoing.isEmpty()) {
            log.warn("boundary_event '{}' has no outgoing flow — instance may stall", boundaryCode);
            return;
        }
        // El boundary token hereda el parent_token_id del activity (mantiene scope
        // de paralelismo si la activity vivía dentro de un SPLIT).
        TokenEntity boundaryToken = newToken(instance, outgoing.get(0).targetId(), activityToken.getParentTokenId());
        tokenRepo.save(boundaryToken);
        ProcessDefinition.FlowElement target = def.findElementById(outgoing.get(0).targetId());
        audit(instance, "token.entered", outgoing.get(0).targetId(), boundaryToken.getId(), null, Map.of(
            "elementCode", target != null ? target.code() : "?",
            "elementType", target != null ? target.type() : "?",
            "fromBoundary", boundaryCode
        ));
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

    private void setVariable(ProcessInstanceEntity instance, String name, Object value) {
        VariableEntity var = varRepo.findByProcessinstanceIdAndVarName(instance.getId(), name)
            .orElseGet(VariableEntity::new);
        OffsetDateTime now = OffsetDateTime.now();
        if (var.getId() == null) {
            var.setId(UUID.randomUUID());
            var.setTenantId(instance.getTenantId());
            var.setProcessinstanceId(instance.getId());
            var.setVarName(name);
            var.setStateId(DEFAULT_STATE_ACTIVE);
            var.setCreatedAt(now);
        }
        var.setVarValue(serializeVarValue(value));
        var.setVarType(inferVarType(value));
        var.setUpdatedAt(now);
        varRepo.save(var);
    }

    /**
     * Serializa value para persistir en bpm_pro_variable_tbl.var_value (text).
     * - Map/List → JSON real con Jackson (NO Java toString que produce {k=v})
     * - Resto    → .toString() (string, number, boolean)
     *
     * Fix Fase 4 Día 4: handlers remotos que reciben Array/Map en variables
     * estaban recibiendo Java toString() (no parseable como JSON). Ahora se
     * persiste JSON real y los handlers pueden Jackson.readValue() directo.
     */
    private String serializeVarValue(Object value) {
        if (value == null) return null;
        if (value instanceof Map || value instanceof List) {
            try {
                return jackson.writeValueAsString(value);
            } catch (Exception e) {
                log.warn("setVariable: failed to JSON-serialize complex var, falling back to toString: {}",
                    e.getMessage());
                return value.toString();
            }
        }
        return value.toString();
    }

    private String inferVarType(Object value) {
        if (value == null) return "string";
        if (value instanceof Boolean) return "boolean";
        if (value instanceof Number) return "number";
        if (value instanceof Map || value instanceof List) return "json";
        return "string";
    }

    private Map<String, Object> currentVariablesAsMap(ProcessInstanceEntity instance) {
        Map<String, Object> out = new LinkedHashMap<>();
        for (VariableEntity v : varRepo.findByProcessinstanceId(instance.getId())) {
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

    private void audit(ProcessInstanceEntity instance, String eventType, UUID flowElementId,
                       UUID tokenId, UUID userId, Map<String, Object> data) {
        AuditLogEntity al = new AuditLogEntity();
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

        // Hook SSE — publica eventos relevantes para token highlighting live.
        // El frontend (ProcessDiagram) filtra por instanceId + actualiza markers.
        publishSseEvent(instance, eventType, data, now);
    }

    /**
     * Whitelist de event types que se publican via SSE. NO publica todos para
     * evitar ruido (audit log tiene ~30 event types, frontend solo necesita 5).
     */
    private void publishSseEvent(ProcessInstanceEntity instance, String eventType,
                                  Map<String, Object> data, OffsetDateTime ts) {
        String sseEvent = switch (eventType) {
            case "token.entered"    -> "bpm.token.entered";
            case "task.created"     -> "bpm.task.created";
            case "task.completed"   -> "bpm.task.completed";
            case "instance.ended"   -> "bpm.instance.completed";
            case "instance.cancelled" -> "bpm.instance.cancelled";
            default -> null;
        };
        if (sseEvent == null) return;
        try {
            Map<String, Object> payload = new java.util.HashMap<>();
            payload.put("instanceId", instance.getId().toString());
            payload.put("processdefId", instance.getProcessdefId() == null
                ? null : instance.getProcessdefId().toString());
            payload.put("ts", ts.toInstant().toEpochMilli());
            // El elementCode/elementType vienen en `data` (lo agrega cada call site).
            // Pasamos solo lo necesario para mantener payload chico.
            if (data != null) {
                if (data.containsKey("elementCode")) payload.put("flowElementCode", data.get("elementCode"));
                if (data.containsKey("elementType")) payload.put("flowElementType", data.get("elementType"));
                if (data.containsKey("taskInstanceId")) payload.put("taskId", data.get("taskInstanceId"));
                if (data.containsKey("reason"))      payload.put("reason", data.get("reason"));
                // Caso #3 (notifs in-app): incluir userIds para que frontend filtre
                // los eventos por currentUserId.
                if (data.containsKey("assignedUserId")) payload.put("assignedUserId", data.get("assignedUserId"));
            }
            // starterUserId siempre disponible desde la instance — útil para
            // notifs de "tu proceso completó/cancelló".
            if (instance.getStartedById() != null) {
                payload.put("starterUserId", instance.getStartedById().toString());
            }
            sseBus.broadcast(sseEvent, payload);
        } catch (Exception e) {
            // SSE no debe romper el flow del motor — log y continúa
            log.warn("SSE publish failed for event {} instance {}: {}",
                sseEvent, instance.getId(), e.getMessage());
        }
    }

    private ProcessInstanceEntity newInstance(ProcessDefinition def, UUID tenantId, UUID userId) {
        ProcessInstanceEntity instance = new ProcessInstanceEntity();
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

    private TokenEntity newToken(ProcessInstanceEntity instance, UUID elementId, UUID parentTokenId) {
        TokenEntity token = new TokenEntity();
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
