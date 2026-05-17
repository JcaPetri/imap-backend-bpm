package com.imap.bpm.domain.engine;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.imap.bpm.infrastructure.entity.*;
import com.imap.bpm.infrastructure.repository.*;
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
 *
 * NO soportado en MVP:
 *   - parallel_gateway  (split + join)
 *   - intermediate_event (timer/message/signal)
 *   - sub_process       (call activity)
 *   - boundary_event    (timer/error/escalation)
 *   - compensation
 *
 * Se agregan en iters siguientes. La estructura de Token + advanceToken ya está
 * preparada para split/join (parent_token_id existe en la entity).
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
    private final ObjectMapper jackson;
    private final JexlEngine jexl;

    public ProcessEngine(ProcessDefinitionLoader loader,
                         ProcessInstanceRepository instanceRepo,
                         TokenRepository tokenRepo,
                         TaskInstanceRepository taskRepo,
                         VariableRepository varRepo,
                         AuditLogRepository auditRepo,
                         ObjectMapper jackson) {
        this.loader = loader;
        this.instanceRepo = instanceRepo;
        this.tokenRepo = tokenRepo;
        this.taskRepo = taskRepo;
        this.varRepo = varRepo;
        this.auditRepo = auditRepo;
        this.jackson = jackson;
        this.jexl = new JexlBuilder().silent(true).strict(false).create();
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
        ProcessDefinition def = loader.load(processVersionId, bearerToken, tenantId);
        log.info("Starting process {} (v{}) for user {}", def.processdefCode(), def.version(), userId);

        // 1. Find start_event
        ProcessDefinition.FlowElement start = def.flowElements().stream()
            .filter(fe -> "start_event".equals(fe.type()))
            .findFirst()
            .orElseThrow(() -> new IllegalStateException(
                "No start_event in processVersion " + processVersionId));

        // 2. Crear ProcessInstance
        ProcessInstance instance = newInstance(def, tenantId, userId);
        instanceRepo.save(instance);
        audit(instance, "instance.started", null, null, userId, Map.of(
            "processCode", def.processdefCode(),
            "version", def.version()
        ));

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

        // Avanza el token asociado
        if (task.getTokenId() != null) {
            Token token = tokenRepo.findById(task.getTokenId()).orElse(null);
            if (token != null) {
                ProcessDefinition def = loader.load(instance.getProcessversionId(), bearerToken, instance.getTenantId());
                advanceToken(instance, token, def, userId);
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
                Map.of("result", endEvent.config().getOrDefault("result", "default"),
                       "endEventCode", endEvent.code()));
            log.info("ProcessInstance {} completed via {}", instance.getId(), endEvent.code());
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
        task.setStateId(DEFAULT_STATE_ACTIVE);
        task.setCreatedAt(now);
        task.setUpdatedAt(now);
        taskRepo.save(task);

        ProcessDefinition.TaskForm form = def.findTaskFormByFlowElement(userTask.id());
        audit(instance, "task.created", userTask.id(), token.getId(), userId, Map.of(
            "taskInstanceId", task.getId().toString(),
            "elementCode", userTask.code(),
            "formEntityDefCode", form != null ? form.entityDefCode() : "(none)"
        ));
        log.info("UserTask '{}' created → task {} waiting (form={})",
            userTask.code(), task.getId(), form != null ? form.entityDefCode() : "none");
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
        moveTokenToElement(instance, token, chosen.targetId(), def, userId);
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
