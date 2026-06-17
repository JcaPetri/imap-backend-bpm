package com.imap.bpm.infrastructure;

import com.imap.bpm.domain.engine.ProcessDefinition;
import com.imap.bpm.domain.engine.ProcessDefinitionLoader;
import com.imap.bpm.domain.engine.ProcessEngine;
import com.imap.bpm.domain.workhub.ScoreService;
import com.imap.bpm.domain.workhub.TaskPriority;
import com.imap.bpm.infrastructure.sse.SseEventBus;
import com.imap.bpm.infrastructure.entity.AuditLog;
import com.imap.bpm.infrastructure.entity.ProcessInstance;
import com.imap.bpm.infrastructure.entity.TaskInstance;
import com.imap.bpm.infrastructure.repository.AuditLogRepository;
import com.imap.bpm.infrastructure.repository.ProcessInstanceRepository;
import com.imap.bpm.infrastructure.repository.TaskInstanceRepository;
import com.imap.bpm.infrastructure.repository.JobExecutorRepository;
import com.imap.bpm.infrastructure.repository.MessageCorrelationRepository;
import com.imap.bpm.infrastructure.repository.TokenRepository;
import com.imap.bpm.infrastructure.repository.VariableRepository;
import com.imap.platform.security.UserContext;
import com.imap.platform.security.UserContextHolder;
import com.imap.platform.tenant.TenantContextHolder;
import com.imap.eav.engine.context.EavTenantSession;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import jakarta.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.*;

/**
 * Endpoints públicos del motor BPM.
 *
 *   POST /v1/bpm/process/{versionId}/start          → arranca instance
 *   POST /v1/bpm/task/{taskInstanceId}/complete     → completa tarea humana
 *   GET  /v1/bpm/me/tasks                           → tareas del usuario actual
 *   GET  /v1/bpm/task/{taskInstanceId}              → 1 task + form metadata
 *   GET  /v1/bpm/instance/{instanceId}              → estado + audit log de instance
 *   POST /v1/bpm/messages/correlate                 → A2 — correlate message
 *   POST /v1/bpm/signals/broadcast                  → A2 — broadcast signal
 *   GET    /v1/bpm/processdef/{id}/instances        → admin: lista instances
 *   GET    /v1/bpm/processdefs/summary              → admin: resumen por processdef
 *   POST   /v1/bpm/instance/{id}/cancel             → admin: soft cancel (C1)
 *   DELETE /v1/bpm/instance/{id}                    → admin: cascade delete
 *   POST   /v1/bpm/task/{id}/raise-error            → dispara boundary error/escalation
 *   GET    /v1/bpm/processversion/{id}/definition   → flowElements + sequenceFlows (para BPMN viewer)
 *
 * Por simplicidad MVP, el endpoint /start recibe el processVersionId directo
 * (no el processdef code). Esto evita una llamada extra a system para resolver
 * el current_version_id. Cuando armemos el frontend "Crear instance", éste va
 * a hacer GET al processdef y obtener la version activa antes de POST.
 *
 * MVP de asignación: mientras no haya AssignmentRule runtime, /me/tasks
 * devuelve TODAS las taskinstance vivas del tenant (RLS las filtra). Iter
 * BPM-5 reemplaza por filtro real assigned_user_id / role / group.
 */
@RestController
@RequestMapping("/v1/bpm")
public class BpmProcessController {

    private static final Logger log = LoggerFactory.getLogger(BpmProcessController.class);

    private final ProcessEngine engine;
    private final TaskInstanceRepository taskRepo;
    private final ProcessInstanceRepository instanceRepo;
    private final AuditLogRepository auditRepo;
    private final ProcessDefinitionLoader loader;
    private final EavTenantSession tenantSession;
    private final TokenRepository tokenRepo;
    private final VariableRepository varRepo;
    private final JobExecutorRepository jobRepo;
    private final MessageCorrelationRepository msgCorrRepo;
    private final ScoreService scoreService;
    private final SseEventBus sseEventBus;

    @PersistenceContext
    private EntityManager em;

    public BpmProcessController(ProcessEngine engine,
                                TaskInstanceRepository taskRepo,
                                ProcessInstanceRepository instanceRepo,
                                AuditLogRepository auditRepo,
                                ProcessDefinitionLoader loader,
                                EavTenantSession tenantSession,
                                TokenRepository tokenRepo,
                                VariableRepository varRepo,
                                JobExecutorRepository jobRepo,
                                MessageCorrelationRepository msgCorrRepo,
                                ScoreService scoreService,
                                SseEventBus sseEventBus) {
        this.engine = engine;
        this.taskRepo = taskRepo;
        this.instanceRepo = instanceRepo;
        this.auditRepo = auditRepo;
        this.loader = loader;
        this.tenantSession = tenantSession;
        this.tokenRepo = tokenRepo;
        this.varRepo = varRepo;
        this.jobRepo = jobRepo;
        this.msgCorrRepo = msgCorrRepo;
        this.scoreService = scoreService;
        this.sseEventBus = sseEventBus;
    }

    @PostMapping("/process/{versionId}/start")
    public Map<String, Object> startProcess(@PathVariable("versionId") String versionId,
                                            @RequestBody(required = false) Map<String, Object> payload,
                                            HttpServletRequest req) {
        UUID processVersionId = UUID.fromString(versionId);
        UUID tenantId = TenantContextHolder.get();
        UserContext user = UserContextHolder.get();
        UUID userId = user != null ? user.userId() : null;
        String bearerToken = extractBearer(req);
        log.info("DIAG startProcess: hdr X-Tenant-Id={} resolved tenant={} userTenants={}",
            req.getHeader("X-Tenant-Id"), tenantId,
            user != null && user.permissionsByTenant() != null ? user.permissionsByTenant().keySet() : null);

        ProcessInstance instance = engine.startProcess(processVersionId,
            payload == null ? Map.of() : payload,
            bearerToken, tenantId, userId);

        return toInstanceResponse(instance);
    }

    /**
     * Fase 3 Día 4 — endpoint de message-start.
     *
     * Body:
     *   { "messageCode": "inventory.purchase_order.arriving",
     *     "variables":   { "productId": "...", "expectedQty": 100, ... } }
     *
     * El motor busca subscriptions activas matching (tenantId del header X-Tenant-Id,
     * messageCode del body) en bpm_pro_message_start_subscription_tbl, y arranca un
     * ProcessInstance por cada match. Permite broadcast.
     *
     * Respuesta:
     *   { "messageCode": "...",
     *     "instancesStarted": 2,
     *     "instances": [ {id, processdefCode, lifecycle, ...}, ... ] }
     *
     * Si no hay subscriptions matching, devuelve instancesStarted=0 y lista vacía.
     * Esto NO es error — permite que el caller (microservicio externo emitiendo
     * events) no se acople a si hay processes suscritos.
     */
    @PostMapping("/messages/start")
    public Map<String, Object> startByMessage(@RequestBody Map<String, Object> body,
                                              HttpServletRequest req) {
        if (body == null || body.get("messageCode") == null) {
            throw new IllegalArgumentException("body.messageCode is required");
        }
        String messageCode = body.get("messageCode").toString();
        @SuppressWarnings("unchecked")
        Map<String, Object> variables = body.get("variables") instanceof Map
            ? (Map<String, Object>) body.get("variables")
            : Map.of();

        UUID tenantId = TenantContextHolder.get();
        UserContext user = UserContextHolder.get();
        UUID userId = user != null ? user.userId() : null;
        String bearerToken = extractBearer(req);

        java.util.List<ProcessInstance> instances = engine.startProcessByMessage(
            messageCode, variables, bearerToken, tenantId, userId);

        java.util.List<Map<String, Object>> instResponses = new java.util.ArrayList<>(instances.size());
        for (ProcessInstance inst : instances) {
            instResponses.add(toInstanceResponse(inst));
        }
        Map<String, Object> out = new java.util.LinkedHashMap<>();
        out.put("messageCode", messageCode);
        out.put("instancesStarted", instances.size());
        out.put("instances", instResponses);
        return out;
    }

    @PostMapping("/task/{taskInstanceId}/complete")
    public Map<String, Object> completeTask(@PathVariable("taskInstanceId") String taskInstanceId,
                                            @RequestBody(required = false) Map<String, Object> outputData,
                                            HttpServletRequest req) {
        UUID taskId = UUID.fromString(taskInstanceId);
        UserContext user = UserContextHolder.get();
        UUID userId = user != null ? user.userId() : null;
        String bearerToken = extractBearer(req);

        TaskInstance task = engine.completeTask(taskId,
            outputData == null ? Map.of() : outputData,
            bearerToken, userId);

        return toTaskResponse(task);
    }

    /**
     * WorkHub — claim atómico de una tarea de cola (candidate group). CAS: solo
     * tiene éxito si la tarea sigue libre (assignee NULL + lifecycle 'created').
     * 409 si otro la tomó primero. Emite SSE `bpm.task.claimed` para invalidar la
     * bandeja de los demás. Ver docs/architecture/workhub-northstar.md §3.
     */
    @PostMapping("/task/{taskInstanceId}/claim")
    @Transactional
    public ResponseEntity<?> claimTask(@PathVariable("taskInstanceId") String taskInstanceId) {
        tenantSession.applyToCurrentTransaction();
        UUID taskId = UUID.fromString(taskInstanceId);
        UserContext user = UserContextHolder.get();
        UUID userId = user != null ? user.userId() : null;
        if (userId == null) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(Map.of(
                "error", "authentication_required",
                "message", "Se requiere usuario autenticado para tomar una tarea."));
        }

        int updated = taskRepo.claimIfUnassigned(taskId, userId, OffsetDateTime.now(ZoneOffset.UTC));
        if (updated == 0) {
            return ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of(
                "error", "already_claimed",
                "message", "La tarea ya fue tomada por otro usuario o no está disponible.",
                "taskId", taskInstanceId));
        }

        TaskInstance task = taskRepo.findById(taskId).orElse(null);
        UUID instanceId = task != null ? task.getProcessinstanceId() : null;
        sseEventBus.broadcast("bpm.task.claimed", Map.of(
            "taskId", taskInstanceId,
            "assignee", userId.toString(),
            "processInstanceId", instanceId != null ? instanceId.toString() : ""));

        return ResponseEntity.ok(task != null ? toTaskResponse(task)
            : Map.of("taskId", taskInstanceId, "claimedBy", userId.toString()));
    }

    /** WorkHub — filtro de vistas por fecha de vencimiento (due_at). */
    private static boolean matchesDateView(String view, OffsetDateTime dueAt, OffsetDateTime now) {
        switch (view) {
            case "overdue":  return dueAt != null && dueAt.isBefore(now);
            case "today":    return dueAt != null && !dueAt.isBefore(now)
                                    && dueAt.isBefore(now.plusHours(24));   // ~ hoy / próximas 24h
            case "upcoming": return dueAt == null || !dueAt.isBefore(now.plusHours(24));
            default:         return true;   // vista desconocida → no filtra
        }
    }

    /**
     * WorkHub — procesos que el usuario puede INICIAR (zona "Iniciar" de la
     * bandeja). Lista el catálogo de processdefs (system) y filtra por el permiso
     * requerido contra los permisos del JWT para el tenant actual. El permiso lo
     * declara el processdef como metadata EAV (`startPermission`); si aún no está,
     * se usa la convención `bpm.start.<code>`. El grant vive en IAM (JWT).
     * Ver docs/architecture/workhub-northstar.md §6.
     */
    @GetMapping("/me/startable")
    @Transactional(readOnly = true)
    public List<Map<String, Object>> startableProcesses() {
        tenantSession.applyToCurrentTransaction();
        UUID tenantId = TenantContextHolder.get();
        UserContext user = UserContextHolder.get();
        if (user == null) return List.of();   // anonymous no puede iniciar nada

        Map<String, List<String>> permsByTenant = user.permissionsByTenant();
        List<String> perms = permsByTenant == null ? List.of()
            : permsByTenant.getOrDefault(tenantId == null ? "" : tenantId.toString(), List.of());

        // Degradación con gracia: si el catálogo de system no se puede leer
        // (ej. el service token no está autorizado para /v1/admin/bpm/processdef →
        // 403, fix de auth pendiente en el sprint de start_permission/permisos),
        // devolvemos lista vacía en vez de 500 — la bandeja no debe romperse.
        List<Map<String, Object>> defs;
        try {
            defs = loader.listProcessdefs(tenantId);
        } catch (Exception e) {
            log.warn("startable: no se pudo listar processdefs desde system ({}). Devuelvo []. "
                + "Pendiente: autorizar el catálogo startable sin system.admin.", e.toString());
            return List.of();
        }

        List<Map<String, Object>> out = new ArrayList<>();
        for (Map<String, Object> d : defs) {
            if (!"active".equals(String.valueOf(d.get("lifecycle")))) continue;   // solo activos
            String code = d.get("code") == null ? null : d.get("code").toString();
            if (code == null) continue;
            Object explicitPerm = d.get("startPermission");   // authoritative si system lo provee
            String requiredPerm = explicitPerm != null ? explicitPerm.toString() : "bpm.start." + code;
            if (!perms.contains(requiredPerm)) continue;       // sin permiso → no startable

            Map<String, Object> row = new LinkedHashMap<>();
            row.put("processdefId", d.get("processdefId"));
            row.put("processKey", code);
            row.put("label", d.get("name"));
            row.put("currentVersionId", d.get("currentVersionId"));
            row.put("permission", requiredPerm);
            out.add(row);
        }
        return out;
    }

    /**
     * Lista tareas humanas vivas asignadas al usuario actual (A3 —
     * AssignmentRule MVP). El backend setea `assignedUserId = startedById`
     * cuando crea la task (ver ProcessEngine.handleUserTask). Iter 5+
     * reemplaza por evaluación de bpm_hum_assignmentrule.
     *
     * Si el JWT no llegó (anonymous), devolvemos lista vacía (no se filtra
     * por tenant porque cualquier usuario del tenant podría ver tareas de
     * otros — comportamiento intencional pre-A3 que ahora se cierra).
     */
    @GetMapping("/me/tasks")
    @Transactional(readOnly = true)
    public List<Map<String, Object>> listMyTasks(
            @RequestParam(value = "view", required = false) String view,
            HttpServletRequest req) {
        tenantSession.applyToCurrentTransaction();
        UUID tenantId = TenantContextHolder.get();
        UserContext user = UserContextHolder.get();
        UUID userId = user != null ? user.userId() : null;
        if (userId == null) return List.of();
        String bearerToken = extractBearer(req);

        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);

        // Asignadas a mí (assignee = yo)
        List<TaskInstance> tasks = new ArrayList<>(
            taskRepo.findByAssignedUserIdAndLifecycleInOrderByCreatedAtDesc(
                userId, List.of("created", "reserved")));

        // WorkHub 3b — tareas DE COLA: sin assignee, cuyo candidate group (permiso
        // bpm.queue.X) está entre mis permisos del JWT para este tenant. Ver §6.2.
        List<String> myQueuePerms = (user.permissionsByTenant() == null) ? List.of()
            : user.permissionsByTenant()
                  .getOrDefault(tenantId == null ? "" : tenantId.toString(), List.of())
                  .stream().filter(p -> p != null && p.startsWith("bpm.queue.")).toList();
        if (!myQueuePerms.isEmpty()) {
            tasks.addAll(taskRepo
                .findByTenantIdAndAssignedUserIdIsNullAndAssignedRoleInAndLifecycleInOrderByCreatedAtDesc(
                    tenantId, myQueuePerms, List.of("created")));
        }

        // WorkHub — vistas de la bandeja por fecha (today/overdue/upcoming).
        // Sin view (o "mine"/"all") devuelve todas. Ver workhub-northstar §1.1.
        if (view != null && !view.isBlank() && !view.equals("mine") && !view.equals("all")) {
            tasks = new ArrayList<>(
                tasks.stream().filter(t -> matchesDateView(view, t.getDueAt(), now)).toList());
        }

        // Cache local de definitions (mismo processversion → 1 load por request)
        Map<UUID, ProcessDefinition> defCache = new HashMap<>();
        List<Map<String, Object>> out = new ArrayList<>(tasks.size());
        for (TaskInstance t : tasks) {
            ProcessInstance pi = instanceRepo.findById(t.getProcessinstanceId()).orElse(null);
            if (pi == null) continue;

            ProcessDefinition def = defCache.computeIfAbsent(pi.getProcessversionId(),
                pvId -> safeLoad(pvId, bearerToken, tenantId));

            Map<String, Object> row = new LinkedHashMap<>();
            row.put("taskId", t.getId().toString());
            row.put("lifecycle", t.getLifecycle());
            row.put("priority", t.getPriority());
            row.put("createdAt", t.getCreatedAt());
            row.put("dueAt", t.getDueAt());
            row.put("processInstanceId", pi.getId().toString());

            if (def != null) {
                row.put("processdefCode", def.processdefCode());
                row.put("processdefName", def.processdefName());
                ProcessDefinition.FlowElement fe = def.findElementById(t.getFlowelementId());
                if (fe != null) {
                    row.put("flowElementCode", fe.code());
                    row.put("flowElementName", fe.name());
                }
            }

            // WorkHub — prioridad% + semáforo (clasificación G/U/T del tenant + SLA).
            TaskPriority prio = scoreService.compute(
                tenantId, pi.getProcessdefId(), t.getFlowelementId(), t.getDueAt());
            row.put("priorityPct", (int) Math.round(prio.prioridadPct()));
            row.put("semaphore", prio.color().name());     // GREEN | YELLOW | RED
            row.put("classified", prio.classified());

            out.add(row);
        }
        return out;
    }

    /**
     * Devuelve UN task con la metadata necesaria para renderizarlo: el
     * entityCode del taskform asociado, el flowElement code/name, el process
     * en el que vive. El frontend usa esto para llamar a system y obtener
     * la entity structure que renderiza el form generator.
     */
    @GetMapping("/task/{taskInstanceId}")
    @Transactional(readOnly = true)
    public ResponseEntity<Map<String, Object>> getTask(@PathVariable("taskInstanceId") String taskInstanceId,
                                                       HttpServletRequest req) {
        tenantSession.applyToCurrentTransaction();
        UUID taskId = UUID.fromString(taskInstanceId);
        UUID tenantId = TenantContextHolder.get();
        String bearerToken = extractBearer(req);

        TaskInstance t = taskRepo.findById(taskId).orElse(null);
        if (t == null) return ResponseEntity.notFound().build();

        ProcessInstance pi = instanceRepo.findById(t.getProcessinstanceId()).orElse(null);
        if (pi == null) return ResponseEntity.notFound().build();

        ProcessDefinition def = safeLoad(pi.getProcessversionId(), bearerToken, tenantId);

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("taskId", t.getId().toString());
        out.put("lifecycle", t.getLifecycle());
        out.put("priority", t.getPriority());
        out.put("createdAt", t.getCreatedAt());
        out.put("dueAt", t.getDueAt());
        out.put("inputData", t.getInputData());
        out.put("outputData", t.getOutputData());
        out.put("processInstanceId", pi.getId().toString());
        out.put("processInstanceLifecycle", pi.getLifecycle());

        if (def != null) {
            out.put("processdefCode", def.processdefCode());
            out.put("processdefName", def.processdefName());
            ProcessDefinition.FlowElement fe = def.findElementById(t.getFlowelementId());
            if (fe != null) {
                out.put("flowElementCode", fe.code());
                out.put("flowElementName", fe.name());
                out.put("flowElementType", fe.type());
            }
            ProcessDefinition.TaskForm tf = def.findTaskFormByFlowElement(t.getFlowelementId());
            if (tf != null) {
                out.put("entityDefCode", tf.entityDefCode());
                out.put("entityDefId", tf.entityDefId() != null ? tf.entityDefId().toString() : null);
                out.put("formMode", tf.mode());
            }
        }
        return ResponseEntity.ok(out);
    }

    /**
     * Devuelve estado consolidado de una instance: header + tasks + audit log.
     * Útil para que la UI muestre "contexto del proceso" cuando el user abre
     * una tarea — qué otras tareas hay, qué eventos pasaron, en qué nodo
     * está parado el motor.
     */
    @GetMapping("/instance/{instanceId}")
    @Transactional(readOnly = true)
    public ResponseEntity<Map<String, Object>> getInstance(@PathVariable("instanceId") String instanceId,
                                                           HttpServletRequest req) {
        tenantSession.applyToCurrentTransaction();
        UUID id = UUID.fromString(instanceId);
        UUID tenantId = TenantContextHolder.get();
        String bearerToken = extractBearer(req);

        ProcessInstance pi = instanceRepo.findById(id).orElse(null);
        if (pi == null) return ResponseEntity.notFound().build();

        ProcessDefinition def = safeLoad(pi.getProcessversionId(), bearerToken, tenantId);
        List<TaskInstance> tasks = taskRepo.findByProcessinstanceId(id);
        List<AuditLog> audits = auditRepo.findByProcessinstanceIdOrderByOccurredAtDesc(id);
        List<com.imap.bpm.infrastructure.entity.Variable> vars = varRepo.findByProcessinstanceId(id);

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("id", pi.getId().toString());
        out.put("processdefId", pi.getProcessdefId().toString());
        out.put("processversionId", pi.getProcessversionId().toString());
        out.put("lifecycle", pi.getLifecycle());
        out.put("startedAt", pi.getStartedAt());
        out.put("endedAt", pi.getEndedAt());
        out.put("startedById", pi.getStartedById() != null ? pi.getStartedById().toString() : null);
        out.put("correlationKey", pi.getCorrelationKey());

        if (def != null) {
            out.put("processdefCode", def.processdefCode());
            out.put("processdefName", def.processdefName());
        }

        // Fase 4 Día 0 (D4): variables del processinstance mergeadas por los handlers.
        // Útil para el frontend que necesita el contexto del flow (ej. ruleApplied,
        // targetLocationCode, moveId) sin pedir endpoint adicional.
        Map<String, Object> varsMap = new LinkedHashMap<>();
        for (com.imap.bpm.infrastructure.entity.Variable v : vars) {
            varsMap.put(v.getVarName(), v.getVarValue());
        }
        out.put("variables", varsMap);

        // Tasks de la instance (orden cronológico ascendente)
        tasks.sort(Comparator.comparing(TaskInstance::getCreatedAt));
        List<Map<String, Object>> tasksOut = new ArrayList<>(tasks.size());
        for (TaskInstance t : tasks) {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("taskId", t.getId().toString());
            row.put("lifecycle", t.getLifecycle());
            row.put("createdAt", t.getCreatedAt());
            row.put("completedAt", t.getCompletedAt());
            if (def != null) {
                ProcessDefinition.FlowElement fe = def.findElementById(t.getFlowelementId());
                if (fe != null) {
                    row.put("flowElementCode", fe.code());
                    row.put("flowElementName", fe.name());
                }
            }
            tasksOut.add(row);
        }
        out.put("tasks", tasksOut);

        // Audit log (más reciente primero, como viene del repo)
        List<Map<String, Object>> auditOut = new ArrayList<>(audits.size());
        for (AuditLog a : audits) {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("eventType", a.getEventType());
            row.put("occurredAt", a.getOccurredAt());
            row.put("userId", a.getUserId() != null ? a.getUserId().toString() : null);
            row.put("data", a.getData());
            if (def != null && a.getFlowelementId() != null) {
                ProcessDefinition.FlowElement fe = def.findElementById(a.getFlowelementId());
                if (fe != null) {
                    row.put("flowElementCode", fe.code());
                    row.put("flowElementName", fe.name());
                }
            }
            auditOut.add(row);
        }
        out.put("auditLog", auditOut);

        return ResponseEntity.ok(out);
    }

    /**
     * Correlate un message entrante con un token waiting (A2 — message event).
     * Body: { "messageCode": "...", "correlationKey": "...", "payload": {...} }
     *
     * Devuelve cuántos tokens fueron reactivados (0 o 1 — semántica point-to-point).
     */
    @PostMapping("/messages/correlate")
    public Map<String, Object> correlateMessage(@RequestBody Map<String, Object> body) {
        String messageCode    = body == null ? null : (String) body.get("messageCode");
        String correlationKey = body == null ? null : (String) body.get("correlationKey");
        @SuppressWarnings("unchecked")
        Map<String, Object> payload = body == null ? null : (Map<String, Object>) body.get("payload");

        int reactivated = engine.correlateMessage(messageCode,
            correlationKey == null ? "*" : correlationKey,
            payload);

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("messageCode", messageCode);
        out.put("correlationKey", correlationKey);
        out.put("reactivatedTokens", reactivated);
        return out;
    }

    /**
     * Broadcast un signal a TODOS los tokens waiting por ese signalCode
     * (A2 — signal event). Body: { "signalCode": "...", "payload": {...} }
     */
    @PostMapping("/signals/broadcast")
    public Map<String, Object> broadcastSignal(@RequestBody Map<String, Object> body) {
        String signalCode = body == null ? null : (String) body.get("signalCode");
        @SuppressWarnings("unchecked")
        Map<String, Object> payload = body == null ? null : (Map<String, Object>) body.get("payload");

        int reactivated = engine.broadcastSignal(signalCode, payload);

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("signalCode", signalCode);
        out.put("reactivatedTokens", reactivated);
        return out;
    }

    // ─── ProcessVersion definition (para BPMN visual viewer) ─────────────────

    /**
     * Devuelve la definition del processversion (flowElements + sequenceFlows
     * + taskForms) en shape simplificado, listo para que el frontend genere
     * BPMN XML y lo renderice con bpmn-js.
     *
     * Reusa el cache de `ProcessDefinitionLoader` — no llama directamente al
     * system si la def ya está cacheada. Permite a cualquier user del tenant
     * (no solo system.admin) ver el diagrama de procesos que están
     * ejecutándose en su tenant.
     *
     * 404 si la def no se puede cargar.
     */
    @GetMapping("/processversion/{processversionId}/definition")
    public ResponseEntity<Map<String, Object>> getProcessversionDefinition(
            @PathVariable("processversionId") String processversionIdStr,
            HttpServletRequest req) {
        UUID processVersionId = UUID.fromString(processversionIdStr);
        // ProcessVersions viven SIEMPRE en SYSTEM tenant (catálogo cross-tenant).
        // Si pasamos el user's tenant, el loader hace s2s con X-Tenant-Id=userTenant
        // y SYSTEM TenantContextFilter rechaza al BPM service user (no es member
        // de tenants operativos). Bug observado en cache MISS de v2 fresca.
        String bearerToken = extractBearer(req);

        ProcessDefinition def;
        try {
            def = loader.load(processVersionId, bearerToken, TenantContextHolder.SYSTEM_TENANT_ID);
        } catch (Exception e) {
            return ResponseEntity.notFound().build();
        }
        if (def == null) return ResponseEntity.notFound().build();

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("processversionId", processVersionId.toString());
        out.put("processdefId", def.processdefId() != null ? def.processdefId().toString() : null);
        out.put("processdefCode", def.processdefCode());
        out.put("processdefName", def.processdefName());
        out.put("version", def.version());

        List<Map<String, Object>> fes = new ArrayList<>();
        for (ProcessDefinition.FlowElement fe : def.flowElements()) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("id", fe.id().toString());
            m.put("code", fe.code());
            m.put("type", fe.type());
            m.put("name", fe.name());
            m.put("config", fe.config());
            m.put("sortOrder", fe.sortOrder());
            fes.add(m);
        }
        out.put("flowElements", fes);

        List<Map<String, Object>> sfs = new ArrayList<>();
        for (ProcessDefinition.SequenceFlow sf : def.sequenceFlows()) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("id", sf.id().toString());
            m.put("sourceId", sf.sourceId().toString());
            m.put("targetId", sf.targetId().toString());
            m.put("sourceCode", sf.sourceCode());
            m.put("targetCode", sf.targetCode());
            m.put("conditionExpr", sf.conditionExpr());
            m.put("sortOrder", sf.sortOrder());
            sfs.add(m);
        }
        out.put("sequenceFlows", sfs);

        return ResponseEntity.ok(out);
    }

    // ─── Boundary error/escalation manual trigger (advanced) ─────────────────

    /**
     * Dispara un error sobre una task activa, buscando boundary_event de tipo
     * error/escalation adjuntos que matcheen el errorCode. Si match → cancela
     * la activity (si interrupting=true) + emite token en outgoing del boundary.
     *
     * Body: {"errorCode": "...", "payload": {...?}}.
     *
     * Devuelve {taskId, activityCode, errorCode, boundariesFired:N}.
     * 404 si task no existe. 409 si task no está activa o no hay boundary matching.
     */
    @PostMapping("/task/{taskInstanceId}/raise-error")
    public ResponseEntity<Map<String, Object>> raiseTaskError(
            @PathVariable("taskInstanceId") String taskInstanceIdStr,
            @RequestBody(required = false) Map<String, Object> body) {
        UUID taskId = UUID.fromString(taskInstanceIdStr);
        String errorCode = body == null ? null : (String) body.get("errorCode");
        @SuppressWarnings("unchecked")
        Map<String, Object> payload = body == null ? null : (Map<String, Object>) body.get("payload");
        UserContext user = UserContextHolder.get();
        UUID userId = user != null ? user.userId() : null;

        try {
            Map<String, Object> result = engine.raiseTaskError(taskId, errorCode, payload, userId);
            return ResponseEntity.ok(result);
        } catch (IllegalArgumentException e) {
            return ResponseEntity.notFound().build();
        } catch (IllegalStateException e) {
            Map<String, Object> err = new LinkedHashMap<>();
            err.put("error", "raise_error_failed");
            err.put("message", e.getMessage());
            return ResponseEntity.status(409).body(err);
        }
    }

    // ─── Admin: cancel instance (C1 — soft) ──────────────────────────────────

    /**
     * Cancel soft de una instance activa (C1): marca lifecycle='cancelled',
     * mata tokens/tasks/jobs/correlations vivas, preserva audit log.
     * Body opcional: {"reason": "..."}.
     *
     * Devuelve {id, lifecycle:"cancelled", reason, counts:{tokensCancelled, ...}}.
     *
     * 404 si instance no existe. 409 si ya no está en lifecycle=active.
     */
    @PostMapping("/instance/{instanceId}/cancel")
    public ResponseEntity<Map<String, Object>> cancelInstance(
            @PathVariable("instanceId") String instanceIdStr,
            @RequestBody(required = false) Map<String, Object> body) {
        UUID instanceId = UUID.fromString(instanceIdStr);
        String reason = body == null ? null : (String) body.get("reason");
        UserContext user = UserContextHolder.get();
        UUID userId = user != null ? user.userId() : null;

        try {
            Map<String, Object> counts = engine.cancelInstance(instanceId, reason, userId);
            Map<String, Object> out = new LinkedHashMap<>();
            out.put("id", instanceIdStr);
            out.put("lifecycle", "cancelled");
            out.put("reason", reason);
            out.put("counts", counts);
            return ResponseEntity.ok(out);
        } catch (IllegalArgumentException e) {
            return ResponseEntity.notFound().build();
        } catch (IllegalStateException e) {
            Map<String, Object> err = new LinkedHashMap<>();
            err.put("error", "instance_not_active");
            err.put("message", e.getMessage());
            return ResponseEntity.status(409).body(err);
        }
    }

    /**
     * Resumen de processdefs activos en el tenant: cada processdef que tenga
     * ≥1 instance + counts por lifecycle. Usa SQL nativo (group by) para
     * eficiencia — vs cargar todas las instances en memoria.
     *
     * Útil para la ProcessAdminPage (C2): muestra cards con stats.
     */
    @GetMapping("/processdefs/summary")
    @Transactional(readOnly = true)
    public List<Map<String, Object>> processdefsSummary() {
        tenantSession.applyToCurrentTransaction();
        // Group by processdef_id + lifecycle. El processdef_code no vive
        // acá (vive en system EAV), el frontend lo resuelve via system si
        // necesita, o lo deja con el UUID.
        List<Object[]> rows = em.createNativeQuery("""
            SELECT CAST(processdef_id AS text) AS processdef_id,
                   lifecycle,
                   COUNT(*) AS total
              FROM bpm.bpm_pro_processinstance_tbl
             GROUP BY processdef_id, lifecycle
             ORDER BY processdef_id, lifecycle
            """).getResultList();

        Map<String, Map<String, Object>> byDef = new LinkedHashMap<>();
        for (Object[] r : rows) {
            String defId = (String) r[0];
            String lifecycle = (String) r[1];
            long count = ((Number) r[2]).longValue();
            Map<String, Object> entry = byDef.computeIfAbsent(defId, k -> {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("processdefId", k);
                m.put("totalInstances", 0L);
                m.put("byLifecycle", new LinkedHashMap<String, Long>());
                return m;
            });
            @SuppressWarnings("unchecked")
            Map<String, Long> lifecycleMap = (Map<String, Long>) entry.get("byLifecycle");
            lifecycleMap.put(lifecycle, count);
            entry.put("totalInstances", (Long) entry.get("totalInstances") + count);
        }
        return new ArrayList<>(byDef.values());
    }

    // ─── Admin: list + delete instance (cascade) ─────────────────────────────

    /**
     * Lista instances de un processdef (uso admin / cleanup script).
     * Filtra por lifecycle opcional. Sin filtro devuelve todas.
     *
     * Devuelve solo metadata mínima (id, lifecycle, startedAt, endedAt) —
     * para detalle usar GET /instance/{id}.
     */
    @GetMapping("/processdef/{processdefId}/instances")
    @Transactional(readOnly = true)
    public List<Map<String, Object>> listInstancesByProcessdef(
            @PathVariable("processdefId") String processdefIdStr,
            @RequestParam(value = "lifecycle", required = false) String lifecycleFilter) {
        tenantSession.applyToCurrentTransaction();
        UUID processdefId = UUID.fromString(processdefIdStr);
        List<ProcessInstance> rows;
        if (lifecycleFilter == null || lifecycleFilter.isBlank()) {
            rows = instanceRepo.findByProcessdefIdOrderByStartedAtDesc(processdefId);
        } else {
            rows = instanceRepo.findByProcessdefIdAndLifecycle(processdefId, lifecycleFilter);
        }
        List<Map<String, Object>> out = new ArrayList<>(rows.size());
        for (ProcessInstance i : rows) {
            Map<String, Object> r = new LinkedHashMap<>();
            r.put("id", i.getId().toString());
            r.put("lifecycle", i.getLifecycle());
            r.put("startedAt", i.getStartedAt());
            r.put("endedAt", i.getEndedAt());
            r.put("parentInstanceId", i.getParentInstanceId() != null
                ? i.getParentInstanceId().toString() : null);
            // Hito 2 multi-version visibility: incluir processversionId
            // para que frontend muestre la version que arrancó cada instance.
            r.put("processversionId", i.getProcessversionId() != null
                ? i.getProcessversionId().toString() : null);
            out.add(r);
        }
        return out;
    }

    /**
     * Cascade DELETE de una processinstance + todas sus dependencias:
     *   variables + tasks + tokens + audit + correlations + jobs + instance.
     *
     * Safety guard: por default solo permite borrar instances en lifecycle
     * terminal (completed/cancelled/failed). Para borrar instances activas
     * usar ?force=true (responsabilidad del caller — usa solo si la instance
     * está atascada huérfana).
     *
     * Devuelve counters de qué borró.
     */
    @DeleteMapping("/instance/{instanceId}")
    @Transactional
    public ResponseEntity<Map<String, Object>> deleteInstance(
            @PathVariable("instanceId") String instanceIdStr,
            @RequestParam(value = "force", defaultValue = "false") boolean force) {
        tenantSession.applyToCurrentTransaction();
        UUID instanceId = UUID.fromString(instanceIdStr);
        ProcessInstance instance = instanceRepo.findById(instanceId).orElse(null);
        if (instance == null) return ResponseEntity.notFound().build();

        // Safety: rechazar si activa salvo ?force=true
        boolean terminal = List.of("completed", "cancelled", "failed")
            .contains(instance.getLifecycle());
        if (!terminal && !force) {
            Map<String, Object> err = new LinkedHashMap<>();
            err.put("error", "instance_active");
            err.put("message", "Instance lifecycle is '" + instance.getLifecycle()
                + "'. Use ?force=true to delete active instances.");
            err.put("lifecycle", instance.getLifecycle());
            return ResponseEntity.status(409).body(err);
        }

        // Cascade delete (orden FK: variables/tasks/tokens/audit/jobs/correlations primero, instance al final)
        long vars  = varRepo.deleteByProcessinstanceId(instanceId);
        long tasks = taskRepo.deleteByProcessinstanceId(instanceId);
        long tokens = tokenRepo.deleteByProcessinstanceId(instanceId);
        long audits = auditRepo.deleteByProcessinstanceId(instanceId);
        long jobs   = jobRepo.deleteByProcessinstanceId(instanceId);
        long corrs  = msgCorrRepo.deleteByProcessinstanceId(instanceId);
        instanceRepo.delete(instance);

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("id", instanceId.toString());
        out.put("deleted", true);
        out.put("forcedActive", !terminal && force);
        out.put("counts", Map.of(
            "variables", vars,
            "tasks", tasks,
            "tokens", tokens,
            "auditLogs", audits,
            "jobs", jobs,
            "messageCorrelations", corrs
        ));
        return ResponseEntity.ok(out);
    }

    // ─────────────────────────────────────────────────────────────────────────

    private ProcessDefinition safeLoad(UUID processVersionId, String bearer, UUID tenantId) {
        try {
            // ProcessVersions viven en SYSTEM tenant (catalog cross-tenant), no
            // en el tenant del user/instance. Pasar tenantId del caller hace que
            // el loader s2s con X-Tenant-Id=callerTenant y SYSTEM TenantContextFilter
            // rechaza al BPM service user (no es member de tenants operativos).
            // El `tenantId` param queda en la signature por compat — ignorado en favor
            // de SYSTEM_TENANT_ID.
            return loader.load(processVersionId, bearer, TenantContextHolder.SYSTEM_TENANT_ID);
        } catch (Exception e) {
            // Si system no responde, igual devolvemos la metadata local sin defs
            return null;
        }
    }

    private String extractBearer(HttpServletRequest req) {
        String h = req.getHeader(HttpHeaders.AUTHORIZATION);
        if (h == null || !h.startsWith("Bearer ")) return null;
        return h.substring(7);
    }

    private Map<String, Object> toInstanceResponse(ProcessInstance i) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("id", i.getId().toString());
        out.put("processdefId", i.getProcessdefId().toString());
        out.put("processversionId", i.getProcessversionId().toString());
        out.put("lifecycle", i.getLifecycle());
        out.put("startedAt", i.getStartedAt());
        out.put("endedAt", i.getEndedAt());
        return out;
    }

    private Map<String, Object> toTaskResponse(TaskInstance t) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("id", t.getId().toString());
        out.put("processinstanceId", t.getProcessinstanceId().toString());
        out.put("flowelementId", t.getFlowelementId().toString());
        out.put("lifecycle", t.getLifecycle());
        out.put("completedAt", t.getCompletedAt());
        out.put("outputData", t.getOutputData());
        return out;
    }
}
