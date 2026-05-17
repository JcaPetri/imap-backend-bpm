package com.imap.bpm.infrastructure;

import com.imap.bpm.domain.engine.ProcessDefinition;
import com.imap.bpm.domain.engine.ProcessDefinitionLoader;
import com.imap.bpm.domain.engine.ProcessEngine;
import com.imap.bpm.infrastructure.entity.AuditLog;
import com.imap.bpm.infrastructure.entity.ProcessInstance;
import com.imap.bpm.infrastructure.entity.TaskInstance;
import com.imap.bpm.infrastructure.repository.AuditLogRepository;
import com.imap.bpm.infrastructure.repository.ProcessInstanceRepository;
import com.imap.bpm.infrastructure.repository.TaskInstanceRepository;
import com.imap.bpm.infrastructure.security.UserContext;
import com.imap.bpm.infrastructure.security.UserContextHolder;
import com.imap.bpm.infrastructure.tenant.TenantContextHolder;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

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

    private final ProcessEngine engine;
    private final TaskInstanceRepository taskRepo;
    private final ProcessInstanceRepository instanceRepo;
    private final AuditLogRepository auditRepo;
    private final ProcessDefinitionLoader loader;

    public BpmProcessController(ProcessEngine engine,
                                TaskInstanceRepository taskRepo,
                                ProcessInstanceRepository instanceRepo,
                                AuditLogRepository auditRepo,
                                ProcessDefinitionLoader loader) {
        this.engine = engine;
        this.taskRepo = taskRepo;
        this.instanceRepo = instanceRepo;
        this.auditRepo = auditRepo;
        this.loader = loader;
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

        ProcessInstance instance = engine.startProcess(processVersionId,
            payload == null ? Map.of() : payload,
            bearerToken, tenantId, userId);

        return toInstanceResponse(instance);
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
    public List<Map<String, Object>> listMyTasks(HttpServletRequest req) {
        UUID tenantId = TenantContextHolder.get();
        UserContext user = UserContextHolder.get();
        UUID userId = user != null ? user.userId() : null;
        if (userId == null) return List.of();
        String bearerToken = extractBearer(req);

        List<TaskInstance> tasks = taskRepo.findByAssignedUserIdAndLifecycleInOrderByCreatedAtDesc(
            userId, List.of("created", "reserved"));

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
    public ResponseEntity<Map<String, Object>> getTask(@PathVariable("taskInstanceId") String taskInstanceId,
                                                       HttpServletRequest req) {
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
    public ResponseEntity<Map<String, Object>> getInstance(@PathVariable("instanceId") String instanceId,
                                                           HttpServletRequest req) {
        UUID id = UUID.fromString(instanceId);
        UUID tenantId = TenantContextHolder.get();
        String bearerToken = extractBearer(req);

        ProcessInstance pi = instanceRepo.findById(id).orElse(null);
        if (pi == null) return ResponseEntity.notFound().build();

        ProcessDefinition def = safeLoad(pi.getProcessversionId(), bearerToken, tenantId);
        List<TaskInstance> tasks = taskRepo.findByProcessinstanceId(id);
        List<AuditLog> audits = auditRepo.findByProcessinstanceIdOrderByOccurredAtDesc(id);

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

    // ─────────────────────────────────────────────────────────────────────────

    private ProcessDefinition safeLoad(UUID processVersionId, String bearer, UUID tenantId) {
        try {
            return loader.load(processVersionId, bearer, tenantId);
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
