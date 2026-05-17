package com.imap.bpm.infrastructure;

import com.imap.bpm.domain.engine.ProcessEngine;
import com.imap.bpm.infrastructure.entity.ProcessInstance;
import com.imap.bpm.infrastructure.entity.TaskInstance;
import com.imap.bpm.infrastructure.security.UserContext;
import com.imap.bpm.infrastructure.security.UserContextHolder;
import com.imap.bpm.infrastructure.tenant.TenantContextHolder;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.HttpHeaders;
import org.springframework.web.bind.annotation.*;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Endpoints públicos del motor BPM.
 *
 *   POST /v1/bpm/process/{versionId}/start          → arranca instance
 *   POST /v1/bpm/task/{taskInstanceId}/complete     → completa tarea humana
 *   GET  /v1/bpm/instance/{instanceId}              → estado de instance (TBD)
 *   GET  /v1/bpm/me/tasks                           → tareas asignadas al user (TBD)
 *
 * Por simplicidad MVP, el endpoint /start recibe el processVersionId directo
 * (no el processdef code). Esto evita una llamada extra a system para resolver
 * el current_version_id. Cuando armemos el frontend "Crear instance", éste va
 * a hacer GET al processdef y obtener la version activa antes de POST.
 */
@RestController
@RequestMapping("/v1/bpm")
public class BpmProcessController {

    private final ProcessEngine engine;

    public BpmProcessController(ProcessEngine engine) {
        this.engine = engine;
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

    // ─────────────────────────────────────────────────────────────────────────

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
