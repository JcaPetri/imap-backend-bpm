package com.imap.bpm.domain.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * TaskAssignmentService — resolveAssignee per user_task creation.
 *
 * Tech-debt sprint Día 2 (P1) fix: cuando processdef arranca via msg-emit desde
 * un microservicio (BpmMessageEmitter usa service token), instance.startedById
 * es un sintético service-account UUID. Resultado: las user_tasks creadas en
 * ese flow quedan asignadas al "service user", nadie las ve en su inbox.
 *
 * Fix V1 — hardcoded fallback:
 *   - Si starterUserId pertenece a un service account conocido → usar fallback
 *     admin (env imap.bpm.fallback-assignee-user-id).
 *   - Si starterUserId es un human user real → mantener comportamiento actual.
 *
 * Futuro (Iter 5 AssignmentRule real):
 *   - Lookup bpm_hum_assignmentrule_tbl por flowelement (processdef.taskDef).
 *   - Reglas: user explicit / role lookup vía iam_role_user / group / expr.
 *   - Configurable por tenant + processdef.
 */
@Service
public class TaskAssignmentService {

    private static final Logger log = LoggerFactory.getLogger(TaskAssignmentService.class);

    /**
     * UUIDs sintéticos derivados de los strings hardcoded en cada
     * `<microservicio>ServiceTokenProvider.java`. Cualquier user_id que
     * caiga acá NO es un human user.
     */
    private static final Set<String> SERVICE_ACCOUNT_NAMES = Set.of(
        "inventory-service-account",
        "bpm-service-account",
        "system-service-account",
        "iam-service-account"
    );

    private Set<UUID> serviceUserIds;

    @Value("${imap.bpm.fallback-assignee-user-id:715ae6f6-e8b5-448d-bc14-b2644b48f508}")
    private String fallbackAssigneeUserIdStr;

    private UUID fallbackAssigneeUserId;

    @PostConstruct
    void init() {
        this.serviceUserIds = Set.copyOf(SERVICE_ACCOUNT_NAMES.stream()
            .map(name -> UUID.nameUUIDFromBytes(name.getBytes(StandardCharsets.UTF_8)))
            .toList());
        try {
            this.fallbackAssigneeUserId = UUID.fromString(fallbackAssigneeUserIdStr);
        } catch (IllegalArgumentException e) {
            log.error("TaskAssignmentService: invalid fallback UUID '{}', tasks created by service "
                + "accounts will have null assignee", fallbackAssigneeUserIdStr);
            this.fallbackAssigneeUserId = null;
        }
        log.info("TaskAssignmentService enabled — {} service accounts known, fallback admin={}",
            serviceUserIds.size(), fallbackAssigneeUserId);
    }

    /**
     * Resuelve el assignee para una user_task recién creada.
     *
     * @param processdefCode  por si V2+ necesita map per processdef
     * @param taskDefCode     por si V2+ necesita map per task element
     * @param starterUserId   instance.getStartedById() — puede ser null
     * @param tenantId        por si V2+ necesita filter por tenant
     * @return UUID a setear en task.assignedUserId (puede ser null si no hay fallback)
     */
    public UUID resolveAssignee(String processdefCode, String taskDefCode,
                                 UUID starterUserId, UUID tenantId) {
        if (starterUserId == null) {
            log.debug("resolveAssignee: starterUserId null for {}/{}, using fallback {}",
                processdefCode, taskDefCode, fallbackAssigneeUserId);
            return fallbackAssigneeUserId;
        }
        if (serviceUserIds.contains(starterUserId)) {
            log.info("resolveAssignee: starter {} is service account for {}/{} (tenant={}), "
                + "routing to fallback admin {}",
                starterUserId, processdefCode, taskDefCode, tenantId, fallbackAssigneeUserId);
            return fallbackAssigneeUserId;
        }
        return starterUserId;
    }

    /** Namespace de permisos de cola (modelo A — ver workhub-northstar §6.2). */
    public static final String QUEUE_PREFIX = "bpm.queue.";

    /**
     * WorkHub 3b.1 — resuelve el candidate group (cola) de un user_task desde su
     * config. Soporta valor estático ("deposito_ba") o expresión "${var}" contra
     * las variables del proceso. Devuelve el PERMISO de cola ('bpm.queue.<codigo>')
     * o null si el user_task NO define candidate group (→ asignación directa).
     * 3b.2 extenderá este punto con resolución por DMN.
     */
    @SuppressWarnings("unchecked")
    public String resolveCandidateGroup(Map<String, Object> config, Map<String, Object> variables) {
        if (config == null || config.isEmpty()) return null;
        Object raw = config.get("candidateGroup");
        if (raw == null && config.get("assignment") instanceof Map<?, ?> a) {
            raw = ((Map<String, Object>) a).get("candidateGroup");
        }
        if (raw == null) return null;
        String expr = raw.toString().trim();
        if (expr.isEmpty()) return null;
        String resolved = substituteVars(expr, variables).trim();
        if (resolved.isEmpty()) return null;
        return resolved.startsWith(QUEUE_PREFIX) ? resolved : QUEUE_PREFIX + resolved.toLowerCase();
    }

    /** Sustituye ${nombre} por el valor de la variable del proceso (3b.1; DMN en 3b.2). */
    private static String substituteVars(String expr, Map<String, Object> variables) {
        if (variables == null || !expr.contains("${")) return expr;
        java.util.regex.Matcher m = java.util.regex.Pattern.compile("\\$\\{([^}]+)\\}").matcher(expr);
        StringBuilder sb = new StringBuilder();
        while (m.find()) {
            Object v = variables.get(m.group(1).trim());
            m.appendReplacement(sb, java.util.regex.Matcher.quoteReplacement(v == null ? "" : v.toString()));
        }
        m.appendTail(sb);
        return sb.toString();
    }
}
