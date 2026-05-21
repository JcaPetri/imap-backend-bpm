package com.imap.bpm.domain.engine;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.imap.bpm.infrastructure.entity.AuditLog;
import com.imap.bpm.infrastructure.entity.ProcessInstance;
import com.imap.bpm.infrastructure.entity.Token;
import com.imap.bpm.infrastructure.repository.AuditLogRepository;
import com.imap.bpm.infrastructure.repository.ProcessInstanceRepository;
import com.imap.bpm.infrastructure.repository.TokenRepository;
import com.imap.bpm.infrastructure.security.BpmServiceTokenProvider;
import com.imap.bpm.infrastructure.sse.SseEventBus;
import com.imap.bpm.infrastructure.tenant.TenantContextHolder;
import com.imap.eav.engine.context.EavTenantSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.reactive.function.client.WebClient;

import java.time.OffsetDateTime;
import java.util.*;

/**
 * Apply de migration plans (Hito 3 multi-version, 2026-05-21).
 *
 * El plan se crea + edita + valida en el SYSTEM microservicio (cell-store
 * de entities virtuales). El APPLY vive acá porque modifica tokens y
 * processinstances del schema bpm + audit log + emite SSE.
 *
 * Flujo:
 *   1. Fetch plan + rules via s2s desde SYSTEM (service token)
 *   2. Fetch ProcessDefinition v2 via loader (caché)
 *   3. Loop instances vivas de v1:
 *      a. Loop tokens vivos
 *      b. Match con regla por currentElement code → action:
 *          - map: token.currentElementId = target flowElement.id (en v2)
 *          - cancel: token.lifecycle = cancelled
 *          - skip: NO IMPLEMENTADO V1 (tratado como cancel + warning)
 *      c. instance.processversionId = targetPvId
 *      d. audit "instance.migrated" con {fromPvId, toPvId, planId}
 *      e. SSE broadcast bpm.instance.migrated
 *   4. Vuelve report con stats {instancesAffected, tokensMapped, tokensCancelled, errors}
 *
 * Idempotency: si la instance ya está en targetPvId la skipea (no error).
 *
 * El SYSTEM marca el plan como 'applied' tras success — esa parte se hace via
 * llamada de vuelta s2s. Si falla, marca 'failed' con stats.
 */
@Service
public class MigrationApplyService {

    private static final Logger log = LoggerFactory.getLogger(MigrationApplyService.class);

    /** UUID dummy del state activo — alineado con ProcessEngine.DEFAULT_STATE_ACTIVE. */
    private static final UUID DEFAULT_STATE_ACTIVE =
        UUID.fromString("019dc6f0-aa83-7333-8888-000000000001");

    private final ProcessInstanceRepository instanceRepo;
    private final TokenRepository tokenRepo;
    private final AuditLogRepository auditRepo;
    private final ProcessDefinitionLoader defLoader;
    private final BpmServiceTokenProvider serviceTokenProvider;
    private final SseEventBus sseBus;
    private final EavTenantSession tenantSession;
    private final WebClient http;
    private final ObjectMapper jackson;

    public MigrationApplyService(ProcessInstanceRepository instanceRepo,
                                 TokenRepository tokenRepo,
                                 AuditLogRepository auditRepo,
                                 ProcessDefinitionLoader defLoader,
                                 BpmServiceTokenProvider serviceTokenProvider,
                                 SseEventBus sseBus,
                                 EavTenantSession tenantSession,
                                 ObjectMapper jackson,
                                 @Value("${imap.system.base-url:http://localhost:8092/imap/system}") String systemBaseUrl) {
        this.instanceRepo = instanceRepo;
        this.tokenRepo = tokenRepo;
        this.auditRepo = auditRepo;
        this.defLoader = defLoader;
        this.serviceTokenProvider = serviceTokenProvider;
        this.sseBus = sseBus;
        this.tenantSession = tenantSession;
        this.jackson = jackson;
        this.http = WebClient.builder().baseUrl(systemBaseUrl).build();
    }

    public record ApplyReport(
        int instancesAffected,
        int tokensMapped,
        int tokensCancelled,
        List<String> errors,
        List<String> warnings
    ) {}

    @Transactional
    public ApplyReport applyPlan(UUID planId, UUID appliedBy) {
        log.info("APPLY migration plan id={} by user={}", planId, appliedBy);

        // RLS: scopear todas las queries al tenant del caller (mismo patrón
        // que BpmProcessController admin endpoints). Cada tenant aplica la
        // migración sobre sus propias instances — invocación cross-tenant
        // requiere que el admin llame por tenant.
        tenantSession.applyToCurrentTransaction();

        // 1. Fetch plan detail (header + rules) via s2s
        Map<String, Object> planDetail = fetchPlanDetail(planId);
        Map<String, Object> header = (Map<String, Object>) planDetail.get("header");
        List<Map<String, Object>> rules = (List<Map<String, Object>>) planDetail.get("rules");

        String status = (String) header.get("status");
        if ("applied".equals(status)) {
            throw new IllegalStateException("Plan " + planId + " already applied");
        }
        if (!"validated".equals(status)) {
            throw new IllegalStateException("Plan must be 'validated' before apply (current: " + status + ")");
        }

        UUID sourcePvId = UUID.fromString((String) header.get("sourceProcessversionId"));
        UUID targetPvId = UUID.fromString((String) header.get("targetProcessversionId"));

        // 2. Construir code → rule map para lookup rápido
        Map<String, Map<String, Object>> rulesByCode = new HashMap<>();
        for (Map<String, Object> r : rules) {
            rulesByCode.put((String) r.get("sourceFlowElementCode"), r);
        }

        // 3. Fetch target def (para resolver target codes → flowElementIds)
        ProcessDefinition targetDef = defLoader.load(targetPvId, null, TenantContextHolder.get());
        Map<String, UUID> targetCodeToId = new HashMap<>();
        for (ProcessDefinition.FlowElement fe : targetDef.flowElements()) {
            targetCodeToId.put(fe.code(), fe.id());
        }

        // 4. Fetch source def (para resolver currentElementId → code, para matchear con rules)
        ProcessDefinition sourceDef = defLoader.load(sourcePvId, null, TenantContextHolder.get());
        Map<UUID, String> sourceIdToCode = new HashMap<>();
        for (ProcessDefinition.FlowElement fe : sourceDef.flowElements()) {
            sourceIdToCode.put(fe.id(), fe.code());
        }

        // 5. Loop instances vivas en source (RLS-filtered al tenant del caller)
        List<ProcessInstance> instances = instanceRepo.findByProcessversionIdAndLifecycle(
            sourcePvId, "active");

        int instancesAffected = 0;
        int tokensMapped = 0;
        int tokensCancelled = 0;
        List<String> errors = new ArrayList<>();
        List<String> warnings = new ArrayList<>();
        OffsetDateTime now = OffsetDateTime.now();

        for (ProcessInstance inst : instances) {
            try {
                List<Token> tokens = tokenRepo.findByProcessinstanceIdAndLifecycleIn(
                    inst.getId(), List.of("active", "waiting"));

                for (Token tk : tokens) {
                    String currentCode = sourceIdToCode.get(tk.getCurrentElementId());
                    if (currentCode == null) {
                        warnings.add("Instance " + inst.getId() + " token " + tk.getId()
                            + " currentElementId not in source def — skipped");
                        continue;
                    }
                    Map<String, Object> rule = rulesByCode.get(currentCode);
                    if (rule == null) {
                        warnings.add("Instance " + inst.getId() + " token at '" + currentCode
                            + "' has no migration rule — left as-is");
                        continue;
                    }
                    String action = (String) rule.get("action");
                    if ("cancel".equals(action) || "skip".equals(action)) {
                        // skip tratado como cancel en V1 (TODO: implementar advance)
                        if ("skip".equals(action)) {
                            warnings.add("'skip' action not implemented in V1 — treated as cancel for token "
                                + tk.getId());
                        }
                        tk.setLifecycle("cancelled");
                        tk.setUpdatedAt(now);
                        tokenRepo.save(tk);
                        tokensCancelled++;
                    } else {
                        // map: resolver target code → target flowElement id
                        String tgtCode = (String) rule.get("targetFlowElementCode");
                        UUID tgtId = tgtCode == null ? null : targetCodeToId.get(tgtCode);
                        if (tgtId == null) {
                            errors.add("Instance " + inst.getId() + " token at '" + currentCode
                                + "' map target '" + tgtCode + "' not found in v2 def");
                            continue;
                        }
                        tk.setCurrentElementId(tgtId);
                        tk.setUpdatedAt(now);
                        tokenRepo.save(tk);
                        tokensMapped++;
                    }
                }

                // Update instance.processversionId al target
                inst.setProcessversionId(targetPvId);
                inst.setUpdatedAt(now);
                instanceRepo.save(inst);

                // Audit — usa el patrón JPA standard (mismo que ProcessEngine.audit())
                // para evitar mismatches con columnas/Hibernate type handlers.
                AuditLog al = new AuditLog();
                al.setId(UUID.randomUUID());
                al.setTenantId(inst.getTenantId());
                al.setProcessinstanceId(inst.getId());
                al.setEventType("instance.migrated");
                al.setOccurredAt(now);
                al.setUserId(appliedBy);
                Map<String, Object> auditData = new LinkedHashMap<>();
                auditData.put("fromProcessversionId", sourcePvId.toString());
                auditData.put("toProcessversionId", targetPvId.toString());
                auditData.put("planId", planId.toString());
                al.setData(auditData);
                al.setStateId(DEFAULT_STATE_ACTIVE);
                al.setCreatedAt(now);
                al.setUpdatedAt(now);
                auditRepo.save(al);

                // SSE broadcast — frontend live update si tiene la instance abierta
                try {
                    sseBus.broadcast("bpm.instance.migrated", Map.of(
                        "instanceId", inst.getId().toString(),
                        "fromProcessversionId", sourcePvId.toString(),
                        "toProcessversionId", targetPvId.toString(),
                        "planId", planId.toString(),
                        "ts", now.toInstant().toEpochMilli()
                    ));
                } catch (Exception sseE) {
                    log.warn("SSE broadcast bpm.instance.migrated failed for instance {}: {}",
                        inst.getId(), sseE.getMessage());
                }

                instancesAffected++;
            } catch (Exception e) {
                errors.add("Instance " + inst.getId() + " failed: " + e.getMessage());
                log.error("Migration apply failed for instance {}", inst.getId(), e);
            }
        }

        ApplyReport report = new ApplyReport(instancesAffected, tokensMapped, tokensCancelled, errors, warnings);
        log.info("APPLY DONE plan={} stats={}", planId, report);

        // 6. Notificar al SYSTEM que el plan se aplicó (update status + stats)
        try {
            String statsJson = jackson.writeValueAsString(report);
            notifyPlanApplied(planId, appliedBy, statsJson, errors.isEmpty());
        } catch (Exception e) {
            log.warn("Failed to notify SYSTEM that plan {} was applied: {}", planId, e.getMessage());
        }

        return report;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> fetchPlanDetail(UUID planId) {
        String svcToken = serviceTokenProvider.currentToken();
        Map<String, Object> resp = http.get()
            .uri("/v1/admin/bpm/migration-plans/{id}", planId.toString())
            .headers(h -> {
                if (svcToken != null) h.set(HttpHeaders.AUTHORIZATION, "Bearer " + svcToken);
                h.set("X-Tenant-Id", TenantContextHolder.get().toString());
            })
            .retrieve()
            .bodyToMono(Map.class)
            .block();
        if (resp == null) throw new IllegalStateException("System returned null for plan " + planId);
        return resp;
    }

    private void notifyPlanApplied(UUID planId, UUID appliedBy, String statsJson, boolean ok) {
        // Callback s2s al SYSTEM para que marque el plan como 'applied' o 'failed'
        // con appliedAt/appliedBy/stats. Si la llamada falla, no rompemos el apply
        // (ya se ejecutó); solo log warn — admin puede re-marcar manualmente.
        String svcToken = serviceTokenProvider.currentToken();
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("appliedBy", appliedBy == null ? null : appliedBy.toString());
        body.put("stats", statsJson);
        body.put("success", ok);

        http.post()
            .uri("/v1/admin/bpm/migration-plans/{id}/apply-status", planId.toString())
            .headers(h -> {
                if (svcToken != null) h.set(HttpHeaders.AUTHORIZATION, "Bearer " + svcToken);
                h.set("X-Tenant-Id", TenantContextHolder.get().toString());
                h.setContentType(org.springframework.http.MediaType.APPLICATION_JSON);
            })
            .bodyValue(body)
            .retrieve()
            .toBodilessEntity()
            .block();

        log.info("Plan {} apply-status notified to SYSTEM: ok={} appliedBy={}", planId, ok, appliedBy);
    }
}
