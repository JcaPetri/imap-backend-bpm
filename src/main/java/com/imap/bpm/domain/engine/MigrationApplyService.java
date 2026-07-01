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

package com.imap.bpm.domain.engine;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.imap.bpm.application.MigrationPlanManagementService;
import com.imap.bpm.domain.dto.MigrationPlanDto;
import com.imap.bpm.infrastructure.entity.AuditLog;
import com.imap.bpm.infrastructure.entity.ProcessInstance;
import com.imap.bpm.infrastructure.entity.Token;
import com.imap.bpm.infrastructure.repository.AuditLogRepository;
import com.imap.bpm.infrastructure.repository.ProcessInstanceRepository;
import com.imap.bpm.infrastructure.repository.TokenRepository;
import com.imap.bpm.infrastructure.sse.SseEventBus;
import com.imap.platform.tenant.TenantContextHolder;
import com.imap.eav.engine.context.EavTenantSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.util.*;

/**
 * Apply de migration plans (Hito 3 multi-version, 2026-05-21).
 *
 * El plan se crea + edita + valida en bpm relacional (F4-mgmt Chunk B —
 * MigrationPlanManagementService, tablas bpm_pro_migrationplan/rule_tbl V015).
 * El APPLY vive acá porque modifica tokens y processinstances del schema bpm +
 * audit log + emite SSE.
 *
 * Flujo:
 *   1. Fetch plan + rules LOCAL (F4-mgmt Chunk B: la gestión se portó a bpm
 *      relacional — antes era un fetch s2s al SYSTEM cell-store)
 *   2. Fetch ProcessDefinition v2 via loader (caché)
 *   3. Loop instances vivas de v1:
 *      a. Loop tokens vivos
 *      b. Match con regla por currentElement code → action:
 *          - map: token.currentElementId = target flowElement.id (en v2)
 *          - cancel: token.lifecycle = cancelled
 *          - skip: token movido al targetCode en v2 + ProcessEngine.skipForMigration()
 *                  cancela tasks asociadas + avanza al outgoing del target. El motor
 *                  cascadea hasta el siguiente wait state (user_task / timer / etc.).
 *      c. instance.processversionId = targetPvId
 *      d. audit "instance.migrated" con {fromPvId, toPvId, planId}
 *      e. SSE broadcast bpm.instance.migrated
 *   4. Vuelve report con stats {instancesAffected, tokensMapped, tokensCancelled, errors}
 *
 * Idempotency: si la instance ya está en targetPvId la skipea (no error).
 *
 * Tras el apply se marca el plan como 'applied' (o 'failed') via
 * MigrationPlanManagementService.markApplyResult LOCAL — antes era un callback
 * s2s de vuelta al SYSTEM.
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
    private final ProcessEngine processEngine;
    private final SseEventBus sseBus;
    private final EavTenantSession tenantSession;
    private final MigrationPlanManagementService planService;
    private final ObjectMapper jackson;

    public MigrationApplyService(ProcessInstanceRepository instanceRepo,
                                 TokenRepository tokenRepo,
                                 AuditLogRepository auditRepo,
                                 ProcessDefinitionLoader defLoader,
                                 ProcessEngine processEngine,
                                 SseEventBus sseBus,
                                 EavTenantSession tenantSession,
                                 MigrationPlanManagementService planService,
                                 ObjectMapper jackson) {
        this.instanceRepo = instanceRepo;
        this.tokenRepo = tokenRepo;
        this.auditRepo = auditRepo;
        this.defLoader = defLoader;
        this.processEngine = processEngine;
        this.sseBus = sseBus;
        this.tenantSession = tenantSession;
        this.planService = planService;
        this.jackson = jackson;
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

        // 1. Fetch plan detail (header + rules) LOCAL (F4-mgmt Chunk B — antes s2s a SYSTEM)
        MigrationPlanDto.PlanDetail planDetail = planService.getPlanDetail(planId);
        MigrationPlanDto.PlanSummary header = planDetail.header();
        List<MigrationPlanDto.RuleDto> rules = planDetail.rules();

        String status = header.status();
        if ("applied".equals(status)) {
            throw new IllegalStateException("Plan " + planId + " already applied");
        }
        if (!"validated".equals(status)) {
            throw new IllegalStateException("Plan must be 'validated' before apply (current: " + status + ")");
        }

        UUID sourcePvId = UUID.fromString(header.sourceProcessversionId());
        UUID targetPvId = UUID.fromString(header.targetProcessversionId());

        // 2. Construir code → rule map para lookup rápido
        Map<String, MigrationPlanDto.RuleDto> rulesByCode = new HashMap<>();
        for (MigrationPlanDto.RuleDto r : rules) {
            rulesByCode.put(r.sourceFlowElementCode(), r);
        }

        // 3. Fetch target def (para resolver target codes → flowElementIds)
        ProcessDefinition targetDef = defLoader.load(targetPvId, null, TenantContextHolder.SYSTEM_TENANT_ID);
        Map<String, UUID> targetCodeToId = new HashMap<>();
        for (ProcessDefinition.FlowElement fe : targetDef.flowElements()) {
            targetCodeToId.put(fe.code(), fe.id());
        }

        // 4. Fetch source def (para resolver currentElementId → code, para matchear con rules)
        ProcessDefinition sourceDef = defLoader.load(sourcePvId, null, TenantContextHolder.SYSTEM_TENANT_ID);
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
                    MigrationPlanDto.RuleDto rule = rulesByCode.get(currentCode);
                    if (rule == null) {
                        warnings.add("Instance " + inst.getId() + " token at '" + currentCode
                            + "' has no migration rule — left as-is");
                        continue;
                    }
                    String action = rule.action();
                    if ("cancel".equals(action)) {
                        tk.setLifecycle("cancelled");
                        tk.setUpdatedAt(now);
                        tokenRepo.save(tk);
                        tokensCancelled++;
                    } else {
                        // map | skip: ambos requieren targetFlowElementCode válido en v2
                        String tgtCode = rule.targetFlowElementCode();
                        UUID tgtId = tgtCode == null ? null : targetCodeToId.get(tgtCode);
                        if (tgtId == null) {
                            errors.add("Instance " + inst.getId() + " token at '" + currentCode
                                + "' " + action + " target '" + tgtCode + "' not found in v2 def");
                            continue;
                        }
                        // Mover el token al targetCode en v2
                        tk.setCurrentElementId(tgtId);
                        tk.setUpdatedAt(now);
                        tokenRepo.save(tk);

                        if ("skip".equals(action)) {
                            // skip: salta este flowElement avanzando al outgoing en v2.
                            // Requiere que el instance.processversionId ya esté en v2
                            // (consumeAndMoveToNext puede triggerear queries de outgoing).
                            if (!targetPvId.equals(inst.getProcessversionId())) {
                                inst.setProcessversionId(targetPvId);
                                inst.setUpdatedAt(now);
                                instanceRepo.save(inst);
                            }
                            ProcessDefinition.FlowElement tgtEl = targetDef.findElementById(tgtId);
                            processEngine.skipForMigration(inst, tk, tgtEl, targetDef, appliedBy);
                        }
                        // map o skip ambos cuentan como "mapped" (el token quedó vivo
                        // en v2; skip además avanzó al siguiente paso automáticamente).
                        tokensMapped++;
                    }
                }

                // Update instance.processversionId al target (idempotente si skip ya lo hizo)
                if (!targetPvId.equals(inst.getProcessversionId())) {
                    inst.setProcessversionId(targetPvId);
                    inst.setUpdatedAt(now);
                    instanceRepo.save(inst);
                }

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

        // 6. Marcar el plan como applied/failed (update status + appliedAt/appliedBy/stats).
        //    LOCAL (F4-mgmt Chunk B) — antes era un callback s2s de vuelta al SYSTEM.
        //    Corre en la MISMA tx del apply; si falla el marcado no rompemos el apply
        //    (ya se ejecutó): log warn — admin puede re-marcar via /apply-status.
        try {
            String statsJson = jackson.writeValueAsString(report);
            planService.markApplyResult(planId,
                appliedBy == null ? null : appliedBy.toString(),
                statsJson, errors.isEmpty());
            log.info("Plan {} marked apply-result LOCAL: ok={} appliedBy={}",
                planId, errors.isEmpty(), appliedBy);
        } catch (Exception e) {
            log.warn("Failed to mark plan {} apply-result: {}", planId, e.getMessage());
        }

        return report;
    }
}
