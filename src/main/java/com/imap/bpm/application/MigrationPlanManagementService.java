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

package com.imap.bpm.application;

import com.imap.bpm.domain.dto.MigrationPlanDto;
import com.imap.bpm.domain.model.Migrationplan;
import com.imap.bpm.domain.model.Migrationrule;
import com.imap.bpm.domain.port.out.MigrationplanRepository;
import com.imap.bpm.domain.port.out.MigrationruleRepository;
import com.imap.bpm.infrastructure.entity.FlowelementEntity;
import com.imap.bpm.infrastructure.entity.ProcessversionEntity;
import com.imap.bpm.infrastructure.repository.FlowelementRepository;
import com.imap.bpm.infrastructure.repository.ProcessInstanceRepository;
import com.imap.bpm.infrastructure.repository.ProcessversionRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.*;

/**
 * Servicio de GESTIÓN de migration plans sobre las tablas RELACIONALES de bpm
 * (V015). Portada fiel de system.MigrationService (que escribía cell-store EAV):
 * ahora el plan vive en bpm_pro_migrationplan_tbl / bpm_pro_migrationrule_tbl y
 * los counts de instancias son LOCALES (F4-mgmt Chunk B disolvió el último resto
 * del SQL cross-service system→bpm que hacía validatePlan).
 *
 * Un migration plan describe cómo mover instances vivas de v1 → v2 del mismo
 * processdef. Cada regla mapea un flowElement source → target con una acción
 * (map | skip | cancel).
 *
 * Lifecycle del plan:
 *   draft → validated → applied (o → failed)
 *
 * Responsabilidades:
 *   - createPlan: header (status 'draft') + auto-genera rules 1:1 por code match
 *   - listPlans: filtrable por processdef (via la source processversion)
 *   - getPlanDetail: header + rules ordenadas
 *   - updateRules: reemplaza las rules (bulk delete + recrea), reset status a draft
 *   - validatePlan: cuenta instances activas LOCALES + verifica coverage
 *   - markApplyResult: marca applied/failed + appliedAt/appliedBy/stats (callback del apply)
 *
 * NOTA de tenant/user: bpm NO setea el GUC de user, así que este servicio setea
 * explícitamente created_by_id/owned_by_id/state_id/tenant_id en cada entity. El
 * tenantSession.applyToCurrentTransaction() lo hace el controller (que abre la tx).
 */
@Service
public class MigrationPlanManagementService {

    private static final Logger log = LoggerFactory.getLogger(MigrationPlanManagementService.class);

    /** state_id default = ACTIVE (bpm no resuelve sys_state por HTTP en el path de escritura). */
    private static final UUID DEFAULT_STATE_ACTIVE =
        UUID.fromString("019dc6f0-aa83-7333-8888-000000000001");

    private final MigrationplanRepository planRepo;
    private final MigrationruleRepository ruleRepo;
    private final FlowelementRepository flowelementRepository;
    private final ProcessversionRepository processversionRepository;
    private final ProcessInstanceRepository processInstanceRepository;

    public MigrationPlanManagementService(MigrationplanRepository planRepo,
                                          MigrationruleRepository ruleRepo,
                                          FlowelementRepository flowelementRepository,
                                          ProcessversionRepository processversionRepository,
                                          ProcessInstanceRepository processInstanceRepository) {
        this.planRepo = planRepo;
        this.ruleRepo = ruleRepo;
        this.flowelementRepository = flowelementRepository;
        this.processversionRepository = processversionRepository;
        this.processInstanceRepository = processInstanceRepository;
    }

    // ════════════════════════════════════════════════════════════════════════
    //  CREATE — con auto-generación de reglas 1:1 por code match
    // ════════════════════════════════════════════════════════════════════════

    /**
     * Crea el plan (status 'draft') + auto-genera reglas 1:1 por code match entre
     * los flowElements de la source y la target processversion (LOCAL, antes system
     * leía cells). Portada fiel de system.MigrationService.createPlan.
     * El controller ya abrió la tx + aplicó tenantSession.
     */
    public MigrationPlanDto.PlanDetail createPlan(MigrationPlanDto.CreatePlanRequest req,
                                                  UUID tenantId, UUID userId) {
        // 1. Validar inputs
        requireNonBlank("code", req.code());
        requireNonBlank("sourceProcessversionId", req.sourceProcessversionId());
        requireNonBlank("targetProcessversionId", req.targetProcessversionId());
        if (req.sourceProcessversionId().equals(req.targetProcessversionId())) {
            throw new IllegalArgumentException("source and target processversion cannot be the same");
        }

        // 2. Verificar code único (por tenant — uq_migrationplan_code)
        if (planRepo.findByTenantIdAndCode(tenantId, req.code()).isPresent()) {
            throw new IllegalArgumentException(
                "migration plan with code '" + req.code() + "' already exists");
        }

        UUID srcPvId = UUID.fromString(req.sourceProcessversionId());
        UUID tgtPvId = UUID.fromString(req.targetProcessversionId());

        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);

        // 3. Crear header del plan (single-INSERT, pero seteamos created_at/updated_at
        //    explícito para no depender del DEFAULT now() en el flush).
        UUID planId = UUID.randomUUID();
        Migrationplan plan = new Migrationplan(
            planId, tenantId, req.code(), blankToNull(req.description()),
            srcPvId, tgtPvId, "draft",
            null, null, null, DEFAULT_STATE_ACTIVE,
            now, now, userId, null, userId);
        planRepo.save(plan);

        // 4. Auto-generar reglas 1:1 por code match entre source y target (LOCAL)
        List<String> sourceCodes = loadFlowElementCodes(srcPvId);
        Set<String> targetCodes  = new HashSet<>(loadFlowElementCodes(tgtPvId));

        int ruleCount = 0;
        for (String srcCode : sourceCodes) {
            boolean inTarget = targetCodes.contains(srcCode);
            String action = inTarget ? "map" : "cancel";
            String tgtCode = inTarget ? srcCode : null;
            createRule(planId, srcCode, tgtCode, action, ruleCount + 1,
                inTarget ? "Auto-generated: same code in v2"
                         : "Auto-generated: source code not in v2 — defaulted to cancel",
                tenantId, userId, now);
            ruleCount++;
        }

        log.info("Created migration plan id={} code='{}' rules={}", planId, req.code(), ruleCount);
        return getPlanDetail(planId);
    }

    private void createRule(UUID planId, String srcCode, String tgtCode, String action,
                            int sortOrder, String notes,
                            UUID tenantId, UUID userId, OffsetDateTime now) {
        Migrationrule rule = new Migrationrule(
            UUID.randomUUID(), tenantId, planId,
            srcCode, blankToNull(tgtCode), action,
            sortOrder, blankToNull(notes), DEFAULT_STATE_ACTIVE,
            now, now, userId, null, userId);
        ruleRepo.save(rule);
    }

    // ════════════════════════════════════════════════════════════════════════
    //  GET / LIST
    // ════════════════════════════════════════════════════════════════════════

    /**
     * Lista los planes del tenant (más recientes primero). Filter opcional por
     * processdefId: se resuelve via la source processversion → processdef.
     */
    public List<MigrationPlanDto.PlanSummary> listPlans(UUID tenantId, String processdefId) {
        List<Migrationplan> plans = planRepo.findByTenantIdOrderByCreatedAtDesc(tenantId);
        List<MigrationPlanDto.PlanSummary> out = new ArrayList<>();
        for (Migrationplan plan : plans) {
            if (processdefId != null && !processdefId.isBlank()) {
                UUID pdId = lookupProcessdefIdOfPv(plan.getSourceProcessversionId());
                if (pdId == null || !pdId.toString().equals(processdefId)) continue;
            }
            out.add(toSummary(plan));
        }
        return out;
    }

    /** Detalle del plan: header + rules ordenadas. Tira NoSuchElementException si no existe. */
    public MigrationPlanDto.PlanDetail getPlanDetail(UUID planId) {
        Migrationplan plan = planRepo.findById(planId).orElse(null);
        if (plan == null) {
            throw new NoSuchElementException("Migration plan not found: " + planId);
        }
        List<MigrationPlanDto.RuleDto> rules = loadRules(planId);
        return new MigrationPlanDto.PlanDetail(toSummary(plan), rules, null);
    }

    // ════════════════════════════════════════════════════════════════════════
    //  UPDATE RULES — admin edita los mappings
    // ════════════════════════════════════════════════════════════════════════

    /**
     * Reemplaza FULL las rules del plan (bulk delete + recrea) y resetea el status
     * a 'draft' (requiere re-validar). No editable si el plan ya está 'applied'.
     * Portada fiel de system.MigrationService.updateRules.
     */
    public MigrationPlanDto.PlanDetail updateRules(UUID planId, MigrationPlanDto.UpdateRulesRequest req,
                                                   UUID tenantId, UUID userId) {
        Migrationplan plan = planRepo.findById(planId).orElse(null);
        if (plan == null) throw new NoSuchElementException("Plan not found: " + planId);
        if ("applied".equals(plan.getStatus())) {
            throw new IllegalStateException("Cannot edit rules of an applied plan");
        }

        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);

        // Strategy: bulk delete rules viejas (int, ejecución inmediata) + recrea.
        // Un deleteBy derivado difiere el DELETE al flush → Hibernate ordena INSERTS
        // antes → colisión de uq_migrationrule (tenant, plan, source_code) al recrear.
        ruleRepo.deleteByMigrationplanId(planId);

        int count = 0;
        if (req.rules() != null) {
            int order = 1;
            for (MigrationPlanDto.RuleDto r : req.rules()) {
                createRule(planId,
                    r.sourceFlowElementCode(),
                    r.targetFlowElementCode(),
                    r.action() != null ? r.action() : "map",
                    r.sortOrder() != null ? r.sortOrder() : order,
                    r.notes(),
                    tenantId, userId, now);
                order++;
                count++;
            }
        }

        // Reset status a draft (re-validar tras editar reglas). Construimos un nuevo
        // modelo copiando lo que NO cambia (el adapter load-and-mutate preserva
        // created_at por updatable=false igual) y seteando status/updated_*.
        Migrationplan updated = new Migrationplan(
            plan.getId(), plan.getTenantId(), plan.getCode(), plan.getDescription(),
            plan.getSourceProcessversionId(), plan.getTargetProcessversionId(), "draft",
            plan.getAppliedAt(), plan.getAppliedBy(), plan.getStats(), plan.getStateId(),
            plan.getCreatedAt(), now, plan.getCreatedById(), userId, plan.getOwnedById());
        planRepo.save(updated);

        log.info("Updated rules for plan {} — count={}", planId, count);
        return getPlanDetail(planId);
    }

    // ════════════════════════════════════════════════════════════════════════
    //  VALIDATE — chequea coverage contra instances vivas LOCALES
    // ════════════════════════════════════════════════════════════════════════

    /**
     * Valida el plan: cuenta instances ACTIVAS de la source processversion (LOCAL,
     * era SQL cross-service system→bpm) y verifica coverage de rules contra los
     * flowElement codes LOCALES. Update status a validated/draft según resultado.
     * Portada fiel de system.MigrationService.validatePlan.
     */
    public MigrationPlanDto.ValidationReport validatePlan(UUID planId, UUID userId) {
        Migrationplan plan = planRepo.findById(planId).orElse(null);
        if (plan == null) throw new NoSuchElementException("Plan not found: " + planId);

        UUID srcPvId = plan.getSourceProcessversionId();
        UUID tgtPvId = plan.getTargetProcessversionId();

        List<String> errors = new ArrayList<>();
        List<String> warnings = new ArrayList<>();

        // 1. Contar instances activas en source (LOCAL, disuelve el último resto de G1)
        int sourceInstancesActive =
            (int) processInstanceRepository.countByProcessversionIdAndLifecycle(srcPvId, "active");

        // 2. Cargar reglas + flowElement codes de source y target (LOCALES)
        List<MigrationPlanDto.RuleDto> rules = loadRules(planId);
        Set<String> ruleSourceCodes = new HashSet<>();
        for (MigrationPlanDto.RuleDto r : rules) {
            if (r.sourceFlowElementCode() != null) {
                ruleSourceCodes.add(r.sourceFlowElementCode());
            }
        }
        Set<String> sourceCodes = new HashSet<>(loadFlowElementCodes(srcPvId));
        Set<String> targetCodes = new HashSet<>(loadFlowElementCodes(tgtPvId));

        // 3. Cada source code debe tener regla
        for (String srcCode : sourceCodes) {
            if (!ruleSourceCodes.contains(srcCode)) {
                errors.add("Source flowElement '" + srcCode + "' has no migration rule");
            }
        }

        // 4. Cada regla con action=map debe tener target code que exista en v2
        for (MigrationPlanDto.RuleDto r : rules) {
            if ("map".equals(r.action())) {
                if (r.targetFlowElementCode() == null || r.targetFlowElementCode().isBlank()) {
                    errors.add("Rule for source '" + r.sourceFlowElementCode() + "' has action=map but no target");
                } else if (!targetCodes.contains(r.targetFlowElementCode())) {
                    errors.add("Rule for source '" + r.sourceFlowElementCode()
                        + "' maps to non-existent target '" + r.targetFlowElementCode() + "'");
                }
            }
        }

        // 5. Warning si hay target codes nuevos no cubiertos
        Set<String> targetCodesMapped = new HashSet<>();
        for (MigrationPlanDto.RuleDto r : rules) {
            if ("map".equals(r.action()) && r.targetFlowElementCode() != null) {
                targetCodesMapped.add(r.targetFlowElementCode());
            }
        }
        for (String tgtCode : targetCodes) {
            if (!targetCodesMapped.contains(tgtCode)) {
                warnings.add("Target flowElement '" + tgtCode
                    + "' is not target of any map rule — new tokens won't be created there during apply");
            }
        }

        // 6. Update status del plan según resultado (preserva created_at via load-and-mutate)
        boolean valid = errors.isEmpty();
        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
        Migrationplan updated = new Migrationplan(
            plan.getId(), plan.getTenantId(), plan.getCode(), plan.getDescription(),
            plan.getSourceProcessversionId(), plan.getTargetProcessversionId(), valid ? "validated" : "draft",
            plan.getAppliedAt(), plan.getAppliedBy(), plan.getStats(), plan.getStateId(),
            plan.getCreatedAt(), now, plan.getCreatedById(), userId, plan.getOwnedById());
        planRepo.save(updated);

        return new MigrationPlanDto.ValidationReport(valid, sourceInstancesActive, errors, warnings);
    }

    // ════════════════════════════════════════════════════════════════════════
    //  MARK APPLY RESULT — invocado por el APPLY local tras ejecutar
    // ════════════════════════════════════════════════════════════════════════

    /**
     * Marca el plan como 'applied' o 'failed' tras la ejecución del apply y persiste
     * appliedAt, appliedBy y stats (JSON). Idempotency: si ya está 'applied' devuelve
     * current sin error. Portada fiel de system.MigrationService.markApplyResult.
     * Carga la entity con findById (preserva created_at) + updated_at=now.
     */
    public MigrationPlanDto.PlanSummary markApplyResult(UUID planId, String appliedByUserId,
                                                        String statsJson, boolean success) {
        Migrationplan plan = planRepo.findById(planId).orElse(null);
        if (plan == null) throw new NoSuchElementException("Plan not found: " + planId);

        // Idempotency: si ya está applied, no re-marcar (devuelve current)
        if ("applied".equals(plan.getStatus())) {
            return toSummary(plan);
        }

        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
        UUID appliedBy = parseUuidOrNull(appliedByUserId);

        String status = success ? "applied" : "failed";
        UUID newAppliedBy = appliedBy != null ? appliedBy : plan.getAppliedBy();
        String newStats = statsJson != null ? statsJson : plan.getStats();

        Migrationplan updated = new Migrationplan(
            plan.getId(), plan.getTenantId(), plan.getCode(), plan.getDescription(),
            plan.getSourceProcessversionId(), plan.getTargetProcessversionId(), status,
            now, newAppliedBy, newStats, plan.getStateId(),
            plan.getCreatedAt(), now, plan.getCreatedById(), appliedBy, plan.getOwnedById());
        Migrationplan saved = planRepo.save(updated);

        log.info("Marked plan {} as '{}' (appliedBy={}, statsLen={})",
            planId, saved.getStatus(), appliedByUserId, statsJson == null ? 0 : statsJson.length());
        return toSummary(saved);
    }

    // ════════════════════════════════════════════════════════════════════════
    //  Helpers
    // ════════════════════════════════════════════════════════════════════════

    private MigrationPlanDto.PlanSummary toSummary(Migrationplan plan) {
        return new MigrationPlanDto.PlanSummary(
            plan.getId().toString(),
            plan.getCode(),
            plan.getDescription(),
            plan.getSourceProcessversionId() == null ? null : plan.getSourceProcessversionId().toString(),
            plan.getTargetProcessversionId() == null ? null : plan.getTargetProcessversionId().toString(),
            plan.getStatus() == null ? "draft" : plan.getStatus(),
            plan.getAppliedAt() == null ? null : plan.getAppliedAt().toString(),
            plan.getAppliedBy() == null ? null : plan.getAppliedBy().toString(),
            plan.getStats()
        );
    }

    private List<MigrationPlanDto.RuleDto> loadRules(UUID planId) {
        List<Migrationrule> rules = ruleRepo.findByMigrationplanIdOrderBySortOrder(planId);
        List<MigrationPlanDto.RuleDto> out = new ArrayList<>(rules.size());
        for (Migrationrule r : rules) {
            out.add(new MigrationPlanDto.RuleDto(
                r.getSourceFlowelementCode(),
                r.getTargetFlowelementCode(),
                r.getAction() == null ? "map" : r.getAction(),
                r.getSortOrder() == null ? 0 : r.getSortOrder(),
                r.getNotes()
            ));
        }
        return out;
    }

    /** Codes de los flowElements de una processversion (LOCAL — antes system leía cells). */
    private List<String> loadFlowElementCodes(UUID processversionId) {
        List<FlowelementEntity> elements = flowelementRepository
            .findByProcessversionIdOrderBySortOrder(processversionId);
        List<String> out = new ArrayList<>(elements.size());
        for (FlowelementEntity fe : elements) out.add(fe.getElementCode());
        return out;
    }

    /** processdef_id de una processversion (para el filtro de listPlans). */
    private UUID lookupProcessdefIdOfPv(UUID pvId) {
        if (pvId == null) return null;
        ProcessversionEntity pv = processversionRepository.findById(pvId).orElse(null);
        return pv == null ? null : pv.getProcessdefId();
    }

    private static UUID parseUuidOrNull(String s) {
        if (s == null || s.isBlank()) return null;
        try {
            return UUID.fromString(s);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    private static String blankToNull(String s) {
        return (s == null || s.isBlank()) ? null : s;
    }

    private static void requireNonBlank(String field, String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("field '" + field + "' is required");
        }
    }
}
