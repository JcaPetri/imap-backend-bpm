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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.imap.bpm.domain.dto.CreateProcessdefRequest;
import com.imap.bpm.domain.dto.CreateProcessdefResponse;
import com.imap.bpm.infrastructure.SystemEntityResolver;
import com.imap.bpm.infrastructure.entity.FlowelementEntity;
import com.imap.bpm.infrastructure.entity.ProcessdefEntity;
import com.imap.bpm.infrastructure.entity.ProcessversionEntity;
import com.imap.bpm.infrastructure.entity.SequenceflowEntity;
import com.imap.bpm.infrastructure.entity.TaskformEntity;
import com.imap.bpm.infrastructure.repository.FlowelementRepository;
import com.imap.bpm.infrastructure.repository.ProcessInstanceRepository;
import com.imap.bpm.infrastructure.repository.ProcessdefRepository;
import com.imap.bpm.infrastructure.repository.ProcessversionRepository;
import com.imap.bpm.infrastructure.repository.SequenceflowRepository;
import com.imap.bpm.infrastructure.repository.TaskformRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.*;

/**
 * Servicio de GESTIÓN de processdefs sobre las tablas RELACIONALES de bpm (V015).
 *
 * Reemplaza al ProcessdefAdminService de system (que escribía cell-store EAV):
 * crea processdef + processversion v1 + flowelements + sequenceflows + taskforms
 * en una transacción atómica, escribiendo tablas convencionales (grafo con FKs
 * reales). Fase 4-mgmt de IMAP_BPM_OWNERSHIP_MIGRATION.md.
 *
 * Validación cruzada PRE-flight (fail fast, portada fiel del system):
 *   - code único (findByTenantIdAndCode)
 *   - tipos válidos (10 soportados) + codes únicos
 *   - source/target de sequenceflows existen en flowElements + no self-loop
 *   - condition_expr básico (balance de paréntesis + longitud)
 *   - taskForm.flowElementCode es user_task + entityDefCode existe (resuelto s2s)
 *   - Topología: ≥1 start, ≥1 end, todos alcanzables desde algún start
 *
 * Versionado v1-only: re-publish del mismo code → error unicidad. Para "editar"
 * crear un processdef nuevo con otro code (multi-version = iter futura).
 *
 * NOTA de tenant/user: bpm NO setea el GUC de user, así que este servicio setea
 * explícitamente created_by_id/owned_by_id/state_id/tenant_id en cada entity. El
 * tenantSession.applyToCurrentTransaction() lo hace el controller (que abre la tx).
 */
@Service
public class ProcessdefManagementService {

    private static final Logger log = LoggerFactory.getLogger(ProcessdefManagementService.class);

    /** state_id default = ACTIVE (bpm no resuelve sys_state por HTTP en el path de escritura). */
    private static final UUID DEFAULT_STATE_ACTIVE =
        UUID.fromString("019dc6f0-aa83-7333-8888-000000000001");

    private final ProcessdefRepository processdefRepository;
    private final ProcessversionRepository processversionRepository;
    private final FlowelementRepository flowelementRepository;
    private final SequenceflowRepository sequenceflowRepository;
    private final TaskformRepository taskformRepository;
    private final ProcessInstanceRepository processInstanceRepository;
    private final SystemEntityResolver systemEntityResolver;
    private final ObjectMapper mapper;

    public ProcessdefManagementService(ProcessdefRepository processdefRepository,
                                       ProcessversionRepository processversionRepository,
                                       FlowelementRepository flowelementRepository,
                                       SequenceflowRepository sequenceflowRepository,
                                       TaskformRepository taskformRepository,
                                       ProcessInstanceRepository processInstanceRepository,
                                       SystemEntityResolver systemEntityResolver,
                                       ObjectMapper mapper) {
        this.processdefRepository = processdefRepository;
        this.processversionRepository = processversionRepository;
        this.flowelementRepository = flowelementRepository;
        this.sequenceflowRepository = sequenceflowRepository;
        this.taskformRepository = taskformRepository;
        this.processInstanceRepository = processInstanceRepository;
        this.systemEntityResolver = systemEntityResolver;
        this.mapper = mapper;
    }

    // ════════════════════════════════════════════════════════════════════════
    //  CREATE
    // ════════════════════════════════════════════════════════════════════════

    /**
     * Crea el processdef completo (header + v1 + grafo). El controller ya abrió la
     * tx + aplicó tenantSession. Devuelve response con ids reales o (dryRun) nulls.
     */
    public CreateProcessdefResponse create(CreateProcessdefRequest req, UUID tenantId, UUID userId) {
        boolean dryRun = Boolean.TRUE.equals(req.dryRun());

        // 1. Validación PRE-flight (siempre, incluso en dryRun)
        validateRequest(req, false, tenantId);

        int feCount = req.flowElements() == null ? 0 : req.flowElements().size();
        int sfCount = req.sequenceFlows() == null ? 0 : req.sequenceFlows().size();
        int tfCount = req.taskForms() == null ? 0 : req.taskForms().size();

        if (dryRun) {
            log.info("ProcessdefEntity create — DRY RUN OK code='{}' flowElements={} sequenceFlows={} taskForms={}",
                req.header().code(), feCount, sfCount, tfCount);
            return new CreateProcessdefResponse(null, null, 1,
                new CreateProcessdefResponse.Stats(feCount, sfCount, tfCount),
                "Dry-run validation OK", true);
        }

        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);

        // 2. ProcessdefEntity (header)
        ProcessdefEntity pd = new ProcessdefEntity();
        pd.setId(UUID.randomUUID());
        pd.setCode(req.header().code());
        pd.setName(req.header().name());
        pd.setDescription(req.header().description());
        pd.setLifecycle(req.header().lifecycle());
        pd.setStartPermission(blankToNull(req.header().startPermission()));
        stampAudit(pd::setTenantId, pd::setStateId, pd::setCreatedById, pd::setOwnedById, tenantId, userId);
        // created_at/updated_at explícitos: el processdef se re-guarda (UPDATE al setear
        // currentversion_id) y ese UPDATE mandaría created_at=NULL (la entity en memoria no
        // toma el DEFAULT now() del INSERT) → viola NOT NULL. Los demás son single-INSERT.
        pd.setCreatedAt(now);
        pd.setUpdatedAt(now);
        processdefRepository.save(pd);

        // 3. ProcessversionEntity v1
        ProcessversionEntity pv = new ProcessversionEntity();
        pv.setId(UUID.randomUUID());
        pv.setProcessdefId(pd.getId());
        pv.setVersion(1);
        pv.setPublishedAt(now);
        pv.setDefinition("{}");
        pv.setLocked(true);
        pv.setDescription("v1 of " + req.header().code());
        pv.setTenantId(tenantId);
        pv.setStateId(DEFAULT_STATE_ACTIVE);
        pv.setCreatedById(userId);
        pv.setOwnedById(userId);
        processversionRepository.save(pv);

        // 4. Puntero currentversion
        pd.setCurrentversionId(pv.getId());
        processdefRepository.save(pd);

        // 5-7. Grafo (flowElements → sequenceFlows → taskForms) sobre la version v1.
        writeGraph(req, pv.getId(), tenantId, userId);

        log.info("ProcessdefEntity created code='{}' id={} v1={} (fe={}, sf={}, tf={})",
            pd.getCode(), pd.getId(), pv.getId(), feCount, sfCount, tfCount);

        return new CreateProcessdefResponse(pd.getId(), pv.getId(), 1,
            new CreateProcessdefResponse.Stats(feCount, sfCount, tfCount),
            "ProcessdefEntity created", false);
    }

    /**
     * Escribe el grafo (flowElements → sequenceFlows → taskForms) sobre la
     * processversion dada. Reutilizado por create/update/publishNewVersion.
     * Devuelve el mapa code→flowelementId (por si el caller lo necesita).
     * Todas las entities son single-INSERT → created_at lo llena el DEFAULT now().
     */
    private Map<String, UUID> writeGraph(CreateProcessdefRequest req, UUID processversionId,
                                         UUID tenantId, UUID userId) {
        // Flow elements → mapa code→id (para resolver source/target + taskforms)
        Map<String, UUID> feIdByCode = new LinkedHashMap<>();
        for (CreateProcessdefRequest.FlowElement fe : req.flowElements()) {
            FlowelementEntity e = new FlowelementEntity();
            e.setId(UUID.randomUUID());
            e.setProcessversionId(processversionId);
            e.setElementCode(fe.code());
            e.setElementType(fe.type());
            e.setName(fe.name());
            e.setConfig(serializeConfig(fe.config()));
            e.setSortOrder(fe.sortOrder());
            e.setTenantId(tenantId);
            e.setStateId(DEFAULT_STATE_ACTIVE);
            e.setCreatedById(userId);
            e.setOwnedById(userId);
            flowelementRepository.save(e);
            feIdByCode.put(fe.code(), e.getId());
        }

        // Sequence flows (source/target resueltos del mapa)
        if (req.sequenceFlows() != null) {
            for (CreateProcessdefRequest.SequenceFlow sf : req.sequenceFlows()) {
                SequenceflowEntity e = new SequenceflowEntity();
                e.setId(UUID.randomUUID());
                e.setProcessversionId(processversionId);
                e.setSourceId(feIdByCode.get(sf.sourceCode()));   // no-null garantizado por validateRequest
                e.setTargetId(feIdByCode.get(sf.targetCode()));
                e.setConditionExpr(blankToNull(sf.conditionExpr()));
                e.setSortOrder(sf.sortOrder());
                e.setTenantId(tenantId);
                e.setStateId(DEFAULT_STATE_ACTIVE);
                e.setCreatedById(userId);
                e.setOwnedById(userId);
                sequenceflowRepository.save(e);
            }
        }

        // Task forms (flowElementId del mapa; entitydefId resuelto s2s)
        if (req.taskForms() != null) {
            for (CreateProcessdefRequest.TaskForm tf : req.taskForms()) {
                TaskformEntity e = new TaskformEntity();
                e.setId(UUID.randomUUID());
                e.setFlowelementId(feIdByCode.get(tf.flowElementCode()));
                e.setEntitydefId(systemEntityResolver.resolveId(tf.entityDefCode(), tenantId));
                e.setMode(tf.mode() == null || tf.mode().isBlank() ? "edit" : tf.mode());
                e.setTenantId(tenantId);
                e.setStateId(DEFAULT_STATE_ACTIVE);
                e.setCreatedById(userId);
                e.setOwnedById(userId);
                taskformRepository.save(e);
            }
        }
        return feIdByCode;
    }

    // ════════════════════════════════════════════════════════════════════════
    //  UPDATE / SOFT-DELETE / PUBLISH NEW VERSION (F4-mgmt Chunk A)
    // ════════════════════════════════════════════════════════════════════════

    /**
     * Update FULL del processdef (header + shape completo de la version vigente).
     * Portada fiel de system.ProcessdefAdminService.update (líneas 303-433).
     *
     * v1-only in-place: reemplaza el shape de la CURRENT processversion (borra sus
     * flowElements/sequenceFlows/taskforms y los recrea desde el req). NO crea v2
     * (para eso está publishNewVersion).
     *
     * GUARD G1 (ahora LOCAL, ya no SQL cross-service): bloquea con 409 si hay
     * instances activas (lifecycle='active') para no romper runtime.
     *
     * El controller ya abrió la tx + aplicó tenantSession.
     */
    public CreateProcessdefResponse update(UUID processdefId, CreateProcessdefRequest req,
                                           UUID tenantId, UUID userId) {
        boolean dryRun = Boolean.TRUE.equals(req.dryRun());

        // 1. Verificar existencia (null → el controller devuelve 404)
        ProcessdefEntity pd = processdefRepository.findById(processdefId).orElse(null);
        if (pd == null) {
            throw new IllegalArgumentException("ProcessdefEntity not found: " + processdefId);
        }

        // 2. Code es IMMUTABLE
        if (!Objects.equals(pd.getCode(), req.header().code())) {
            throw new IllegalArgumentException(
                "Cannot change code (was '" + pd.getCode() + "', got '" + req.header().code()
                + "'). Create a new processdef with the new code instead.");
        }

        // 3. GUARD de instances activas (G1 local — antes era SQL cross-service system→bpm)
        long active = processInstanceRepository.countByProcessdefIdAndLifecycle(processdefId, "active");
        if (active > 0) {
            throw new IllegalStateException(
                "Cannot edit processdef with " + active + " active instance(s). "
                + "Cancel them first, or create a new processdef with a different code.");
        }

        // 4. Validación cruzada (isUpdate=true → NO chequea unicidad del code)
        validateRequest(req, true, tenantId);

        int feCount = req.flowElements() == null ? 0 : req.flowElements().size();
        int sfCount = req.sequenceFlows() == null ? 0 : req.sequenceFlows().size();
        int tfCount = req.taskForms() == null ? 0 : req.taskForms().size();

        if (dryRun) {
            return new CreateProcessdefResponse(processdefId, null, 1,
                new CreateProcessdefResponse.Stats(feCount, sfCount, tfCount),
                "Dry-run update OK", true);
        }

        // 5. Resolver la current processversion (puntero, o la única si el puntero está vacío)
        ProcessversionEntity pv = null;
        if (pd.getCurrentversionId() != null) {
            pv = processversionRepository.findById(pd.getCurrentversionId()).orElse(null);
        }
        if (pv == null) {
            List<ProcessversionEntity> versions = processversionRepository
                .findByProcessdefIdOrderByVersionDesc(processdefId);
            if (!versions.isEmpty()) pv = versions.get(0);
        }
        if (pv == null) {
            throw new IllegalStateException("ProcessversionEntity not found for processdef " + processdefId);
        }
        UUID pvId = pv.getId();

        // 6. Borrar shape existente de la version (taskforms → sequenceflows → flowelements)
        //    Los taskforms cuelgan de los flowelements, así que se borran ANTES.
        List<FlowelementEntity> existingFe = flowelementRepository.findByProcessversionIdOrderBySortOrder(pvId);
        if (!existingFe.isEmpty()) {
            List<UUID> feIds = new ArrayList<>(existingFe.size());
            for (FlowelementEntity fe : existingFe) feIds.add(fe.getId());
            taskformRepository.deleteByFlowelementIdIn(feIds);
        }
        sequenceflowRepository.deleteByProcessversionId(pvId);
        flowelementRepository.deleteByProcessversionId(pvId);

        // 7. Recrear el grafo desde el req sobre la MISMA version
        writeGraph(req, pvId, tenantId, userId);

        // 8. Actualizar header del processdef (code NO se toca — ya validado immutable).
        //    Cargamos la entity existente (ya tiene created_at de la DB) → el UPDATE preserva created_at.
        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
        pd.setName(req.header().name());
        pd.setDescription(req.header().description());
        pd.setLifecycle(req.header().lifecycle());
        pd.setStartPermission(blankToNull(req.header().startPermission()));
        pd.setUpdatedById(userId);
        pd.setUpdatedAt(now);
        processdefRepository.save(pd);

        log.info("ProcessdefEntity updated — code='{}' processdefId={} pv={} fe={} sf={} tf={}",
            pd.getCode(), processdefId, pvId, feCount, sfCount, tfCount);

        return new CreateProcessdefResponse(processdefId, pvId,
            pv.getVersion() == null ? 1 : pv.getVersion(),
            new CreateProcessdefResponse.Stats(feCount, sfCount, tfCount),
            "Updated successfully", false);
    }

    /**
     * Soft-delete: lifecycle='inactive'. NO borra nada, NO afecta instances vivas —
     * solo impide arrancar nuevas. Portada fiel de system.softDelete (líneas 440-462).
     * Carga la entity existente (preserva created_at) y solo modifica lifecycle + updated_at.
     */
    public void softDelete(UUID processdefId, UUID userId) {
        ProcessdefEntity pd = processdefRepository.findById(processdefId).orElse(null);
        if (pd == null) {
            throw new IllegalArgumentException("ProcessdefEntity not found: " + processdefId);
        }
        pd.setLifecycle("inactive");
        pd.setUpdatedById(userId);
        pd.setUpdatedAt(OffsetDateTime.now(ZoneOffset.UTC));
        processdefRepository.save(pd);
        log.info("ProcessdefEntity {} soft-deleted (lifecycle=inactive)", processdefId);
    }

    /**
     * Publica una NUEVA processversion (v=N+1) con el shape provisto. NON-destructive:
     * las versions anteriores quedan intactas (instances vivas siguen con su version).
     * currentVersionId apunta a la nueva → las nuevas instances usan v(N+1).
     * Portada fiel de system.publishNewVersion (líneas 554+).
     *
     * Diferencia con update(): update reemplaza shape de la version vigente in-place
     * (bloquea con instances activas); publishNewVersion crea una version nueva (NO
     * bloquea — las viejas siguen safe).
     */
    public CreateProcessdefResponse publishNewVersion(UUID processdefId, CreateProcessdefRequest req,
                                                      UUID tenantId, UUID userId) {
        boolean dryRun = Boolean.TRUE.equals(req.dryRun());

        // 1. Verificar existencia
        ProcessdefEntity pd = processdefRepository.findById(processdefId).orElse(null);
        if (pd == null) {
            throw new IllegalArgumentException("ProcessdefEntity not found: " + processdefId);
        }

        // 2. Code immutable (las versions de un mismo processdef comparten code)
        if (!Objects.equals(pd.getCode(), req.header().code())) {
            throw new IllegalArgumentException(
                "Cannot change code when publishing new version (was '" + pd.getCode()
                + "', got '" + req.header().code() + "'). For a different code, create a new processdef.");
        }

        // 3. Validación cruzada (isUpdate=true → NO chequea unicidad del code)
        validateRequest(req, true, tenantId);

        // 4. version = max(existing) + 1
        List<ProcessversionEntity> versions = processversionRepository
            .findByProcessdefIdOrderByVersionDesc(processdefId);
        int currentMaxVer = 0;
        if (!versions.isEmpty() && versions.get(0).getVersion() != null) {
            currentMaxVer = versions.get(0).getVersion();
        }
        int newVer = currentMaxVer + 1;

        int feCount = req.flowElements() == null ? 0 : req.flowElements().size();
        int sfCount = req.sequenceFlows() == null ? 0 : req.sequenceFlows().size();
        int tfCount = req.taskForms() == null ? 0 : req.taskForms().size();

        if (dryRun) {
            return new CreateProcessdefResponse(processdefId, null, newVer,
                new CreateProcessdefResponse.Stats(feCount, sfCount, tfCount),
                "Dry-run publish v" + newVer + " OK", true);
        }

        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);

        // 5. Crear la nueva processversion (single-INSERT)
        ProcessversionEntity pv = new ProcessversionEntity();
        pv.setId(UUID.randomUUID());
        pv.setProcessdefId(processdefId);
        pv.setVersion(newVer);
        pv.setPublishedAt(now);
        pv.setDefinition("{}");
        pv.setLocked(true);
        pv.setDescription("v" + newVer + " of " + req.header().code());
        pv.setTenantId(tenantId);
        pv.setStateId(DEFAULT_STATE_ACTIVE);
        pv.setCreatedById(userId);
        pv.setOwnedById(userId);
        processversionRepository.save(pv);

        // 6. Grafo de la nueva version
        writeGraph(req, pv.getId(), tenantId, userId);

        // 7. Header del processdef puede haber cambiado (name/description/lifecycle);
        //    code NO. Cargamos la entity existente → preserva created_at. Puntero al nuevo.
        pd.setName(req.header().name());
        pd.setDescription(req.header().description());
        pd.setLifecycle(req.header().lifecycle());
        pd.setStartPermission(blankToNull(req.header().startPermission()));
        pd.setCurrentversionId(pv.getId());
        pd.setUpdatedById(userId);
        pd.setUpdatedAt(now);
        processdefRepository.save(pd);

        log.info("Published v{} of processdef code='{}' processdefId={} newPv={} fe={} sf={} tf={}",
            newVer, pd.getCode(), processdefId, pv.getId(), feCount, sfCount, tfCount);

        return new CreateProcessdefResponse(processdefId, pv.getId(), newVer,
            new CreateProcessdefResponse.Stats(feCount, sfCount, tfCount),
            "Published v" + newVer + " successfully", false);
    }

    // ════════════════════════════════════════════════════════════════════════
    //  READS
    // ════════════════════════════════════════════════════════════════════════

    /**
     * Lista los processdefs del tenant: id, code, name, description, lifecycle,
     * currentVersionId, versionCount.
     */
    public List<Map<String, Object>> listAll(UUID tenantId) {
        List<ProcessdefEntity> defs = processdefRepository.findByTenantIdOrderByCode(tenantId);
        List<Map<String, Object>> out = new ArrayList<>(defs.size());
        for (ProcessdefEntity pd : defs) {
            int versionCount = processversionRepository
                .findByProcessdefIdOrderByVersionDesc(pd.getId()).size();
            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put("processdefId", pd.getId().toString());
            entry.put("code", pd.getCode());
            entry.put("name", pd.getName());
            entry.put("description", pd.getDescription());
            entry.put("lifecycle", pd.getLifecycle());
            entry.put("startPermission", pd.getStartPermission());
            entry.put("currentVersionId",
                pd.getCurrentversionId() == null ? null : pd.getCurrentversionId().toString());
            entry.put("versionCount", versionCount);
            out.add(entry);
        }
        return out;
    }

    /**
     * Detalle COMPLETO del processdef (header + currentversion + flowElements +
     * sequenceFlows + taskForms), mismo shape que devolvía system.getDetail para
     * reuso 1:1 en el builder del frontend. Devuelve null si no existe.
     */
    public Map<String, Object> getDetail(UUID processdefId) {
        ProcessdefEntity pd = processdefRepository.findById(processdefId).orElse(null);
        if (pd == null) return null;

        // currentversion (o última si el puntero está vacío)
        ProcessversionEntity pv = null;
        if (pd.getCurrentversionId() != null) {
            pv = processversionRepository.findById(pd.getCurrentversionId()).orElse(null);
        }
        if (pv == null) {
            List<ProcessversionEntity> versions = processversionRepository
                .findByProcessdefIdOrderByVersionDesc(processdefId);
            if (!versions.isEmpty()) pv = versions.get(0);
        }
        UUID pvId = pv == null ? null : pv.getId();

        // Grafo (id→code para resolver source/target de las aristas)
        List<FlowelementEntity> feEntities = pvId == null ? List.of()
            : flowelementRepository.findByProcessversionIdOrderBySortOrder(pvId);
        Map<UUID, String> codeById = new HashMap<>();
        for (FlowelementEntity fe : feEntities) codeById.put(fe.getId(), fe.getElementCode());

        List<Map<String, Object>> flowElements = new ArrayList<>();
        for (FlowelementEntity fe : feEntities) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("code", fe.getElementCode());
            m.put("name", fe.getName());
            m.put("type", fe.getElementType());
            m.put("sortOrder", fe.getSortOrder() == null ? 0 : fe.getSortOrder());
            if (fe.getConfig() != null) m.put("config", parseJsonShallow(fe.getConfig()));
            flowElements.add(m);
        }

        List<Map<String, Object>> sequenceFlows = new ArrayList<>();
        if (pvId != null) {
            for (SequenceflowEntity sf : sequenceflowRepository.findByProcessversionIdOrderBySortOrder(pvId)) {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("sourceCode", codeById.get(sf.getSourceId()));
                m.put("targetCode", codeById.get(sf.getTargetId()));
                if (sf.getConditionExpr() != null) m.put("conditionExpr", sf.getConditionExpr());
                m.put("sortOrder", sf.getSortOrder() == null ? 0 : sf.getSortOrder());
                sequenceFlows.add(m);
            }
        }

        List<Map<String, Object>> taskForms = new ArrayList<>();
        if (!codeById.isEmpty()) {
            List<UUID> feIds = new ArrayList<>(codeById.keySet());
            for (TaskformEntity tf : taskformRepository.findByFlowelementIdIn(feIds)) {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("flowElementCode", codeById.get(tf.getFlowelementId()));
                // entityDefId es la FK local; entityDefCode se resuelve s2s (el builder trabaja por code).
                m.put("entityDefId", tf.getEntitydefId() == null ? null : tf.getEntitydefId().toString());
                m.put("entityDefCode", tf.getEntitydefId() == null ? null
                    : systemEntityResolver.resolveCode(tf.getEntitydefId()));
                m.put("mode", tf.getMode());
                taskForms.add(m);
            }
        }

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("processdefId", processdefId.toString());
        result.put("processversionId", pvId == null ? null : pvId.toString());
        result.put("header", Map.of(
            "code", pd.getCode() == null ? "" : pd.getCode(),
            "name", pd.getName() == null ? "" : pd.getName(),
            "description", pd.getDescription() == null ? "" : pd.getDescription(),
            "lifecycle", pd.getLifecycle() == null ? "active" : pd.getLifecycle(),
            "startPermission", pd.getStartPermission() == null ? "" : pd.getStartPermission()
        ));
        result.put("flowElements", flowElements);
        result.put("sequenceFlows", sequenceFlows);
        result.put("taskForms", taskForms);
        return result;
    }

    /**
     * Lista las versiones del processdef (processversionId, version, publishedAt,
     * isLocked, isCurrent), ordenadas por version ascendente.
     *
     * Los counts de instancias por versión (activeInstances/totalInstances) son
     * queries LOCALES en bpm (F4-mgmt disolvió el SQL cross-service que hacía system).
     */
    public List<Map<String, Object>> listVersions(UUID processdefId) {
        ProcessdefEntity pd = processdefRepository.findById(processdefId).orElse(null);
        UUID currentVerId = pd == null ? null : pd.getCurrentversionId();

        List<ProcessversionEntity> versions = processversionRepository
            .findByProcessdefIdOrderByVersionDesc(processdefId);
        // asc por version (el finder devuelve desc)
        versions.sort(Comparator.comparing(
            v -> v.getVersion() == null ? 0 : v.getVersion()));

        List<Map<String, Object>> out = new ArrayList<>(versions.size());
        for (ProcessversionEntity v : versions) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("processversionId", v.getId().toString());
            m.put("version", v.getVersion() == null ? 0 : v.getVersion());
            m.put("publishedAt", v.getPublishedAt());
            m.put("isLocked", v.isLocked());
            m.put("isCurrent", currentVerId != null && currentVerId.equals(v.getId()));
            // Contrato del frontend (ProcessdefVersionSummary): instancesActive/instancesTotal.
            m.put("instancesActive",
                processInstanceRepository.countByProcessversionIdAndLifecycle(v.getId(), "active"));
            m.put("instancesTotal",
                processInstanceRepository.countByProcessversionId(v.getId()));
            out.add(m);
        }
        return out;
    }

    // ════════════════════════════════════════════════════════════════════════
    //  Validación (portada fiel de system.ProcessdefAdminService)
    // ════════════════════════════════════════════════════════════════════════

    private void validateRequest(CreateProcessdefRequest req, boolean isUpdate, UUID tenantId) {
        // Header
        if (req.header() == null) {
            throw new IllegalArgumentException("header is required");
        }
        requireNonBlank("header.code", req.header().code());
        requireNonBlank("header.name", req.header().name());
        requireNonBlank("header.lifecycle", req.header().lifecycle());
        if (!List.of("active", "draft", "inactive").contains(req.header().lifecycle())) {
            throw new IllegalArgumentException("lifecycle must be one of: active, draft, inactive");
        }

        // Unicidad de code — solo en create
        if (!isUpdate
            && processdefRepository.findByTenantIdAndCode(tenantId, req.header().code()).isPresent()) {
            throw new IllegalArgumentException(
                "processdef with code '" + req.header().code() + "' already exists — "
                + "create a new one with a different code (v1-only in this iter)");
        }

        // Flow elements: required + tipos válidos + codes únicos
        if (req.flowElements() == null || req.flowElements().isEmpty()) {
            throw new IllegalArgumentException("at least one flow element is required");
        }
        Set<String> elementCodes = new HashSet<>();
        for (CreateProcessdefRequest.FlowElement fe : req.flowElements()) {
            requireNonBlank("flowElement.code", fe.code());
            requireNonBlank("flowElement.name", fe.name());
            requireNonBlank("flowElement.type", fe.type());
            if (!CreateProcessdefRequest.SUPPORTED_TYPES.contains(fe.type())) {
                throw new IllegalArgumentException("type '" + fe.type()
                    + "' not supported. Allowed: " + CreateProcessdefRequest.SUPPORTED_TYPES);
            }
            if (!elementCodes.add(fe.code())) {
                throw new IllegalArgumentException(
                    "duplicate flowElement code '" + fe.code() + "' within this processdef");
            }
        }

        // Topología: ≥1 start, ≥1 end
        long startCount = req.flowElements().stream().filter(fe -> "start_event".equals(fe.type())).count();
        long endCount   = req.flowElements().stream().filter(fe -> "end_event".equals(fe.type())).count();
        if (startCount < 1) {
            throw new IllegalArgumentException("processdef must have at least one start_event");
        }
        if (endCount < 1) {
            throw new IllegalArgumentException("processdef must have at least one end_event");
        }

        // Sequence flows: sourceCode/targetCode existen en flowElements
        if (req.sequenceFlows() != null) {
            for (CreateProcessdefRequest.SequenceFlow sf : req.sequenceFlows()) {
                requireNonBlank("sequenceFlow.sourceCode", sf.sourceCode());
                requireNonBlank("sequenceFlow.targetCode", sf.targetCode());
                if (!elementCodes.contains(sf.sourceCode())) {
                    throw new IllegalArgumentException(
                        "sourceCode '" + sf.sourceCode() + "' not in flowElements");
                }
                if (!elementCodes.contains(sf.targetCode())) {
                    throw new IllegalArgumentException(
                        "targetCode '" + sf.targetCode() + "' not in flowElements");
                }
                if (sf.sourceCode().equals(sf.targetCode())) {
                    throw new IllegalArgumentException(
                        "self-loop not allowed: source '" + sf.sourceCode() + "' = target");
                }
                if (sf.conditionExpr() != null && !sf.conditionExpr().isBlank()) {
                    validateConditionExprBasic(sf.sourceCode() + "→" + sf.targetCode(), sf.conditionExpr());
                }
            }
        }

        // Topología: reachability — todos alcanzables desde algún start
        validateReachability(req.flowElements(), req.sequenceFlows());

        // Task forms: flowElementCode existe + es user_task + entityDefCode existe (s2s)
        if (req.taskForms() != null) {
            Map<String, String> codeToType = new HashMap<>();
            for (CreateProcessdefRequest.FlowElement fe : req.flowElements()) {
                codeToType.put(fe.code(), fe.type());
            }
            for (CreateProcessdefRequest.TaskForm tf : req.taskForms()) {
                requireNonBlank("taskForm.flowElementCode", tf.flowElementCode());
                requireNonBlank("taskForm.entityDefCode",   tf.entityDefCode());
                String type = codeToType.get(tf.flowElementCode());
                if (type == null) {
                    throw new IllegalArgumentException(
                        "flowElementCode '" + tf.flowElementCode() + "' not in flowElements");
                }
                if (!"user_task".equals(type)) {
                    throw new IllegalArgumentException(
                        "taskForm can only bind to user_task — '" + tf.flowElementCode() + "' is " + type);
                }
                if (systemEntityResolver.resolveId(tf.entityDefCode(), tenantId) == null) {
                    throw new IllegalArgumentException(
                        "entityDef '" + tf.entityDefCode() + "' not found in system");
                }
            }
        }
    }

    private void validateReachability(List<CreateProcessdefRequest.FlowElement> elements,
                                      List<CreateProcessdefRequest.SequenceFlow> flows) {
        Set<String> starts = new HashSet<>();
        for (CreateProcessdefRequest.FlowElement fe : elements) {
            if ("start_event".equals(fe.type())) starts.add(fe.code());
        }
        Map<String, List<String>> outgoing = new HashMap<>();
        if (flows != null) {
            for (CreateProcessdefRequest.SequenceFlow sf : flows) {
                outgoing.computeIfAbsent(sf.sourceCode(), k -> new ArrayList<>()).add(sf.targetCode());
            }
        }
        Set<String> reachable = new HashSet<>(starts);
        Deque<String> queue = new ArrayDeque<>(starts);
        while (!queue.isEmpty()) {
            String node = queue.poll();
            for (String next : outgoing.getOrDefault(node, List.of())) {
                if (reachable.add(next)) queue.offer(next);
            }
        }
        for (CreateProcessdefRequest.FlowElement fe : elements) {
            if (!reachable.contains(fe.code())) {
                throw new IllegalArgumentException(
                    "flowElement '" + fe.code() + "' is not reachable from any start_event");
            }
        }
    }

    private void validateConditionExprBasic(String location, String expr) {
        // Validación básica: balance de paréntesis + longitud razonable.
        // JEXL parser real queda como mejora V2 (commons-jexl3 está en bpm).
        if (expr.length() > 1000) {
            throw new IllegalArgumentException("conditionExpr too long (>1000 chars) @" + location);
        }
        int balance = 0;
        for (int i = 0; i < expr.length(); i++) {
            char c = expr.charAt(i);
            if (c == '(') balance++;
            else if (c == ')') balance--;
            if (balance < 0) {
                throw new IllegalArgumentException(
                    "unbalanced parens in conditionExpr @" + location + ": '" + expr + "'");
            }
        }
        if (balance != 0) {
            throw new IllegalArgumentException(
                "unbalanced parens in conditionExpr @" + location + ": '" + expr + "'");
        }
    }

    // ════════════════════════════════════════════════════════════════════════
    //  Helpers
    // ════════════════════════════════════════════════════════════════════════

    private static void requireNonBlank(String field, String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("field '" + field + "' is required");
        }
    }

    private static String blankToNull(String s) {
        return (s == null || s.isBlank()) ? null : s;
    }

    /**
     * Serializa el config Map a JSON (columna jsonb de flowelement). null/vacío →
     * null (columna NULL). Usa Jackson (bpm ya lo tiene inyectado) para soportar
     * configs nested — a diferencia del serializador shallow de system.
     */
    private String serializeConfig(Map<String, Object> config) {
        if (config == null || config.isEmpty()) return null;
        try {
            return mapper.writeValueAsString(config);
        } catch (Exception e) {
            throw new IllegalArgumentException("could not serialize flowElement config: " + e.getMessage());
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> parseJsonShallow(String json) {
        if (json == null || json.isBlank()) return Map.of();
        try {
            return mapper.readValue(json, Map.class);
        } catch (Exception e) {
            return Map.of();
        }
    }

    /** Setea el núcleo audit-7 (bpm no setea GUC de user → explícito). */
    private void stampAudit(java.util.function.Consumer<UUID> setTenant,
                            java.util.function.Consumer<UUID> setState,
                            java.util.function.Consumer<UUID> setCreatedBy,
                            java.util.function.Consumer<UUID> setOwnedBy,
                            UUID tenantId, UUID userId) {
        setTenant.accept(tenantId);
        setState.accept(DEFAULT_STATE_ACTIVE);
        setCreatedBy.accept(userId);
        setOwnedBy.accept(userId);
    }
}
