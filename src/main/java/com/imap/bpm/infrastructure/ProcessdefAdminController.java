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
//  • [controller] DTOs, nunca exponer entidades del domain en la API
//  • [bpm] Orquestador central (tipo Temporal/Step Functions)
//  • [bpm] processdef→processversion: snapshot inmutable al activar
//  • [bpm] Las defs de proceso viven acá (no en system)
//  • [bpm] Camunda 8 como norte; interim form-driven en prod
// ─── GOLDEN-RULES:END ───

package com.imap.bpm.infrastructure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.imap.bpm.application.ProcessdefManagementService;
import com.imap.bpm.application.engine.DecisionDefinitionLoader;
import com.imap.bpm.domain.dto.CreateProcessdefRequest;
import com.imap.bpm.domain.dto.CreateProcessdefResponse;
import com.imap.bpm.infrastructure.entity.DecisiondefEntity;
import com.imap.bpm.infrastructure.repository.DecisiondefRepository;
import com.imap.bpm.infrastructure.repository.DmnRuleRepository;
import com.imap.eav.engine.context.EavTenantSession;
import org.springframework.jdbc.core.JdbcTemplate;
import com.imap.platform.security.UserContext;
import com.imap.platform.security.UserContextHolder;
import com.imap.platform.tenant.TenantContextHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;

import java.time.OffsetDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Endpoints de GESTIÓN (admin) de processdefs sobre las tablas RELACIONALES de
 * bpm (V015). Reemplaza al admin EAV de system (Fase 4-mgmt de
 * IMAP_BPM_OWNERSHIP_MIGRATION.md). Aditivo — no toca los endpoints existentes
 * de BpmProcessController.
 *
 *   POST   /v1/bpm/admin/processdef              → create (+ v1 + grafo, atómico)
 *   GET    /v1/bpm/admin/processdef              → listAll del tenant
 *   GET    /v1/bpm/admin/processdef/{id}         → detalle completo (shape builder)
 *   GET    /v1/bpm/admin/processdef/{id}/versions→ versiones del processdef (+ instance counts locales)
 *   PUT    /v1/bpm/admin/processdef/{id}         → update in-place de la version vigente (409 si active)
 *   DELETE /v1/bpm/admin/processdef/{id}         → soft-delete (lifecycle=inactive)
 *   POST   /v1/bpm/admin/processdef/{id}/versions→ publish nueva version (v=N+1, non-destructive)
 *
 * Cada endpoint abre tx + aplica tenantSession en la primera línea (bpm setea el
 * GUC de tenant; el user_id se propaga explícito al service para el núcleo audit-7).
 */
@RestController
@RequestMapping("/v1/bpm/admin")
public class ProcessdefAdminController {

    private static final Logger log = LoggerFactory.getLogger(ProcessdefAdminController.class);

    private static final UUID DEFAULT_STATE_ACTIVE =
        UUID.fromString("019dc6f0-aa83-7333-8888-000000000001");

    private final ProcessdefManagementService service;
    private final EavTenantSession tenantSession;
    private final DecisiondefRepository decisiondefRepo;
    private final DmnRuleRepository dmnRuleRepo;
    private final DecisionDefinitionLoader decisionLoader;
    private final ObjectMapper jackson;
    private final JdbcTemplate jdbc;

    public ProcessdefAdminController(ProcessdefManagementService service,
                                     EavTenantSession tenantSession,
                                     DecisiondefRepository decisiondefRepo,
                                     DmnRuleRepository dmnRuleRepo,
                                     DecisionDefinitionLoader decisionLoader,
                                     ObjectMapper jackson,
                                     JdbcTemplate jdbc) {
        this.service = service;
        this.tenantSession = tenantSession;
        this.decisiondefRepo = decisiondefRepo;
        this.dmnRuleRepo = dmnRuleRepo;
        this.decisionLoader = decisionLoader;
        this.jackson = jackson;
        this.jdbc = jdbc;
    }

    // ── DMN decision authoring (Ola 7.1 — antes solo via SQL directo) ───────────

    /**
     * Crea una decisiondef + sus rules. Body (JSON en formato var_name, tal como lo lee
     * el LocalDefinitionReader): {code, name, description, hitPolicy, inputSchema[], outputSchema[],
     * requiredDecisions[], rules:[{priority, inputs[], outputs[], description}]}.
     */
    @PostMapping("/decisiondef")
    @Transactional
    @SuppressWarnings("unchecked")
    public ResponseEntity<Map<String, Object>> createDecision(@RequestBody Map<String, Object> body) {
        tenantSession.applyToCurrentTransaction();
        UUID tenantId = TenantContextHolder.get();
        String code = body == null ? null : (String) body.get("code");
        if (code == null || code.isBlank()) {
            return ResponseEntity.badRequest().body(Map.of("error", "code is required"));
        }

        // JdbcTemplate + CAST(? AS jsonb): las columnas JSON no aceptan bind de varchar
        // (Hibernate bindea String→varchar). Los by-id del núcleo audit-7 los llena el
        // trigger fn_fill_nucleo; created_at/updated_at tienen DEFAULT now().
        UUID ddId = UUID.randomUUID();
        jdbc.update(
            "INSERT INTO bpm.bpm_dmn_decisiondef_tbl " +
            "(id, tenant_id, code, name, description, hit_policy, input_schema, output_schema, required_decisions, state_id) " +
            "VALUES (?, ?, ?, ?, ?, ?, CAST(? AS jsonb), CAST(? AS jsonb), CAST(? AS jsonb), ?)",
            ddId, tenantId, code, str(body.get("name"), code), str(body.get("description"), null),
            str(body.get("hitPolicy"), "first"),
            toJson(body.get("inputSchema")), toJson(body.get("outputSchema")), toJson(body.get("requiredDecisions")),
            DEFAULT_STATE_ACTIVE);

        int ruleCount = 0;
        if (body.get("rules") instanceof List<?> rules) {
            int prio = 1;
            for (Object rObj : rules) {
                if (!(rObj instanceof Map)) continue;
                Map<String, Object> r = (Map<String, Object>) rObj;
                Object p = r.get("priority");
                int priority = p instanceof Number n ? n.intValue() : prio;
                jdbc.update(
                    "INSERT INTO bpm.bpm_dmn_rule_tbl " +
                    "(id, tenant_id, decisiondef_id, priority, inputs, outputs, description, state_id) " +
                    "VALUES (?, ?, ?, ?, CAST(? AS jsonb), CAST(? AS jsonb), ?, ?)",
                    UUID.randomUUID(), tenantId, ddId, priority,
                    toJson(r.get("inputs")), toJson(r.get("outputs")), str(r.get("description"), null),
                    DEFAULT_STATE_ACTIVE);
                ruleCount++;
                prio++;
            }
        }
        decisionLoader.invalidate(code);   // por si estaba cacheada
        log.info("createDecision '{}' (rules={}, requiredDecisions={})", code, ruleCount, body.get("requiredDecisions"));
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("id", ddId.toString());
        out.put("code", code);
        out.put("rules", ruleCount);
        return ResponseEntity.ok(out);
    }

    /** Borra una decisiondef + sus rules (autoría/cleanup). */
    @DeleteMapping("/decisiondef/{code}")
    @Transactional
    public ResponseEntity<Map<String, Object>> deleteDecision(@PathVariable("code") String code) {
        tenantSession.applyToCurrentTransaction();
        UUID tenantId = TenantContextHolder.get();
        DecisiondefEntity dd = decisiondefRepo.findByTenantIdAndCode(tenantId, code).orElse(null);
        if (dd == null) return ResponseEntity.notFound().build();
        long rules = dmnRuleRepo.deleteByDecisiondefId(dd.getId());
        decisiondefRepo.delete(dd);
        decisionLoader.invalidate(code);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("code", code);
        out.put("deleted", true);
        out.put("rules", rules);
        return ResponseEntity.ok(out);
    }

    private String toJson(Object o) {
        if (o == null) return null;
        try { return jackson.writeValueAsString(o); } catch (Exception e) { return null; }
    }

    private static String str(Object o, String def) { return o == null ? def : o.toString(); }

    @PostMapping("/processdef")
    @Transactional
    public ResponseEntity<CreateProcessdefResponse> create(@RequestBody CreateProcessdefRequest req) {
        tenantSession.applyToCurrentTransaction();
        UUID tenantId = TenantContextHolder.get();
        UserContext user = UserContextHolder.get();
        UUID userId = user != null ? user.userId() : null;
        CreateProcessdefResponse resp = service.create(req, tenantId, userId);
        return ResponseEntity.ok(resp);
    }

    @GetMapping("/processdef")
    @Transactional(readOnly = true)
    public List<Map<String, Object>> listAll() {
        tenantSession.applyToCurrentTransaction();
        UUID tenantId = TenantContextHolder.get();
        return service.listAll(tenantId);
    }

    @GetMapping("/processdef/{id}")
    @Transactional(readOnly = true)
    public ResponseEntity<Map<String, Object>> getDetail(@PathVariable("id") String id) {
        tenantSession.applyToCurrentTransaction();
        Map<String, Object> detail = service.getDetail(UUID.fromString(id));
        if (detail == null) return ResponseEntity.notFound().build();
        return ResponseEntity.ok(detail);
    }

    @GetMapping("/processdef/{id}/versions")
    @Transactional(readOnly = true)
    public List<Map<String, Object>> listVersions(@PathVariable("id") String id) {
        tenantSession.applyToCurrentTransaction();
        return service.listVersions(UUID.fromString(id));
    }

    /**
     * Update in-place del processdef + shape de la version vigente. 404 si no existe,
     * 409 (IllegalStateException del service, vía GlobalExceptionHandler) si hay
     * instances activas, 400 si la validación cruzada falla.
     */
    @PutMapping("/processdef/{id}")
    @Transactional
    public ResponseEntity<CreateProcessdefResponse> update(@PathVariable("id") String id,
                                                           @RequestBody CreateProcessdefRequest req) {
        tenantSession.applyToCurrentTransaction();
        UUID tenantId = TenantContextHolder.get();
        UserContext user = UserContextHolder.get();
        UUID userId = user != null ? user.userId() : null;
        try {
            CreateProcessdefResponse resp = service.update(UUID.fromString(id), req, tenantId, userId);
            return ResponseEntity.ok(resp);
        } catch (IllegalArgumentException ex) {
            if (isNotFound(ex)) return ResponseEntity.notFound().build();
            throw ex;   // otras validaciones → 400 vía GlobalExceptionHandler
        }
    }

    /**
     * Soft-delete: lifecycle=inactive. 204 No Content si OK, 404 si no existe.
     */
    @DeleteMapping("/processdef/{id}")
    @Transactional
    public ResponseEntity<Void> softDelete(@PathVariable("id") String id) {
        tenantSession.applyToCurrentTransaction();
        UserContext user = UserContextHolder.get();
        UUID userId = user != null ? user.userId() : null;
        try {
            service.softDelete(UUID.fromString(id), userId);
            return ResponseEntity.noContent().build();
        } catch (IllegalArgumentException ex) {
            if (isNotFound(ex)) return ResponseEntity.notFound().build();
            throw ex;
        }
    }

    /**
     * Publica una nueva version (v=N+1) non-destructive. 404 si no existe,
     * 400 si la validación cruzada falla.
     */
    @PostMapping("/processdef/{id}/versions")
    @Transactional
    public ResponseEntity<CreateProcessdefResponse> publishNewVersion(@PathVariable("id") String id,
                                                                      @RequestBody CreateProcessdefRequest req) {
        tenantSession.applyToCurrentTransaction();
        UUID tenantId = TenantContextHolder.get();
        UserContext user = UserContextHolder.get();
        UUID userId = user != null ? user.userId() : null;
        try {
            CreateProcessdefResponse resp = service.publishNewVersion(UUID.fromString(id), req, tenantId, userId);
            return ResponseEntity.ok(resp);
        } catch (IllegalArgumentException ex) {
            if (isNotFound(ex)) return ResponseEntity.notFound().build();
            throw ex;
        }
    }

    /** El service tira IllegalArgumentException("ProcessdefEntity not found: ...") cuando no existe → 404. */
    private static boolean isNotFound(IllegalArgumentException ex) {
        return ex.getMessage() != null && ex.getMessage().startsWith("ProcessdefEntity not found");
    }
}
