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

import com.imap.bpm.application.MigrationPlanManagementService;
import com.imap.bpm.domain.dto.MigrationPlanDto;
import com.imap.eav.engine.context.EavTenantSession;
import com.imap.platform.security.UserContext;
import com.imap.platform.security.UserContextHolder;
import com.imap.platform.tenant.TenantContextHolder;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;

/**
 * Endpoints de GESTIÓN (admin) de migration plans sobre las tablas RELACIONALES
 * de bpm (V015). Espejo de system.MigrationPlanController (F4-mgmt Chunk B): la
 * gestión de planes se portó de system (cell-store EAV) a bpm relacional.
 *
 *   POST /v1/bpm/admin/migration-plans              → create (+ auto-rules 1:1)
 *   GET  /v1/bpm/admin/migration-plans[?processdefId] → listado del tenant
 *   GET  /v1/bpm/admin/migration-plans/{id}         → detalle (header + rules)
 *   PUT  /v1/bpm/admin/migration-plans/{id}/rules   → reemplaza rules (reset a draft)
 *   POST /v1/bpm/admin/migration-plans/{id}/validate→ valida (instances activas LOCALES)
 *   POST /v1/bpm/admin/migration-plans/{id}/apply-status → marca applied/failed (callback del apply)
 *
 * Cada endpoint abre tx + aplica tenantSession en la primera línea (bpm setea el
 * GUC de tenant; el user_id se propaga explícito al service para el núcleo audit-7).
 * 404 si el plan no existe.
 */
@RestController
@RequestMapping("/v1/bpm/admin/migration-plans")
public class MigrationPlanAdminController {

    private final MigrationPlanManagementService service;
    private final EavTenantSession tenantSession;

    public MigrationPlanAdminController(MigrationPlanManagementService service,
                                        EavTenantSession tenantSession) {
        this.service = service;
        this.tenantSession = tenantSession;
    }

    /** Crea plan + auto-genera reglas 1:1 por code match entre source y target processversions. */
    @PostMapping
    @Transactional
    public ResponseEntity<MigrationPlanDto.PlanDetail> create(
            @RequestBody MigrationPlanDto.CreatePlanRequest req) {
        tenantSession.applyToCurrentTransaction();
        UUID tenantId = TenantContextHolder.get();
        UUID userId = currentUserId();
        MigrationPlanDto.PlanDetail detail = service.createPlan(req, tenantId, userId);
        return ResponseEntity.status(HttpStatus.CREATED).body(detail);
    }

    /** Lista planes del tenant. Filter opcional ?processdefId=X. */
    @GetMapping
    @Transactional(readOnly = true)
    public List<MigrationPlanDto.PlanSummary> list(
            @RequestParam(required = false) String processdefId) {
        tenantSession.applyToCurrentTransaction();
        UUID tenantId = TenantContextHolder.get();
        return service.listPlans(tenantId, processdefId);
    }

    /** Detail completo del plan: header + rules. */
    @GetMapping("/{id}")
    @Transactional(readOnly = true)
    public ResponseEntity<MigrationPlanDto.PlanDetail> getDetail(@PathVariable("id") String id) {
        tenantSession.applyToCurrentTransaction();
        try {
            return ResponseEntity.ok(service.getPlanDetail(UUID.fromString(id)));
        } catch (NoSuchElementException e) {
            return ResponseEntity.notFound().build();
        }
    }

    /** Update FULL de las rules. Reset status a 'draft' (requiere re-validar). */
    @PutMapping("/{id}/rules")
    @Transactional
    public ResponseEntity<MigrationPlanDto.PlanDetail> updateRules(
            @PathVariable("id") String id,
            @RequestBody MigrationPlanDto.UpdateRulesRequest req) {
        tenantSession.applyToCurrentTransaction();
        UUID tenantId = TenantContextHolder.get();
        UUID userId = currentUserId();
        try {
            return ResponseEntity.ok(service.updateRules(UUID.fromString(id), req, tenantId, userId));
        } catch (NoSuchElementException e) {
            return ResponseEntity.notFound().build();
        }
    }

    /** Valida el plan contra instances activas LOCALES de la source processversion. */
    @PostMapping("/{id}/validate")
    @Transactional
    public ResponseEntity<MigrationPlanDto.ValidationReport> validate(@PathVariable("id") String id) {
        tenantSession.applyToCurrentTransaction();
        UUID userId = currentUserId();
        try {
            return ResponseEntity.ok(service.validatePlan(UUID.fromString(id), userId));
        } catch (NoSuchElementException e) {
            return ResponseEntity.notFound().build();
        }
    }

    /**
     * Marca el plan como 'applied' o 'failed' + appliedAt/appliedBy/stats. Antes era
     * un callback s2s desde bpm→system; ahora la gestión vive en bpm, así que lo puede
     * invocar el propio apply local (o el frontend admin para re-marcar).
     * Body: {"appliedBy":"uuid-or-null", "stats":"json-string", "success":true}
     */
    @PostMapping("/{id}/apply-status")
    @Transactional
    public ResponseEntity<MigrationPlanDto.PlanSummary> markApplyResult(
            @PathVariable("id") String id,
            @RequestBody Map<String, Object> body) {
        tenantSession.applyToCurrentTransaction();
        try {
            UUID planId = UUID.fromString(id);
            String appliedBy = body == null ? null : (String) body.get("appliedBy");
            String statsJson = body == null ? null : (String) body.get("stats");
            boolean success = body != null && Boolean.TRUE.equals(body.get("success"));
            return ResponseEntity.ok(service.markApplyResult(planId, appliedBy, statsJson, success));
        } catch (NoSuchElementException e) {
            return ResponseEntity.notFound().build();
        }
    }

    private static UUID currentUserId() {
        UserContext user = UserContextHolder.get();
        return user != null ? user.userId() : null;
    }
}
