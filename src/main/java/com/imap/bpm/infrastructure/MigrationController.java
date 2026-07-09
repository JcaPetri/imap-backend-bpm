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

import com.imap.bpm.application.engine.MigrationApplyService;
import com.imap.platform.security.UserContext;
import com.imap.platform.security.UserContextHolder;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Endpoints del motor de migración multi-version (Hito 3, 2026-05-21).
 *
 *   POST /v1/bpm/migrations/apply/{planId}  → aplica plan a instances vivas de v1
 *
 * El management del plan (create/list/edit/validate) vive en bpm relacional
 * (F4-mgmt Chunk B) bajo /v1/bpm/admin/migration-plans — MigrationPlanAdminController.
 * Acá va el APPLY porque modifica tokens + processinstances + audit log de schema bpm.
 *
 * Permiso: requiere system.admin (mismo que el resto del admin BPM).
 * Por ahora el guard es implícito (el JWT debe tener system.admin para
 * llamar — el SYSTEM hace el check antes de validar). Iter futura: agregar
 * SecurityFilterChain explícito.
 */
@RestController
@RequestMapping("/v1/bpm/migrations")
public class MigrationController {

    private final MigrationApplyService applyService;

    public MigrationController(MigrationApplyService applyService) {
        this.applyService = applyService;
    }

    /**
     * Aplica un migration plan. Devuelve ApplyReport con stats:
     *   {instancesAffected, tokensMapped, tokensCancelled, errors[], warnings[]}
     *
     * Errores comunes:
     *   400 — plan no está en status 'validated' (validar primero via /v1/bpm/admin/migration-plans/{id}/validate)
     *   400 — plan ya está 'applied' (idempotency: re-apply rechazado)
     *   404 — plan no existe
     *   500 — falla parcial; el ApplyReport.errors trae detalle por instance
     *
     * El resultado (status applied/failed + appliedAt/appliedBy/stats) lo marca el
     * propio apply LOCAL via MigrationPlanManagementService.markApplyResult (F4-mgmt
     * Chunk B — antes era un callback s2s al SYSTEM). El frontend re-fetchea el plan
     * via /v1/bpm/admin/migration-plans/{id} para ver el status actualizado.
     */
    @PostMapping("/apply/{planId}")
    public ResponseEntity<MigrationApplyService.ApplyReport> applyPlan(
            @PathVariable("planId") String planIdStr) {
        UUID planId = UUID.fromString(planIdStr);
        UserContext user = UserContextHolder.get();
        UUID userId = user != null ? user.userId() : null;

        try {
            MigrationApplyService.ApplyReport report = applyService.applyPlan(planId, userId);
            return ResponseEntity.ok(report);
        } catch (IllegalStateException e) {
            // Plan no está validated, ya aplicado, etc.
            // Devolvemos 400 con un report-shaped body para que el frontend tenga shape consistente.
            MigrationApplyService.ApplyReport errReport = new MigrationApplyService.ApplyReport(
                0, 0, 0,
                java.util.List.of(e.getMessage()),
                java.util.List.of());
            return ResponseEntity.badRequest().body(errReport);
        }
    }
}
