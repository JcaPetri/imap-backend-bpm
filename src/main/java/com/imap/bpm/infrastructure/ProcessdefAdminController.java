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

import com.imap.bpm.application.ProcessdefManagementService;
import com.imap.bpm.domain.dto.CreateProcessdefRequest;
import com.imap.bpm.domain.dto.CreateProcessdefResponse;
import com.imap.eav.engine.context.EavTenantSession;
import com.imap.platform.security.UserContext;
import com.imap.platform.security.UserContextHolder;
import com.imap.platform.tenant.TenantContextHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;

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

    private final ProcessdefManagementService service;
    private final EavTenantSession tenantSession;

    public ProcessdefAdminController(ProcessdefManagementService service,
                                     EavTenantSession tenantSession) {
        this.service = service;
        this.tenantSession = tenantSession;
    }

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
