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
 *   POST /v1/bpm/admin/processdef              → create (+ v1 + grafo, atómico)
 *   GET  /v1/bpm/admin/processdef              → listAll del tenant
 *   GET  /v1/bpm/admin/processdef/{id}         → detalle completo (shape builder)
 *   GET  /v1/bpm/admin/processdef/{id}/versions→ versiones del processdef
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
}
