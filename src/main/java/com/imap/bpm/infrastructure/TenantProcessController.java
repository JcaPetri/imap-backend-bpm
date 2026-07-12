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

import com.imap.bpm.application.TenantProcessService;
import com.imap.eav.engine.context.EavTenantSession;
import com.imap.platform.security.UserContext;
import com.imap.platform.security.UserContextHolder;
import com.imap.platform.tenant.TenantContextHolder;
import org.springframework.http.ResponseEntity;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Overlay de procesos por tenant (gemelo del plan de cuentas per-tenant).
 *   POST   /v1/bpm/tenant-process/{code}/enable   → Nivel 1: habilitar el proceso del catálogo
 *   DELETE /v1/bpm/tenant-process/{code}          → deshabilitar (soft, conserva config)
 *   PUT    /v1/bpm/tenant-process/{code}/config   → Nivel 2: setear la config overlay (body = map)
 *   GET    /v1/bpm/tenant-process                 → listar el overlay del tenant
 * Ver IMAP_BPM_PROCESS_CATALOG.md §1.
 */
@RestController
@RequestMapping("/v1/bpm/tenant-process")
public class TenantProcessController {

    private final TenantProcessService service;
    private final EavTenantSession tenantSession;

    public TenantProcessController(TenantProcessService service, EavTenantSession tenantSession) {
        this.service = service;
        this.tenantSession = tenantSession;
    }

    @PostMapping("/{code}/enable")
    @Transactional
    public ResponseEntity<Map<String, Object>> enable(@PathVariable("code") String code) {
        tenantSession.applyToCurrentTransaction();
        service.enable(TenantContextHolder.get(), code, userId());
        return ResponseEntity.ok(Map.of("processdefCode", code, "enabled", true));
    }

    @DeleteMapping("/{code}")
    @Transactional
    public ResponseEntity<Map<String, Object>> disable(@PathVariable("code") String code) {
        tenantSession.applyToCurrentTransaction();
        if (service.disable(TenantContextHolder.get(), code, userId()) == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(Map.of("processdefCode", code, "enabled", false));
    }

    @PutMapping("/{code}/config")
    @Transactional
    @SuppressWarnings("unchecked")
    public ResponseEntity<Map<String, Object>> setConfig(@PathVariable("code") String code,
                                                         @RequestBody(required = false) Map<String, Object> config) {
        tenantSession.applyToCurrentTransaction();
        service.setConfig(TenantContextHolder.get(), code, config == null ? Map.of() : config, userId());
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("processdefCode", code);
        out.put("config", config == null ? Map.of() : config);
        return ResponseEntity.ok(out);
    }

    @GetMapping
    @Transactional(readOnly = true)
    public List<Map<String, Object>> list() {
        tenantSession.applyToCurrentTransaction();
        return service.list(TenantContextHolder.get());
    }

    private static UUID userId() {
        UserContext u = UserContextHolder.get();
        return u != null ? u.userId() : null;
    }
}
