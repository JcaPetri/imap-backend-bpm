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

import com.imap.bpm.infrastructure.entity.TenantProcessEntity;
import com.imap.bpm.infrastructure.repository.TenantProcessRepository;
import org.springframework.stereotype.Service;

import java.time.OffsetDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Overlay de procesos por tenant (gemelo de la adopción/parametrización del plan de cuentas).
 * Nivel 1 (habilitar) + Nivel 2 (config). El controller abre la tx + aplica tenantSession.
 * Ver IMAP_BPM_PROCESS_CATALOG.md §1.
 */
@Service
public class TenantProcessService {

    private static final UUID DEFAULT_STATE_ACTIVE =
        UUID.fromString("019dc6f0-aa83-7333-8888-000000000001");

    private final TenantProcessRepository repo;

    public TenantProcessService(TenantProcessRepository repo) { this.repo = repo; }

    /** Habilita un proceso del catálogo para el tenant (Nivel 1). Idempotente. */
    public TenantProcessEntity enable(UUID tenantId, String code, UUID userId) {
        return upsert(tenantId, code, userId, true, null, false);
    }

    /** Deshabilita (soft — conserva la config). */
    public TenantProcessEntity disable(UUID tenantId, String code, UUID userId) {
        TenantProcessEntity e = repo.findByTenantIdAndProcessdefCode(tenantId, code).orElse(null);
        if (e == null) return null;
        e.setEnabled(false);
        e.setUpdatedAt(OffsetDateTime.now());
        e.setUpdatedById(userId);
        return repo.save(e);
    }

    /** Setea la config overlay (Nivel 2). Crea la fila (habilitada) si no existe. */
    public TenantProcessEntity setConfig(UUID tenantId, String code, Map<String, Object> config, UUID userId) {
        return upsert(tenantId, code, userId, true, config, true);
    }

    private TenantProcessEntity upsert(UUID tenantId, String code, UUID userId,
                                       boolean enabled, Map<String, Object> config, boolean applyConfig) {
        OffsetDateTime now = OffsetDateTime.now();
        TenantProcessEntity e = repo.findByTenantIdAndProcessdefCode(tenantId, code).orElse(null);
        if (e == null) {
            e = new TenantProcessEntity();
            e.setId(UUID.randomUUID());
            e.setTenantId(tenantId);
            e.setProcessdefCode(code);
            e.setStateId(DEFAULT_STATE_ACTIVE);
            e.setCreatedAt(now);
            e.setCreatedById(userId);
            e.setEnabled(enabled);
            if (applyConfig) e.setConfig(config);
        } else {
            e.setEnabled(enabled);
            if (applyConfig) e.setConfig(config);
        }
        e.setUpdatedAt(now);
        e.setUpdatedById(userId);
        return repo.save(e);
    }

    /** Lista el overlay del tenant (code, enabled, config). */
    public List<Map<String, Object>> list(UUID tenantId) {
        return repo.findByTenantId(tenantId).stream().map(e -> {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("processdefCode", e.getProcessdefCode());
            m.put("enabled", e.isEnabled());
            m.put("config", e.getConfig() == null ? Map.of() : e.getConfig());
            return m;
        }).collect(Collectors.toList());
    }

    /** Config overlay de un proceso habilitado (para inyectar como variable 'config' al arrancar). */
    public Map<String, Object> configFor(UUID tenantId, String code) {
        return repo.findByTenantIdAndProcessdefCode(tenantId, code)
            .filter(TenantProcessEntity::isEnabled)
            .map(TenantProcessEntity::getConfig)
            .orElse(null);
    }

    /** Codes habilitados por el tenant (para filtrar el catálogo startable). */
    public Set<String> enabledCodes(UUID tenantId) {
        return repo.findByTenantIdAndEnabledTrue(tenantId).stream()
            .map(TenantProcessEntity::getProcessdefCode).collect(Collectors.toSet());
    }

    /** ¿el tenant adoptó el overlay? (si no, el startable cae al comportamiento previo). */
    public boolean hasOverlay(UUID tenantId) { return repo.existsByTenantId(tenantId); }
}
