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

package com.imap.bpm.infrastructure.repository;

import com.imap.bpm.infrastructure.entity.TenantProcessEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

/** Overlay de procesos por tenant (habilitación + config). Ver IMAP_BPM_PROCESS_CATALOG.md §1. */
public interface TenantProcessRepository extends JpaRepository<TenantProcessEntity, UUID> {

    Optional<TenantProcessEntity> findByTenantIdAndProcessdefCode(UUID tenantId, String processdefCode);

    List<TenantProcessEntity> findByTenantId(UUID tenantId);

    List<TenantProcessEntity> findByTenantIdAndEnabledTrue(UUID tenantId);

    /** ¿el tenant tiene alguna fila de overlay? (para el fallback backward-compat del startable). */
    boolean existsByTenantId(UUID tenantId);
}
