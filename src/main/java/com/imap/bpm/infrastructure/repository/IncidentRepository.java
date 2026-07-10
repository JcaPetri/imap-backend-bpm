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

import com.imap.bpm.infrastructure.entity.IncidentEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface IncidentRepository extends JpaRepository<IncidentEntity, UUID> {

    /** Incidentes de un lifecycle (ej. 'open') del tenant, mas reciente primero. */
    List<IncidentEntity> findByTenantIdAndLifecycleOrderByCreatedAtDesc(UUID tenantId, String lifecycle);

    /** Todos los del tenant (para el panel con filtro), mas reciente primero. */
    List<IncidentEntity> findByTenantIdOrderByCreatedAtDesc(UUID tenantId);

    /** Para cascade DELETE de instance (admin cleanup). */
    long deleteByProcessinstanceId(UUID processinstanceId);

    /**
     * Encarnación de un dispatch de service_task = cantidad de incidents ya abiertos
     * para este (token, elemento). Semilla de la idempotency-key determinística: crece
     * en cada incident-retry (key nueva → re-ejecuta) pero es estable ante crash/restart
     * (un crash no abre incident → misma key → el receptor deduplica).
     */
    long countByTokenIdAndElementId(UUID tokenId, UUID elementId);
}
