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

import com.imap.bpm.infrastructure.entity.ProcessInstance;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface ProcessInstanceRepository extends JpaRepository<ProcessInstance, UUID> {
    List<ProcessInstance> findByTenantIdAndLifecycle(UUID tenantId, String lifecycle);
    List<ProcessInstance> findByProcessdefIdAndLifecycle(UUID processdefId, String lifecycle);
    List<ProcessInstance> findByParentInstanceId(UUID parentInstanceId);

    /** Para listado admin (cleanup script). */
    List<ProcessInstance> findByProcessdefIdOrderByStartedAtDesc(UUID processdefId);

    /** Hito 3 — migration: instances vivas en una processversion source. */
    List<ProcessInstance> findByProcessversionIdAndLifecycle(UUID processversionId, String lifecycle);

    // ── Counts LOCALES (F4-mgmt: disuelven el SQL cross-service que hacía system a bpm) ──
    /** Count de instances de una processversion en un lifecycle dado (ej. 'active'). */
    long countByProcessversionIdAndLifecycle(UUID processversionId, String lifecycle);
    /** Count de instances de un processdef en un lifecycle dado (guard G1 del update). */
    long countByProcessdefIdAndLifecycle(UUID processdefId, String lifecycle);
    /** Count total de instances (todos los lifecycles) de una processversion. */
    long countByProcessversionId(UUID processversionId);
}
