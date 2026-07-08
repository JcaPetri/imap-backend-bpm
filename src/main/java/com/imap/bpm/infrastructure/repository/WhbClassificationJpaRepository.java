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

import com.imap.bpm.infrastructure.entity.WhbClassificationEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Clasificación G/U/T del WorkHub. Resolución (en ScoreService): override por
 * user_task (flowelement) si existe, si no la de nivel processdef. F0 trabaja a
 * nivel current (processversion_id NULL; el snapshot por versión se difiere).
 * Filtro por tenant explícito (RLS de bpm inerte hasta el sprint RLS-bpm-wide).
 */
public interface WhbClassificationJpaRepository extends JpaRepository<WhbClassificationEntity, UUID> {

    /** Clasificación a nivel processdef (current, sin versión, sin user_task). */
    Optional<WhbClassificationEntity>
        findByTenantIdAndProcessdefIdAndFlowelementIdIsNullAndProcessversionIdIsNull(
            UUID tenantId, UUID processdefId);

    /** Override a nivel user_task (current, sin versión). */
    Optional<WhbClassificationEntity>
        findByTenantIdAndProcessdefIdAndFlowelementIdAndProcessversionIdIsNull(
            UUID tenantId, UUID processdefId, UUID flowelementId);

    /** Listado para la pantalla admin (current, sin versión) del tenant. */
    List<WhbClassificationEntity> findByTenantIdAndProcessversionIdIsNull(UUID tenantId);
}
