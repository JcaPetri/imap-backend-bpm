package com.imap.bpm.infrastructure.repository;

import com.imap.bpm.infrastructure.entity.WhbClassification;
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
public interface WhbClassificationRepository extends JpaRepository<WhbClassification, UUID> {

    /** Clasificación a nivel processdef (current, sin versión, sin user_task). */
    Optional<WhbClassification>
        findByTenantIdAndProcessdefIdAndFlowelementIdIsNullAndProcessversionIdIsNull(
            UUID tenantId, UUID processdefId);

    /** Override a nivel user_task (current, sin versión). */
    Optional<WhbClassification>
        findByTenantIdAndProcessdefIdAndFlowelementIdAndProcessversionIdIsNull(
            UUID tenantId, UUID processdefId, UUID flowelementId);

    /** Listado para la pantalla admin (current, sin versión) del tenant. */
    List<WhbClassification> findByTenantIdAndProcessversionIdIsNull(UUID tenantId);
}
