package com.imap.bpm.infrastructure.repository;

import com.imap.bpm.infrastructure.entity.MessageStartSubscription;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Repository de MessageStartSubscription.
 *
 * Patrón:
 *   - findActiveByTenantAndMessageCode: lookup principal del endpoint
 *     /v1/bpm/messages/start. Devuelve TODAS las subscriptions activas que
 *     matchean (puede ser >1 si N processdefs distintos suscritos al mismo
 *     messageCode → broadcast).
 *   - findExistingForUpsert: lookup por unique constraint para evitar duplicar
 *     subscription cuando el loader re-carga un processdef.
 *   - deactivateOldVersions: al subir una version nueva, marca inactivas las
 *     viejas del mismo processdef_id+message_code+tenant (V1 simple: solo
 *     current version dispara).
 */
public interface MessageStartSubscriptionRepository extends JpaRepository<MessageStartSubscription, UUID> {

    @Query("""
        SELECT s FROM MessageStartSubscription s
        WHERE s.tenantId = :tenantId
          AND s.messageCode = :messageCode
          AND s.active = true
        """)
    List<MessageStartSubscription> findActiveByTenantAndMessageCode(
        @Param("tenantId") UUID tenantId,
        @Param("messageCode") String messageCode);

    @Query("""
        SELECT s FROM MessageStartSubscription s
        WHERE s.tenantId = :tenantId
          AND s.messageCode = :messageCode
          AND s.processversionId = :processversionId
          AND s.startFlowElementId = :flowElementId
        """)
    Optional<MessageStartSubscription> findForUpsert(
        @Param("tenantId") UUID tenantId,
        @Param("messageCode") String messageCode,
        @Param("processversionId") UUID processversionId,
        @Param("flowElementId") UUID flowElementId);

    /**
     * Marca inactivas todas las subscriptions del mismo (tenant, messageCode, processdefId)
     * que NO sean la processversion actual. Se llama al subir una version nueva.
     */
    @Modifying
    @Query("""
        UPDATE MessageStartSubscription s
           SET s.active = false, s.updatedAt = CURRENT_TIMESTAMP
         WHERE s.tenantId = :tenantId
           AND s.messageCode = :messageCode
           AND s.processdefId = :processdefId
           AND s.processversionId <> :currentVersionId
           AND s.active = true
        """)
    int deactivateOldVersions(
        @Param("tenantId") UUID tenantId,
        @Param("messageCode") String messageCode,
        @Param("processdefId") UUID processdefId,
        @Param("currentVersionId") UUID currentVersionId);
}
