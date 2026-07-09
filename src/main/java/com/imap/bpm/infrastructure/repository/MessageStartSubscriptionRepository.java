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

import com.imap.bpm.infrastructure.entity.MessageStartSubscriptionEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;
import java.util.UUID;

/**
 * Repository de MessageStartSubscriptionEntity.
 *
 * Patrón:
 *   - findActiveByTenantAndMessageCode: lookup principal del endpoint
 *     /v1/bpm/messages/start. Devuelve TODAS las subscriptions activas que
 *     matchean (puede ser >1 si N processdefs distintos suscritos al mismo
 *     messageCode → broadcast).
 *   - upsertActive: UPSERT atómico (ON CONFLICT) que reactiva o crea la
 *     subscription cuando el loader re-carga un processdef (race-free).
 *   - deactivateOldVersions: al subir una version nueva, marca inactivas las
 *     viejas del mismo processdef_id+message_code+tenant (V1 simple: solo
 *     current version dispara).
 */
public interface MessageStartSubscriptionRepository extends JpaRepository<MessageStartSubscriptionEntity, UUID> {

    @Query("""
        SELECT s FROM MessageStartSubscriptionEntity s
        WHERE s.tenantId = :tenantId
          AND s.messageCode = :messageCode
          AND s.active = true
        """)
    List<MessageStartSubscriptionEntity> findActiveByTenantAndMessageCode(
        @Param("tenantId") UUID tenantId,
        @Param("messageCode") String messageCode);

    /**
     * Marca inactivas todas las subscriptions del mismo (tenant, messageCode, processdefId)
     * que NO sean la processversion actual. Se llama al subir una version nueva.
     */
    @Modifying
    @Query("""
        UPDATE MessageStartSubscriptionEntity s
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

    /**
     * UPSERT atómico de la subscription (race-free). Reactiva la existente
     * (mismo unique tenant+message+version+startEvent) o la inserta si no existe.
     *
     * Reemplaza al patrón findForUpsert()+save() que NO era atómico: en cold-start
     * (cache vacío tras restart) dos threads cargaban el mismo processversion en
     * paralelo, ambos veían findForUpsert vacío e intentaban INSERT → el segundo
     * daba "duplicate key bpm_pro_msg_start_sub_unique" (no fatal pero ruidoso).
     * ON CONFLICT lo resuelve en el motor (nativo — no expresable en JPQL).
     */
    @Modifying
    @Query(value = """
        INSERT INTO bpm.bpm_pro_message_start_subscription_tbl
            (id, tenant_id, message_code, processdef_id, processversion_id,
             start_flow_element_id, is_active, state_id, created_at, updated_at)
        VALUES (:id, :tenantId, :messageCode, :processdefId, :processversionId,
                :startFlowElementId, true, :stateId, now(), now())
        ON CONFLICT (tenant_id, message_code, processversion_id, start_flow_element_id)
        DO UPDATE SET is_active = true, updated_at = now()
        """, nativeQuery = true)
    int upsertActive(
        @Param("id") UUID id,
        @Param("tenantId") UUID tenantId,
        @Param("messageCode") String messageCode,
        @Param("processdefId") UUID processdefId,
        @Param("processversionId") UUID processversionId,
        @Param("startFlowElementId") UUID startFlowElementId,
        @Param("stateId") UUID stateId);
}
