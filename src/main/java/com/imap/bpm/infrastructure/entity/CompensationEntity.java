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
//  • [persistence] Audit7: tenant_id|state_id|created_at|created_by_id|updated_at|updated_by_id|owned_by_id
//  • [persistence] RLS por tenant + tenant_id en toda tabla
//  • [persistence] Soft-delete por state (archived), nunca DELETE físico
//  • [persistence] Naming SQL Opción B (id PK · _id FKs · _at timestamps · is_* booleans)
//  • [persistence] Flyway: cambios de schema versionados, nunca DDL ad-hoc en prod
//  • [persistence] Native queries: CAST(x AS t), NO x::t (Hibernate confunde :: con bind param)
//  • [bpm] Orquestador central (tipo Temporal/Step Functions)
//  • [bpm] processdef→processversion: snapshot inmutable al activar
//  • [bpm] Las defs de proceso viven acá (no en system)
//  • [bpm] Camunda 8 como norte; interim form-driven en prod
// ─── GOLDEN-RULES:END ───

package com.imap.bpm.infrastructure.entity;

import jakarta.persistence.*;

import java.time.OffsetDateTime;
import java.util.Map;
import java.util.UUID;

/**
 * JPA entity para bpm.bpm_pro_compensation_tbl.
 *
 * Registro de una activity compensable YA completada (patron Saga). Cuando un
 * service_task compensable completa OK, se registra una fila con snapshot de
 * variables; al dispararse la compensacion (end_event compensate=true) se
 * ejecutan los handlers en LIFO (completion_order DESC = inverso a la
 * completacion).
 *
 * completed_element_id     -> el activity original (flow_element) que se completo
 * compensation_element_id  -> el flow_element handler (service_task off-path) que lo deshace
 */
@Entity
@Table(name = "bpm_pro_compensation_tbl")
public class CompensationEntity {

    @Id
    @Column(name = "id")
    private UUID id;

    @Column(name = "tenant_id", nullable = false)
    private UUID tenantId;

    @Column(name = "processinstance_id", nullable = false)
    private UUID processinstanceId;

    @Column(name = "completed_element_id", nullable = false)
    private UUID completedElementId;

    @Column(name = "compensation_element_id", nullable = false)
    private UUID compensationElementId;

    @Column(name = "completion_order", nullable = false)
    private Integer completionOrder;

    @Column(name = "completion_data_jsonb", columnDefinition = "jsonb")
    private Map<String, Object> completionData;

    @Column(name = "lifecycle", nullable = false, length = 20)
    private String lifecycle;     // registered | compensating | compensated | failed | cancelled

    @Column(name = "compensated_at")
    private OffsetDateTime compensatedAt;

    @Column(name = "state_id", nullable = false)        private UUID stateId;
    @Column(name = "created_at", nullable = false, updatable = false)      private OffsetDateTime createdAt;
    @Column(name = "updated_at", nullable = false)      private OffsetDateTime updatedAt;
    @Column(name = "created_by_id", updatable = false)                     private UUID createdById;
    @Column(name = "updated_by_id")                     private UUID updatedById;
    @Column(name = "owned_by_id", updatable = false)                       private UUID ownedById;

    public CompensationEntity() {}

    public UUID getId()                         { return id; }
    public void setId(UUID id)                  { this.id = id; }
    public UUID getTenantId()                   { return tenantId; }
    public void setTenantId(UUID id)            { this.tenantId = id; }
    public UUID getProcessinstanceId()          { return processinstanceId; }
    public void setProcessinstanceId(UUID id)   { this.processinstanceId = id; }
    public UUID getCompletedElementId()         { return completedElementId; }
    public void setCompletedElementId(UUID id)  { this.completedElementId = id; }
    public UUID getCompensationElementId()      { return compensationElementId; }
    public void setCompensationElementId(UUID id){ this.compensationElementId = id; }
    public Integer getCompletionOrder()         { return completionOrder; }
    public void setCompletionOrder(Integer o)   { this.completionOrder = o; }
    public Map<String, Object> getCompletionData()          { return completionData; }
    public void setCompletionData(Map<String, Object> m)    { this.completionData = m; }
    public String getLifecycle()                { return lifecycle; }
    public void setLifecycle(String s)          { this.lifecycle = s; }
    public OffsetDateTime getCompensatedAt()    { return compensatedAt; }
    public void setCompensatedAt(OffsetDateTime t){ this.compensatedAt = t; }

    public UUID getStateId()                    { return stateId; }
    public void setStateId(UUID id)             { this.stateId = id; }
    public OffsetDateTime getCreatedAt()        { return createdAt; }
    public void setCreatedAt(OffsetDateTime t)  { this.createdAt = t; }
    public OffsetDateTime getUpdatedAt()        { return updatedAt; }
    public void setUpdatedAt(OffsetDateTime t)  { this.updatedAt = t; }
    public UUID getCreatedById()                { return createdById; }
    public void setCreatedById(UUID id)         { this.createdById = id; }
    public UUID getUpdatedById()                { return updatedById; }
    public void setUpdatedById(UUID id)         { this.updatedById = id; }
    public UUID getOwnedById()                  { return ownedById; }
    public void setOwnedById(UUID id)           { this.ownedById = id; }
}
