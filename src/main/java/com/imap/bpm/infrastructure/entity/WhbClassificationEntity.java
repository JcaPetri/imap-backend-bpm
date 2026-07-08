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
import java.util.UUID;

/**
 * JPA entity para bpm.bpm_whb_classification_tbl — valores Gravedad/Urgencia/
 * Tendencia por processdef (flowelementId NULL) o por user_task (flowelementId
 * NOT NULL). processversionId NULL = fila draft/current a nivel processdef.
 * Ver docs/architecture/workhub-northstar.md §5.1/§5.4.
 */
@Entity
@Table(name = "bpm_whb_classification_tbl")
public class WhbClassificationEntity {

    @Id @Column(name = "id")                          private UUID id;
    @Column(name = "tenant_id", nullable = false)    private UUID tenantId;
    @Column(name = "processdef_id", nullable = false) private UUID processdefId;
    @Column(name = "processversion_id")              private UUID processversionId;
    @Column(name = "flowelement_id")                 private UUID flowelementId;
    @Column(name = "gravity", nullable = false)      private Short gravity;
    @Column(name = "urgency", nullable = false)      private Short urgency;
    @Column(name = "trend", nullable = false)        private Short trend;

    // Auditoría estándar IMAP
    @Column(name = "state_id", nullable = false)     private UUID stateId;
    @Column(name = "created_at", nullable = false, updatable = false)   private OffsetDateTime createdAt;
    @Column(name = "updated_at", nullable = false)   private OffsetDateTime updatedAt;
    @Column(name = "created_by_id", updatable = false)                  private UUID createdById;
    @Column(name = "updated_by_id")                  private UUID updatedById;
    @Column(name = "owned_by_id", updatable = false)                    private UUID ownedById;

    public WhbClassificationEntity() {}

    public UUID getId()                       { return id; }
    public void setId(UUID id)                { this.id = id; }
    public UUID getTenantId()                 { return tenantId; }
    public void setTenantId(UUID id)          { this.tenantId = id; }
    public UUID getProcessdefId()             { return processdefId; }
    public void setProcessdefId(UUID id)      { this.processdefId = id; }
    public UUID getProcessversionId()         { return processversionId; }
    public void setProcessversionId(UUID id)  { this.processversionId = id; }
    public UUID getFlowelementId()            { return flowelementId; }
    public void setFlowelementId(UUID id)     { this.flowelementId = id; }
    public Short getGravity()                 { return gravity; }
    public void setGravity(Short g)           { this.gravity = g; }
    public Short getUrgency()                 { return urgency; }
    public void setUrgency(Short u)           { this.urgency = u; }
    public Short getTrend()                   { return trend; }
    public void setTrend(Short t)             { this.trend = t; }

    public UUID getStateId()                  { return stateId; }
    public void setStateId(UUID id)           { this.stateId = id; }
    public OffsetDateTime getCreatedAt()      { return createdAt; }
    public void setCreatedAt(OffsetDateTime t){ this.createdAt = t; }
    public OffsetDateTime getUpdatedAt()      { return updatedAt; }
    public void setUpdatedAt(OffsetDateTime t){ this.updatedAt = t; }
    public UUID getCreatedById()              { return createdById; }
    public void setCreatedById(UUID id)       { this.createdById = id; }
    public UUID getUpdatedById()              { return updatedById; }
    public void setUpdatedById(UUID id)       { this.updatedById = id; }
    public UUID getOwnedById()                { return ownedById; }
    public void setOwnedById(UUID id)         { this.ownedById = id; }
}
