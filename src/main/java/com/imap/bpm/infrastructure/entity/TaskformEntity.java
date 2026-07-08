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
 * JPA entity para bpm.bpm_hum_taskform_tbl (V015).
 * Form de un user_task; entitydef_id = FK lógica cross-service a system.sys_entity_def.
 */
@Entity
@Table(name = "bpm_hum_taskform_tbl")
public class TaskformEntity {

    @Id
    @Column(name = "id")
    private UUID id;

    @Column(name = "tenant_id", nullable = false)
    private UUID tenantId;

    @Column(name = "flowelement_id", nullable = false)
    private UUID flowelementId;

    @Column(name = "entitydef_id")
    private UUID entitydefId;

    @Column(name = "mode", length = 20)
    private String mode;

    @Column(name = "state_id", nullable = false)
    private UUID stateId;

    @Column(name = "created_at", nullable = false, updatable = false)
    private OffsetDateTime createdAt;

    @Column(name = "updated_at", nullable = false)
    private OffsetDateTime updatedAt;

    @Column(name = "created_by_id", updatable = false)
    private UUID createdById;

    @Column(name = "updated_by_id")
    private UUID updatedById;

    @Column(name = "owned_by_id", updatable = false)
    private UUID ownedById;

    public UUID getId() { return id; }
    public void setId(UUID id) { this.id = id; }
    public UUID getTenantId() { return tenantId; }
    public void setTenantId(UUID tenantId) { this.tenantId = tenantId; }
    public UUID getFlowelementId() { return flowelementId; }
    public void setFlowelementId(UUID flowelementId) { this.flowelementId = flowelementId; }
    public UUID getEntitydefId() { return entitydefId; }
    public void setEntitydefId(UUID entitydefId) { this.entitydefId = entitydefId; }
    public String getMode() { return mode; }
    public void setMode(String mode) { this.mode = mode; }
    public UUID getStateId() { return stateId; }
    public void setStateId(UUID stateId) { this.stateId = stateId; }
    public OffsetDateTime getCreatedAt() { return createdAt; }
    public void setCreatedAt(OffsetDateTime createdAt) { this.createdAt = createdAt; }
    public OffsetDateTime getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(OffsetDateTime updatedAt) { this.updatedAt = updatedAt; }
    public UUID getCreatedById() { return createdById; }
    public void setCreatedById(UUID createdById) { this.createdById = createdById; }
    public UUID getUpdatedById() { return updatedById; }
    public void setUpdatedById(UUID updatedById) { this.updatedById = updatedById; }
    public UUID getOwnedById() { return ownedById; }
    public void setOwnedById(UUID ownedById) { this.ownedById = ownedById; }
}
