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

package com.imap.bpm.domain.model;

import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * Plan de migración de instancias entre dos processversions (V015).
 * Agregado de dominio puro (sin JPA).
 */
public class Migrationplan {

    private final UUID id;
    private final UUID tenantId;
    private final String code;
    private final String description;
    private final UUID sourceProcessversionId;
    private final UUID targetProcessversionId;
    private final String status;
    private final OffsetDateTime appliedAt;
    private final UUID appliedBy;
    private final String stats;
    private final UUID stateId;
    private final OffsetDateTime createdAt;
    private final OffsetDateTime updatedAt;
    private final UUID createdById;
    private final UUID updatedById;
    private final UUID ownedById;

    public Migrationplan(UUID id, UUID tenantId, String code, String description,
                         UUID sourceProcessversionId, UUID targetProcessversionId, String status,
                         OffsetDateTime appliedAt, UUID appliedBy, String stats, UUID stateId,
                         OffsetDateTime createdAt, OffsetDateTime updatedAt,
                         UUID createdById, UUID updatedById, UUID ownedById) {
        this.id = id;
        this.tenantId = tenantId;
        this.code = code;
        this.description = description;
        this.sourceProcessversionId = sourceProcessversionId;
        this.targetProcessversionId = targetProcessversionId;
        this.status = status;
        this.appliedAt = appliedAt;
        this.appliedBy = appliedBy;
        this.stats = stats;
        this.stateId = stateId;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
        this.createdById = createdById;
        this.updatedById = updatedById;
        this.ownedById = ownedById;
    }

    public UUID getId() { return id; }
    public UUID getTenantId() { return tenantId; }
    public String getCode() { return code; }
    public String getDescription() { return description; }
    public UUID getSourceProcessversionId() { return sourceProcessversionId; }
    public UUID getTargetProcessversionId() { return targetProcessversionId; }
    public String getStatus() { return status; }
    public OffsetDateTime getAppliedAt() { return appliedAt; }
    public UUID getAppliedBy() { return appliedBy; }
    public String getStats() { return stats; }
    public UUID getStateId() { return stateId; }
    public OffsetDateTime getCreatedAt() { return createdAt; }
    public OffsetDateTime getUpdatedAt() { return updatedAt; }
    public UUID getCreatedById() { return createdById; }
    public UUID getUpdatedById() { return updatedById; }
    public UUID getOwnedById() { return ownedById; }
}
