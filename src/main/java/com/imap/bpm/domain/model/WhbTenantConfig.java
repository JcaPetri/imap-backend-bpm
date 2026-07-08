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

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * Política de priorización del WorkHub por tenant (agregación G/U/T + umbrales).
 * Agregado de dominio puro (sin JPA). Fila "current" única por tenant.
 */
public class WhbTenantConfig {

    private final UUID id;
    private final UUID tenantId;
    private final String aggregationMode;
    private final BigDecimal weightGravity;
    private final BigDecimal weightUrgency;
    private final BigDecimal weightTrend;
    private final BigDecimal thresholdHighPct;
    private final BigDecimal thresholdMediumPct;
    private final UUID stateId;
    private final OffsetDateTime createdAt;
    private final UUID createdById;
    private final OffsetDateTime updatedAt;
    private final UUID updatedById;
    private final UUID ownedById;

    public WhbTenantConfig(UUID id, UUID tenantId, String aggregationMode,
                           BigDecimal weightGravity, BigDecimal weightUrgency, BigDecimal weightTrend,
                           BigDecimal thresholdHighPct, BigDecimal thresholdMediumPct, UUID stateId,
                           OffsetDateTime createdAt, UUID createdById, OffsetDateTime updatedAt,
                           UUID updatedById, UUID ownedById) {
        this.id = id;
        this.tenantId = tenantId;
        this.aggregationMode = aggregationMode;
        this.weightGravity = weightGravity;
        this.weightUrgency = weightUrgency;
        this.weightTrend = weightTrend;
        this.thresholdHighPct = thresholdHighPct;
        this.thresholdMediumPct = thresholdMediumPct;
        this.stateId = stateId;
        this.createdAt = createdAt;
        this.createdById = createdById;
        this.updatedAt = updatedAt;
        this.updatedById = updatedById;
        this.ownedById = ownedById;
    }

    public UUID getId() { return id; }
    public UUID getTenantId() { return tenantId; }
    public String getAggregationMode() { return aggregationMode; }
    public BigDecimal getWeightGravity() { return weightGravity; }
    public BigDecimal getWeightUrgency() { return weightUrgency; }
    public BigDecimal getWeightTrend() { return weightTrend; }
    public BigDecimal getThresholdHighPct() { return thresholdHighPct; }
    public BigDecimal getThresholdMediumPct() { return thresholdMediumPct; }
    public UUID getStateId() { return stateId; }
    public OffsetDateTime getCreatedAt() { return createdAt; }
    public UUID getCreatedById() { return createdById; }
    public OffsetDateTime getUpdatedAt() { return updatedAt; }
    public UUID getUpdatedById() { return updatedById; }
    public UUID getOwnedById() { return ownedById; }
}
