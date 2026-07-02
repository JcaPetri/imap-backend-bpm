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

package com.imap.bpm.infrastructure.entity;

import jakarta.persistence.*;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * JPA entity para bpm.bpm_whb_tenantconfig_tbl — política de priorización del
 * WorkHub por tenant (1 fila por tenant). La carga el admin en la pantalla de
 * admin de BPM. Ver docs/architecture/workhub-northstar.md §5.1/§5.4.
 */
@Entity
@Table(name = "bpm_whb_tenantconfig_tbl")
public class WhbTenantConfig {

    @Id @Column(name = "id")                                  private UUID id;
    @Column(name = "tenant_id", nullable = false)            private UUID tenantId;
    @Column(name = "aggregation_mode", nullable = false, length = 10) private String aggregationMode;
    @Column(name = "weight_gravity", nullable = false)       private BigDecimal weightGravity;
    @Column(name = "weight_urgency", nullable = false)       private BigDecimal weightUrgency;
    @Column(name = "weight_trend", nullable = false)         private BigDecimal weightTrend;
    @Column(name = "threshold_high_pct", nullable = false)   private BigDecimal thresholdHighPct;
    @Column(name = "threshold_medium_pct", nullable = false) private BigDecimal thresholdMediumPct;

    // Auditoría estándar IMAP
    @Column(name = "state_id", nullable = false)             private UUID stateId;
    @Column(name = "created_at", nullable = false, updatable = false)           private OffsetDateTime createdAt;
    @Column(name = "updated_at", nullable = false)           private OffsetDateTime updatedAt;
    @Column(name = "created_by_id", updatable = false)                          private UUID createdById;
    @Column(name = "updated_by_id")                          private UUID updatedById;
    @Column(name = "owned_by_id", updatable = false)                            private UUID ownedById;

    public WhbTenantConfig() {}

    public UUID getId()                          { return id; }
    public void setId(UUID id)                   { this.id = id; }
    public UUID getTenantId()                    { return tenantId; }
    public void setTenantId(UUID id)             { this.tenantId = id; }
    public String getAggregationMode()           { return aggregationMode; }
    public void setAggregationMode(String s)     { this.aggregationMode = s; }
    public BigDecimal getWeightGravity()         { return weightGravity; }
    public void setWeightGravity(BigDecimal w)   { this.weightGravity = w; }
    public BigDecimal getWeightUrgency()         { return weightUrgency; }
    public void setWeightUrgency(BigDecimal w)   { this.weightUrgency = w; }
    public BigDecimal getWeightTrend()           { return weightTrend; }
    public void setWeightTrend(BigDecimal w)     { this.weightTrend = w; }
    public BigDecimal getThresholdHighPct()      { return thresholdHighPct; }
    public void setThresholdHighPct(BigDecimal t){ this.thresholdHighPct = t; }
    public BigDecimal getThresholdMediumPct()    { return thresholdMediumPct; }
    public void setThresholdMediumPct(BigDecimal t){ this.thresholdMediumPct = t; }

    public UUID getStateId()                     { return stateId; }
    public void setStateId(UUID id)              { this.stateId = id; }
    public OffsetDateTime getCreatedAt()         { return createdAt; }
    public void setCreatedAt(OffsetDateTime t)   { this.createdAt = t; }
    public OffsetDateTime getUpdatedAt()         { return updatedAt; }
    public void setUpdatedAt(OffsetDateTime t)   { this.updatedAt = t; }
    public UUID getCreatedById()                 { return createdById; }
    public void setCreatedById(UUID id)          { this.createdById = id; }
    public UUID getUpdatedById()                 { return updatedById; }
    public void setUpdatedById(UUID id)          { this.updatedById = id; }
    public UUID getOwnedById()                   { return ownedById; }
    public void setOwnedById(UUID id)            { this.ownedById = id; }
}
