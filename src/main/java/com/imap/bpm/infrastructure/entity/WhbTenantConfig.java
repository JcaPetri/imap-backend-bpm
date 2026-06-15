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
    @Column(name = "created_at", nullable = false)           private OffsetDateTime createdAt;
    @Column(name = "updated_at", nullable = false)           private OffsetDateTime updatedAt;
    @Column(name = "created_by_id")                          private UUID createdById;
    @Column(name = "updated_by_id")                          private UUID updatedById;
    @Column(name = "owned_by_id")                            private UUID ownedById;
    @Column(name = "timezone_id")                            private UUID timezoneId;
    @Column(name = "table_history")                          private String tableHistory;
    @Column(name = "data_language_id")                       private UUID dataLanguageId;

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
    public UUID getTimezoneId()                  { return timezoneId; }
    public void setTimezoneId(UUID id)           { this.timezoneId = id; }
    public String getTableHistory()              { return tableHistory; }
    public void setTableHistory(String s)        { this.tableHistory = s; }
    public UUID getDataLanguageId()              { return dataLanguageId; }
    public void setDataLanguageId(UUID id)       { this.dataLanguageId = id; }
}
