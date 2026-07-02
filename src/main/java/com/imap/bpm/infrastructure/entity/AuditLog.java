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

import io.hypersistence.utils.hibernate.type.json.JsonBinaryType;
import jakarta.persistence.*;
import org.hibernate.annotations.Type;

import java.time.OffsetDateTime;
import java.util.Map;
import java.util.UUID;

/**
 * JPA entity para bpm.bpm_pro_auditlog_tbl. Log de eventos del proceso.
 */
@Entity
@Table(name = "bpm_pro_auditlog_tbl")
public class AuditLog {

    @Id @Column(name = "id")                                 private UUID id;
    @Column(name = "tenant_id", nullable = false)            private UUID tenantId;
    @Column(name = "processinstance_id", nullable = false)   private UUID processinstanceId;
    @Column(name = "event_type", nullable = false, length = 50) private String eventType;
    @Column(name = "flowelement_id")                          private UUID flowelementId;
    @Column(name = "token_id")                                private UUID tokenId;
    @Column(name = "user_id")                                 private UUID userId;
    @Column(name = "occurred_at", nullable = false)         private OffsetDateTime occurredAt;

    @Type(JsonBinaryType.class)
    @Column(name = "data_jsonb", columnDefinition = "jsonb")
    private Map<String, Object> data;

    @Column(name = "correlation_id", length = 100)            private String correlationId;

    @Column(name = "state_id", nullable = false)             private UUID stateId;
    @Column(name = "created_at", nullable = false, updatable = false)           private OffsetDateTime createdAt;
    @Column(name = "updated_at", nullable = false)           private OffsetDateTime updatedAt;
    @Column(name = "created_by_id", updatable = false)                          private UUID createdById;
    @Column(name = "updated_by_id")                          private UUID updatedById;
    @Column(name = "owned_by_id", updatable = false)                            private UUID ownedById;

    public AuditLog() {}

    public UUID getId()                       { return id; }
    public void setId(UUID id)                { this.id = id; }
    public UUID getTenantId()                 { return tenantId; }
    public void setTenantId(UUID id)          { this.tenantId = id; }
    public UUID getProcessinstanceId()        { return processinstanceId; }
    public void setProcessinstanceId(UUID id) { this.processinstanceId = id; }
    public String getEventType()              { return eventType; }
    public void setEventType(String s)        { this.eventType = s; }
    public UUID getFlowelementId()            { return flowelementId; }
    public void setFlowelementId(UUID id)     { this.flowelementId = id; }
    public UUID getTokenId()                  { return tokenId; }
    public void setTokenId(UUID id)           { this.tokenId = id; }
    public UUID getUserId()                   { return userId; }
    public void setUserId(UUID id)            { this.userId = id; }
    public OffsetDateTime getOccurredAt()     { return occurredAt; }
    public void setOccurredAt(OffsetDateTime t) { this.occurredAt = t; }
    public Map<String, Object> getData()      { return data; }
    public void setData(Map<String, Object> m){ this.data = m; }
    public String getCorrelationId()          { return correlationId; }
    public void setCorrelationId(String s)    { this.correlationId = s; }

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
