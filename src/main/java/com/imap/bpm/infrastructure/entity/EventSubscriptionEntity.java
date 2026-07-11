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
 * JPA entity para bpm.bpm_pro_event_subscription_tbl (Ola 6.1).
 *
 * Suscripcion de un event_sub_process DORMANTE adjunto a una instancia: se registra al
 * arrancar la instancia y se dispara cuando llega el evento (MVP: signal) con el
 * trigger_code que matchea. Runtime state — el motor la escribe directo (exencion §F.6).
 */
@Entity
@Table(name = "bpm_pro_event_subscription_tbl")
public class EventSubscriptionEntity {

    @Id
    @Column(name = "id")
    private UUID id;

    @Column(name = "tenant_id", nullable = false)
    private UUID tenantId;

    @Column(name = "processinstance_id", nullable = false)
    private UUID processinstanceId;

    @Column(name = "element_id")
    private UUID elementId;

    @Column(name = "element_code")
    private String elementCode;

    @Column(name = "trigger_type", nullable = false, length = 20)
    private String triggerType;    // signal (MVP) | message | error (futuro)

    @Column(name = "trigger_code", nullable = false, length = 100)
    private String triggerCode;

    @Column(name = "correlation_key", length = 200)
    private String correlationKey;    // solo message: rutea el mensaje a la instancia (null/'*' = wildcard)

    @Column(name = "handler_version_id", nullable = false)
    private UUID handlerVersionId;

    @Column(name = "interrupting", nullable = false)
    private boolean interrupting;

    @Column(name = "lifecycle", nullable = false, length = 20)
    private String lifecycle;      // active | consumed

    @Column(name = "state_id", nullable = false)        private UUID stateId;
    @Column(name = "created_at", nullable = false, updatable = false)      private OffsetDateTime createdAt;
    @Column(name = "updated_at", nullable = false)      private OffsetDateTime updatedAt;
    @Column(name = "created_by_id", updatable = false)                     private UUID createdById;
    @Column(name = "updated_by_id")                     private UUID updatedById;
    @Column(name = "owned_by_id", updatable = false)                       private UUID ownedById;

    public EventSubscriptionEntity() {}

    public UUID getId()                        { return id; }
    public void setId(UUID id)                 { this.id = id; }
    public UUID getTenantId()                  { return tenantId; }
    public void setTenantId(UUID id)           { this.tenantId = id; }
    public UUID getProcessinstanceId()         { return processinstanceId; }
    public void setProcessinstanceId(UUID id)  { this.processinstanceId = id; }
    public UUID getElementId()                 { return elementId; }
    public void setElementId(UUID id)          { this.elementId = id; }
    public String getElementCode()             { return elementCode; }
    public void setElementCode(String s)       { this.elementCode = s; }
    public String getTriggerType()             { return triggerType; }
    public void setTriggerType(String s)       { this.triggerType = s; }
    public String getTriggerCode()             { return triggerCode; }
    public void setTriggerCode(String s)       { this.triggerCode = s; }
    public String getCorrelationKey()          { return correlationKey; }
    public void setCorrelationKey(String s)    { this.correlationKey = s; }
    public UUID getHandlerVersionId()          { return handlerVersionId; }
    public void setHandlerVersionId(UUID id)   { this.handlerVersionId = id; }
    public boolean isInterrupting()            { return interrupting; }
    public void setInterrupting(boolean b)     { this.interrupting = b; }
    public String getLifecycle()               { return lifecycle; }
    public void setLifecycle(String s)         { this.lifecycle = s; }

    public UUID getStateId()                   { return stateId; }
    public void setStateId(UUID id)            { this.stateId = id; }
    public OffsetDateTime getCreatedAt()       { return createdAt; }
    public void setCreatedAt(OffsetDateTime t) { this.createdAt = t; }
    public OffsetDateTime getUpdatedAt()       { return updatedAt; }
    public void setUpdatedAt(OffsetDateTime t) { this.updatedAt = t; }
    public UUID getCreatedById()               { return createdById; }
    public void setCreatedById(UUID id)        { this.createdById = id; }
    public UUID getUpdatedById()               { return updatedById; }
    public void setUpdatedById(UUID id)        { this.updatedById = id; }
    public UUID getOwnedById()                 { return ownedById; }
    public void setOwnedById(UUID id)          { this.ownedById = id; }
}
