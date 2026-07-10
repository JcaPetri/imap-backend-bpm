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
 * JPA entity para bpm.bpm_pro_incident_tbl (Ola 4.2).
 *
 * Incidente de ejecucion: se crea cuando un service_task/job falla terminal
 * (agota retries, sin boundary). Ops lo inspecciona y lo reintenta-desde-el-paso
 * o lo resuelve. Runtime state — el motor la escribe directo (exencion §F.6).
 */
@Entity
@Table(name = "bpm_pro_incident_tbl")
public class IncidentEntity {

    @Id
    @Column(name = "id")
    private UUID id;

    @Column(name = "tenant_id", nullable = false)
    private UUID tenantId;

    @Column(name = "processinstance_id", nullable = false)
    private UUID processinstanceId;

    @Column(name = "token_id")
    private UUID tokenId;

    @Column(name = "element_id")
    private UUID elementId;

    @Column(name = "element_code")
    private String elementCode;

    @Column(name = "incident_type", nullable = false, length = 40)
    private String incidentType;   // service_task_failure | job_failure

    @Column(name = "error_code", length = 80)
    private String errorCode;

    @Column(name = "error_message")
    private String errorMessage;

    @Column(name = "lifecycle", nullable = false, length = 20)
    private String lifecycle;      // open | retrying | resolved

    @Column(name = "resolved_at")
    private OffsetDateTime resolvedAt;

    @Column(name = "state_id", nullable = false)        private UUID stateId;
    @Column(name = "created_at", nullable = false, updatable = false)      private OffsetDateTime createdAt;
    @Column(name = "updated_at", nullable = false)      private OffsetDateTime updatedAt;
    @Column(name = "created_by_id", updatable = false)                     private UUID createdById;
    @Column(name = "updated_by_id")                     private UUID updatedById;
    @Column(name = "owned_by_id", updatable = false)                       private UUID ownedById;

    public IncidentEntity() {}

    public UUID getId()                        { return id; }
    public void setId(UUID id)                 { this.id = id; }
    public UUID getTenantId()                  { return tenantId; }
    public void setTenantId(UUID id)           { this.tenantId = id; }
    public UUID getProcessinstanceId()         { return processinstanceId; }
    public void setProcessinstanceId(UUID id)  { this.processinstanceId = id; }
    public UUID getTokenId()                   { return tokenId; }
    public void setTokenId(UUID id)            { this.tokenId = id; }
    public UUID getElementId()                 { return elementId; }
    public void setElementId(UUID id)          { this.elementId = id; }
    public String getElementCode()             { return elementCode; }
    public void setElementCode(String s)       { this.elementCode = s; }
    public String getIncidentType()            { return incidentType; }
    public void setIncidentType(String s)      { this.incidentType = s; }
    public String getErrorCode()               { return errorCode; }
    public void setErrorCode(String s)         { this.errorCode = s; }
    public String getErrorMessage()            { return errorMessage; }
    public void setErrorMessage(String s)      { this.errorMessage = s; }
    public String getLifecycle()               { return lifecycle; }
    public void setLifecycle(String s)         { this.lifecycle = s; }
    public OffsetDateTime getResolvedAt()      { return resolvedAt; }
    public void setResolvedAt(OffsetDateTime t){ this.resolvedAt = t; }

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
