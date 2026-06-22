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

import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * JPA entity para bpm.bpm_pro_processinstance_tbl.
 *
 * Una instance representa un proceso en ejecución (ej. un Login específico
 * iniciado por el user X en T). Snapshot del processversion_id que está
 * corriendo — si admin publica una nueva version, instances activas siguen
 * con su version vieja.
 *
 * FKs cross-microservicio (processdef_id, processversion_id) son UUID sin
 * FK física Postgres — la referencia es lógica (vive en system EAV).
 */
@Entity
@Table(name = "bpm_pro_processinstance_tbl")
public class ProcessInstance {

    @Id
    @Column(name = "id")
    private UUID id;

    @Column(name = "tenant_id", nullable = false)
    private UUID tenantId;

    @Column(name = "processdef_id", nullable = false)
    private UUID processdefId;

    @Column(name = "processversion_id", nullable = false)
    private UUID processversionId;

    @Column(name = "parent_instance_id")
    private UUID parentInstanceId;

    /** B1 — token específico del parent que está waiting en el sub_process. */
    @Column(name = "parent_token_id")
    private UUID parentTokenId;

    @Column(name = "lifecycle", nullable = false, length = 20)
    private String lifecycle;       // 'active' | 'completed' | 'cancelled' | 'failed' | 'suspended'

    @Column(name = "started_at", nullable = false)
    private OffsetDateTime startedAt;

    @Column(name = "ended_at")
    private OffsetDateTime endedAt;

    @Column(name = "started_by_id")
    private UUID startedById;

    @Column(name = "correlation_key", length = 255)
    private String correlationKey;

    // Audit columns (los 10 estándar)
    @Column(name = "state_id", nullable = false)        private UUID stateId;
    @Column(name = "created_at", nullable = false)      private OffsetDateTime createdAt;
    @Column(name = "updated_at", nullable = false)      private OffsetDateTime updatedAt;
    @Column(name = "created_by_id")                     private UUID createdById;
    @Column(name = "updated_by_id")                     private UUID updatedById;
    @Column(name = "owned_by_id")                       private UUID ownedById;
    @Column(name = "timezone_id")                       private UUID timezoneId;
    @Column(name = "table_history")                     private String tableHistory;
    @Column(name = "data_language_id")                  private UUID dataLanguageId;

    // ── Constructors ─────────────────────────────────────────────────────────
    public ProcessInstance() {}

    // ── Getters / Setters ────────────────────────────────────────────────────
    public UUID getId()                      { return id; }
    public void setId(UUID id)               { this.id = id; }
    public UUID getTenantId()                { return tenantId; }
    public void setTenantId(UUID tenantId)   { this.tenantId = tenantId; }
    public UUID getProcessdefId()            { return processdefId; }
    public void setProcessdefId(UUID id)     { this.processdefId = id; }
    public UUID getProcessversionId()        { return processversionId; }
    public void setProcessversionId(UUID id) { this.processversionId = id; }
    public UUID getParentInstanceId()        { return parentInstanceId; }
    public void setParentInstanceId(UUID id) { this.parentInstanceId = id; }
    public UUID getParentTokenId()           { return parentTokenId; }
    public void setParentTokenId(UUID id)    { this.parentTokenId = id; }
    public String getLifecycle()             { return lifecycle; }
    public void setLifecycle(String s)       { this.lifecycle = s; }
    public OffsetDateTime getStartedAt()     { return startedAt; }
    public void setStartedAt(OffsetDateTime t) { this.startedAt = t; }
    public OffsetDateTime getEndedAt()       { return endedAt; }
    public void setEndedAt(OffsetDateTime t) { this.endedAt = t; }
    public UUID getStartedById()             { return startedById; }
    public void setStartedById(UUID id)      { this.startedById = id; }
    public String getCorrelationKey()        { return correlationKey; }
    public void setCorrelationKey(String s)  { this.correlationKey = s; }

    public UUID getStateId()                 { return stateId; }
    public void setStateId(UUID id)          { this.stateId = id; }
    public OffsetDateTime getCreatedAt()     { return createdAt; }
    public void setCreatedAt(OffsetDateTime t) { this.createdAt = t; }
    public OffsetDateTime getUpdatedAt()     { return updatedAt; }
    public void setUpdatedAt(OffsetDateTime t) { this.updatedAt = t; }
    public UUID getCreatedById()             { return createdById; }
    public void setCreatedById(UUID id)      { this.createdById = id; }
    public UUID getUpdatedById()             { return updatedById; }
    public void setUpdatedById(UUID id)      { this.updatedById = id; }
    public UUID getOwnedById()               { return ownedById; }
    public void setOwnedById(UUID id)        { this.ownedById = id; }
    public UUID getTimezoneId()              { return timezoneId; }
    public void setTimezoneId(UUID id)       { this.timezoneId = id; }
    public String getTableHistory()          { return tableHistory; }
    public void setTableHistory(String s)    { this.tableHistory = s; }
    public UUID getDataLanguageId()          { return dataLanguageId; }
    public void setDataLanguageId(UUID id)   { this.dataLanguageId = id; }
}
