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
 * JPA entity para bpm.bpm_pro_token_tbl.
 *
 * Un token es la "marca de ejecución" del proceso — está parado en un
 * flow_element específico. Parallel gateway crea N tokens hijos. Join
 * gateway los espera y los re-une.
 *
 * current_element_id apunta a una entity virtual del system (bpm_pro_flowelement).
 */
@Entity
@Table(name = "bpm_pro_token_tbl")
public class Token {

    @Id
    @Column(name = "id")
    private UUID id;

    @Column(name = "tenant_id", nullable = false)
    private UUID tenantId;

    @Column(name = "processinstance_id", nullable = false)
    private UUID processinstanceId;

    @Column(name = "current_element_id", nullable = false)
    private UUID currentElementId;

    @Column(name = "parent_token_id")
    private UUID parentTokenId;

    @Column(name = "lifecycle", nullable = false, length = 20)
    private String lifecycle;     // 'active' | 'waiting' | 'consumed'

    @Column(name = "entered_at", nullable = false)
    private OffsetDateTime enteredAt;

    @Column(name = "state_id", nullable = false)        private UUID stateId;
    @Column(name = "created_at", nullable = false)      private OffsetDateTime createdAt;
    @Column(name = "updated_at", nullable = false)      private OffsetDateTime updatedAt;
    @Column(name = "created_by_id")                     private UUID createdById;
    @Column(name = "updated_by_id")                     private UUID updatedById;
    @Column(name = "owned_by_id")                       private UUID ownedById;
    @Column(name = "timezone_id")                       private UUID timezoneId;
    @Column(name = "table_history")                     private String tableHistory;
    @Column(name = "data_language_id")                  private UUID dataLanguageId;

    public Token() {}

    public UUID getId()                       { return id; }
    public void setId(UUID id)                { this.id = id; }
    public UUID getTenantId()                 { return tenantId; }
    public void setTenantId(UUID id)          { this.tenantId = id; }
    public UUID getProcessinstanceId()        { return processinstanceId; }
    public void setProcessinstanceId(UUID id) { this.processinstanceId = id; }
    public UUID getCurrentElementId()         { return currentElementId; }
    public void setCurrentElementId(UUID id)  { this.currentElementId = id; }
    public UUID getParentTokenId()            { return parentTokenId; }
    public void setParentTokenId(UUID id)     { this.parentTokenId = id; }
    public String getLifecycle()              { return lifecycle; }
    public void setLifecycle(String s)        { this.lifecycle = s; }
    public OffsetDateTime getEnteredAt()      { return enteredAt; }
    public void setEnteredAt(OffsetDateTime t){ this.enteredAt = t; }

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
    public UUID getTimezoneId()               { return timezoneId; }
    public void setTimezoneId(UUID id)        { this.timezoneId = id; }
    public String getTableHistory()           { return tableHistory; }
    public void setTableHistory(String s)     { this.tableHistory = s; }
    public UUID getDataLanguageId()           { return dataLanguageId; }
    public void setDataLanguageId(UUID id)    { this.dataLanguageId = id; }
}
