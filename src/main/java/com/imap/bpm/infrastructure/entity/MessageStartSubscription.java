package com.imap.bpm.infrastructure.entity;

import jakarta.persistence.*;

import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * JPA entity para bpm.bpm_pro_message_start_subscription_tbl.
 *
 * Cada row representa un startEvent de un processversion suscrito a un message
 * code. Cuando llega un POST /v1/bpm/messages/start con ese messageCode, el
 * motor BPM arranca una nueva ProcessInstance por cada subscription activa
 * matching del tenant.
 *
 * Diferencia con MessageCorrelation:
 *   - MessageCorrelation: despierta tokens existentes (intermediate events)
 *   - MessageStartSubscription: arranca NEW instances (start events)
 *
 * Populada por ProcessDefinitionLoader al cargar un processdef con startEvents
 * que tienen config.message.messageCode. Idempotente (UPSERT por unique constraint).
 *
 * V1 simple: solo la version current del processdef dispara (cuando una version
 * nueva sube, el loader desactiva las viejas del mismo processdef_id+message_code).
 */
@Entity
@Table(name = "bpm_pro_message_start_subscription_tbl")
public class MessageStartSubscription {

    @Id @Column(name = "id")                                          private UUID id;
    @Column(name = "tenant_id", nullable = false)                     private UUID tenantId;
    @Column(name = "message_code", nullable = false, length = 100)    private String messageCode;
    @Column(name = "processdef_id", nullable = false)                 private UUID processdefId;
    @Column(name = "processversion_id", nullable = false)             private UUID processversionId;
    @Column(name = "start_flow_element_id", nullable = false)         private UUID startFlowElementId;
    @Column(name = "is_active", nullable = false)                     private boolean active = true;

    @Column(name = "state_id", nullable = false)                      private UUID stateId;
    @Column(name = "created_at", nullable = false)                    private OffsetDateTime createdAt;
    @Column(name = "updated_at", nullable = false)                    private OffsetDateTime updatedAt;
    @Column(name = "created_by_id")                                   private UUID createdById;
    @Column(name = "updated_by_id")                                   private UUID updatedById;
    @Column(name = "owned_by_id")                                     private UUID ownedById;
    @Column(name = "timezone_id")                                     private UUID timezoneId;
    @Column(name = "table_history")                                   private String tableHistory;
    @Column(name = "data_language_id")                                private UUID dataLanguageId;

    public MessageStartSubscription() {}

    // ─── Getters/setters ──────────────────────────────────────────────────────

    public UUID getId() { return id; }
    public void setId(UUID id) { this.id = id; }

    public UUID getTenantId() { return tenantId; }
    public void setTenantId(UUID tenantId) { this.tenantId = tenantId; }

    public String getMessageCode() { return messageCode; }
    public void setMessageCode(String messageCode) { this.messageCode = messageCode; }

    public UUID getProcessdefId() { return processdefId; }
    public void setProcessdefId(UUID processdefId) { this.processdefId = processdefId; }

    public UUID getProcessversionId() { return processversionId; }
    public void setProcessversionId(UUID processversionId) { this.processversionId = processversionId; }

    public UUID getStartFlowElementId() { return startFlowElementId; }
    public void setStartFlowElementId(UUID startFlowElementId) { this.startFlowElementId = startFlowElementId; }

    public boolean isActive() { return active; }
    public void setActive(boolean active) { this.active = active; }

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

    public UUID getTimezoneId() { return timezoneId; }
    public void setTimezoneId(UUID timezoneId) { this.timezoneId = timezoneId; }

    public String getTableHistory() { return tableHistory; }
    public void setTableHistory(String tableHistory) { this.tableHistory = tableHistory; }

    public UUID getDataLanguageId() { return dataLanguageId; }
    public void setDataLanguageId(UUID dataLanguageId) { this.dataLanguageId = dataLanguageId; }
}
