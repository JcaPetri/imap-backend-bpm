package com.imap.bpm.infrastructure.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * Inbox de dedup de message-start (Consistencia Fase 0 #3-B / outbox at-least-once).
 *
 * Una fila por (tenant_id, event_id) ya procesado. Si llega un reintento del outbox con un eventId
 * ya visto, el endpoint /messages/start lo saltea en vez de arrancar el proceso de nuevo.
 */
@Entity
@Table(name = "bpm_inbox_event_tbl")
public class BpmInboxEventEntity {

    @Id @Column(name = "id")                          private UUID id;
    @Column(name = "tenant_id", nullable = false)     private UUID tenantId;
    @Column(name = "event_id", nullable = false)      private UUID eventId;
    @Column(name = "message_code", nullable = false, length = 100) private String messageCode;
    @Column(name = "instances_started", nullable = false) private int instancesStarted;
    @Column(name = "processed_at", nullable = false)  private OffsetDateTime processedAt;

    protected BpmInboxEventEntity() { }

    public BpmInboxEventEntity(UUID tenantId, UUID eventId, String messageCode, int instancesStarted) {
        this.id = UUID.randomUUID();
        this.tenantId = tenantId;
        this.eventId = eventId;
        this.messageCode = messageCode;
        this.instancesStarted = instancesStarted;
        this.processedAt = OffsetDateTime.now();
    }

    public UUID getId()             { return id; }
    public UUID getTenantId()       { return tenantId; }
    public UUID getEventId()        { return eventId; }
    public String getMessageCode()  { return messageCode; }
    public int getInstancesStarted(){ return instancesStarted; }
    public OffsetDateTime getProcessedAt() { return processedAt; }
}
