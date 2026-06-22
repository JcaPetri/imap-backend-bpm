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
