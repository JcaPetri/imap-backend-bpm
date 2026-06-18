package com.imap.bpm.infrastructure.repository;

import com.imap.bpm.infrastructure.entity.BpmInboxEventEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

/**
 * Dedup de message-start (Consistencia Fase 0 #3-B). Lookup por (tenant_id, event_id);
 * el unique constraint atrapa los duplicados concurrentes.
 */
public interface BpmInboxEventRepository extends JpaRepository<BpmInboxEventEntity, UUID> {

    boolean existsByTenantIdAndEventId(UUID tenantId, UUID eventId);
}
