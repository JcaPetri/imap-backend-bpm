package com.imap.bpm.infrastructure.repository;

import com.imap.bpm.infrastructure.entity.AuditLog;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface AuditLogRepository extends JpaRepository<AuditLog, UUID> {
    List<AuditLog> findByProcessinstanceIdOrderByOccurredAtDesc(UUID processinstanceId);

    /** Para cascade DELETE de instance (admin cleanup). */
    long deleteByProcessinstanceId(UUID processinstanceId);
}
