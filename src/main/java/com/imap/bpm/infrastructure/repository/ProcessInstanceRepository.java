package com.imap.bpm.infrastructure.repository;

import com.imap.bpm.infrastructure.entity.ProcessInstance;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface ProcessInstanceRepository extends JpaRepository<ProcessInstance, UUID> {
    List<ProcessInstance> findByTenantIdAndLifecycle(UUID tenantId, String lifecycle);
    List<ProcessInstance> findByProcessdefIdAndLifecycle(UUID processdefId, String lifecycle);
}
