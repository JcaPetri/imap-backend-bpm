package com.imap.bpm.infrastructure.repository;

import com.imap.bpm.infrastructure.entity.TaskInstance;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface TaskInstanceRepository extends JpaRepository<TaskInstance, UUID> {
    List<TaskInstance> findByAssignedUserIdAndLifecycleIn(UUID assignedUserId, List<String> lifecycles);
    List<TaskInstance> findByProcessinstanceId(UUID processinstanceId);
}
