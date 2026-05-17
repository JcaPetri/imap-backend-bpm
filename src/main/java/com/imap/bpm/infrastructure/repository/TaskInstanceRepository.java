package com.imap.bpm.infrastructure.repository;

import com.imap.bpm.infrastructure.entity.TaskInstance;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface TaskInstanceRepository extends JpaRepository<TaskInstance, UUID> {
    List<TaskInstance> findByAssignedUserIdAndLifecycleIn(UUID assignedUserId, List<String> lifecycles);
    List<TaskInstance> findByProcessinstanceId(UUID processinstanceId);

    /**
     * A3 — query principal de /v1/bpm/me/tasks: tareas vivas asignadas al
     * usuario actual, más recientes primero. AssignmentRule MVP setea
     * assignedUserId = processInstance.startedById en el momento de crear
     * la TaskInstance. Iter 5+ reemplaza por evaluación de rules dinámicas.
     */
    List<TaskInstance> findByAssignedUserIdAndLifecycleInOrderByCreatedAtDesc(
        UUID assignedUserId, List<String> lifecycles);

    /**
     * Fallback (uso admin/debug): TODAS las taskinstance vivas del tenant.
     * RLS las filtra. NO usar en /me/tasks (eso es per-user vía A3 method).
     */
    List<TaskInstance> findByTenantIdAndLifecycleInOrderByCreatedAtDesc(
        UUID tenantId, List<String> lifecycles);
}
