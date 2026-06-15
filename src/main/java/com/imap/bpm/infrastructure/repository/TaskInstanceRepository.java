package com.imap.bpm.infrastructure.repository;

import com.imap.bpm.infrastructure.entity.TaskInstance;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

public interface TaskInstanceRepository extends JpaRepository<TaskInstance, UUID> {

    /**
     * WorkHub — claim atómico (compare-and-set). Asigna la tarea al usuario SOLO
     * si sigue libre (assigned_user_id NULL y lifecycle 'created'). Devuelve el
     * número de filas afectadas: 1 = claim exitoso, 0 = ya la tomó otro (→ 409).
     * Sin lock pesimista. Ver docs/architecture/workhub-northstar.md §3.
     */
    @Modifying
    @Query("UPDATE TaskInstance t SET t.assignedUserId = :userId, t.lifecycle = 'assigned', "
         + "t.assignedAt = :now, t.updatedAt = :now "
         + "WHERE t.id = :taskId AND t.assignedUserId IS NULL AND t.lifecycle = 'created'")
    int claimIfUnassigned(@Param("taskId") UUID taskId,
                          @Param("userId") UUID userId,
                          @Param("now") OffsetDateTime now);
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

    /** Para cascade DELETE de instance (admin cleanup). */
    long deleteByProcessinstanceId(UUID processinstanceId);

    /** Hito 3 — migration: cancelar tasks asociadas al token cuando 'skip' action. */
    List<TaskInstance> findByTokenIdAndLifecycleIn(UUID tokenId, List<String> lifecycles);
}
