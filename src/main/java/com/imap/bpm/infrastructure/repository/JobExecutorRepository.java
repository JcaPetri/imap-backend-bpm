package com.imap.bpm.infrastructure.repository;

import com.imap.bpm.infrastructure.entity.JobExecutor;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

public interface JobExecutorRepository extends JpaRepository<JobExecutor, UUID> {

    /**
     * Jobs vencidos esperando firing. Worker scheduled lo polea cada N seg.
     * Limita resultado a evitar lock-storms si hubiera cola gigante (en MVP
     * el volumen es bajo, igual lockeamos por seguridad).
     *
     * NOTE: usa LIMIT 50 — overflow se procesa en la próxima iter del worker.
     */
    @Query("""
        SELECT j FROM JobExecutor j
         WHERE j.lifecycle = 'scheduled'
           AND j.fireAt <= :now
         ORDER BY j.fireAt
        """)
    List<JobExecutor> findDueJobs(@Param("now") OffsetDateTime now,
                                  org.springframework.data.domain.Pageable pageable);

    List<JobExecutor> findByProcessinstanceIdAndLifecycle(UUID processinstanceId, String lifecycle);
    List<JobExecutor> findByTokenIdAndLifecycle(UUID tokenId, String lifecycle);

    /**
     * Cascade DELETE de instance (admin cleanup).
     *
     * Bulk DELETE via JPQL @Modifying (no entity lifecycle), evita
     * StaleObjectStateException en race con worker tick: el derived
     * deleteByXxx hace findAll + em.remove → optimistic lock check por
     * row. Esta versión hace un solo DELETE SQL atómico.
     */
    @Modifying
    @Transactional
    @Query("DELETE FROM JobExecutor j WHERE j.processinstanceId = :pid")
    int deleteByProcessinstanceId(@Param("pid") UUID processinstanceId);
}
