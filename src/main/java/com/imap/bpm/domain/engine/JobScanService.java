package com.imap.bpm.domain.engine;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

/**
 * Scan + claim atómico de jobs vencidos para el `JobExecutorWorker`.
 *
 * Resuelve 2 problemas con una sola query:
 *   1. RLS — el worker corre sin tenant context. Setea `app.bypass_rls=true`
 *      para ver jobs de todos los tenants.
 *   2. Race condition entre worker tick y otros UPDATEs (ej cancelBoundary
 *      desde completeTask). Patrón estándar de job-queue: SELECT FOR UPDATE
 *      SKIP LOCKED + UPDATE atómico a lifecycle='firing'. Una vez claimed,
 *      `cancelBoundaryJobsForToken` (que filtra por 'scheduled') ya no lo
 *      toca → 0 race con StaleObjectStateException.
 *
 * Devuelve pares (jobId, tenantId) — necesarios para que el caller setee
 * el tenant en su propia tx antes de procesar el job.
 */
@Service
public class JobScanService {

    @PersistenceContext
    private EntityManager em;

    /** Par (jobId, tenantId) — necesario para que fireTimerJob setee el
     * tenant antes del findById (sino RLS bloquea). */
    public record DueJob(UUID jobId, UUID tenantId) {}

    /**
     * Claim atómico de hasta N jobs vencidos.
     *
     * SQL: SELECT FOR UPDATE SKIP LOCKED elige rows libres (otros workers
     * concurrentes no los pisan), UPDATE atómico los marca 'firing'. RETURNING
     * devuelve los IDs claimed.
     *
     * Tras este claim, los jobs están protegidos contra cancelación porque
     * `cancelBoundaryJobsForToken` solo toca jobs en 'scheduled'.
     */
    @Transactional
    @SuppressWarnings("unchecked")
    public List<DueJob> claimDueJobs(int batchSize) {
        em.createNativeQuery("SET LOCAL app.bypass_rls = 'true'").executeUpdate();
        List<Object[]> rows = em.createNativeQuery("""
            WITH locked AS (
                SELECT id
                  FROM bpm.bpm_pro_jobexecutor_tbl
                 WHERE lifecycle = 'scheduled' AND fire_at <= NOW()
                 ORDER BY fire_at
                 LIMIT :batch
                 FOR UPDATE SKIP LOCKED
            )
            UPDATE bpm.bpm_pro_jobexecutor_tbl j
               SET lifecycle = 'firing', updated_at = NOW()
              FROM locked l
             WHERE j.id = l.id
            RETURNING CAST(j.id AS text), CAST(j.tenant_id AS text)
            """).setParameter("batch", batchSize).getResultList();
        return rows.stream()
            .map(r -> new DueJob(UUID.fromString((String) r[0]), UUID.fromString((String) r[1])))
            .toList();
    }
}
