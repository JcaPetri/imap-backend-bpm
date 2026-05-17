package com.imap.bpm.domain.engine;

import com.imap.bpm.infrastructure.entity.JobExecutor;
import com.imap.bpm.infrastructure.repository.JobExecutorRepository;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

/**
 * Scan de jobs vencidos para el `JobExecutorWorker` scheduled.
 *
 * Separado del worker porque necesita una transacción propia con RLS bypass:
 * el worker corre en un thread sin tenant context (no viene de un request),
 * y los jobs viven en tenants operativos, por lo que la policy RLS por defecto
 * los filtra todos.
 *
 * Aplica `SET LOCAL app.bypass_rls = 'true'` al inicio para que findDueJobs
 * vea todos los tenants. Devuelve solo IDs (no entidades) para que el caller
 * los procese cada uno en su propia transacción (con el tenant real del job
 * cargado en `engine.fireTimerJob`).
 */
@Service
public class JobScanService {

    @PersistenceContext
    private EntityManager em;

    private final JobExecutorRepository jobRepo;

    public JobScanService(JobExecutorRepository jobRepo) {
        this.jobRepo = jobRepo;
    }

    @Transactional(readOnly = true)
    public List<UUID> findDueJobIds(int batchSize) {
        em.createNativeQuery("SET LOCAL app.bypass_rls = 'true'").executeUpdate();
        List<JobExecutor> rows = jobRepo.findDueJobs(OffsetDateTime.now(),
            PageRequest.of(0, batchSize));
        return rows.stream().map(JobExecutor::getId).toList();
    }
}
