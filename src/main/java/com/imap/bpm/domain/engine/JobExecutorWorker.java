package com.imap.bpm.domain.engine;

import com.imap.bpm.infrastructure.entity.JobExecutor;
import com.imap.bpm.infrastructure.repository.JobExecutorRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.PageRequest;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.OffsetDateTime;
import java.util.List;

/**
 * Worker scheduled del motor BPM. Polea bpm_pro_jobexecutor_tbl cada N
 * segundos buscando jobs vencidos y dispara la acción correspondiente.
 *
 * MVP (A2): solo job_type='timer'. Cada timer vencido dispara
 * `ProcessEngine.fireTimerJob(jobId)` que reactiva el token y avanza.
 *
 * Frecuencia: cada 5 segundos. Buffer razonable para procesos con timers
 * de pocos segundos a minutos. Si llegan a haber miles de timers/min,
 * habría que pensar en otra arquitectura (Quartz, schedule-driven con
 * pg_notify, etc). Para MVP IMAP volumen bajo.
 *
 * Lock distribuido: no implementado — si corremos múltiples replicas del
 * bpm, los jobs serán procesados varias veces. Para evitar eso:
 *   - Single-replica deploy (MVP)
 *   - O agregar SELECT ... FOR UPDATE SKIP LOCKED en findDueJobs
 *
 * Habilita via @EnableScheduling en BpmApplication.
 */
@Component
public class JobExecutorWorker {

    private static final Logger log = LoggerFactory.getLogger(JobExecutorWorker.class);
    private static final int BATCH_SIZE = 50;

    private final JobExecutorRepository jobRepo;
    private final ProcessEngine engine;

    public JobExecutorWorker(JobExecutorRepository jobRepo, ProcessEngine engine) {
        this.jobRepo = jobRepo;
        this.engine = engine;
    }

    @Scheduled(fixedDelay = 5000, initialDelay = 5000)
    public void tick() {
        List<JobExecutor> due;
        try {
            due = jobRepo.findDueJobs(OffsetDateTime.now(), PageRequest.of(0, BATCH_SIZE));
        } catch (Exception e) {
            log.error("JobExecutorWorker tick: findDueJobs failed", e);
            return;
        }
        if (due.isEmpty()) return;

        log.debug("JobExecutorWorker tick: {} due jobs to process", due.size());
        for (JobExecutor job : due) {
            try {
                switch (job.getJobType()) {
                    case "timer" -> engine.fireTimerJob(job.getId());
                    default -> log.warn("JobExecutorWorker: unsupported job_type '{}' on job {}",
                        job.getJobType(), job.getId());
                }
            } catch (Exception e) {
                // fireTimerJob ya maneja su propio retry; acá solo logueamos
                // cualquier error que escape al filtro de @Transactional
                log.error("JobExecutorWorker: uncaught error processing job {}", job.getId(), e);
            }
        }
    }
}
