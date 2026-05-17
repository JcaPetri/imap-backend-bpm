package com.imap.bpm.domain.engine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.UUID;

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

    private final JobScanService scanService;
    private final ProcessEngine engine;

    public JobExecutorWorker(JobScanService scanService, ProcessEngine engine) {
        this.scanService = scanService;
        this.engine = engine;
    }

    @Scheduled(fixedDelay = 5000, initialDelay = 5000)
    public void tick() {
        List<JobScanService.DueJob> dueJobs;
        try {
            // El scan corre en tx separada con bypass RLS (el worker no tiene
            // tenant context). Devuelve pares (jobId, tenantId) para que
            // fireTimerJob setee el tenant ANTES del findById (sino RLS
            // filtraría y devolvería null).
            dueJobs = scanService.findDueJobIds(BATCH_SIZE);
        } catch (Exception e) {
            log.error("JobExecutorWorker tick: findDueJobIds failed", e);
            return;
        }
        if (dueJobs.isEmpty()) return;

        log.debug("JobExecutorWorker tick: {} due jobs to process", dueJobs.size());
        for (JobScanService.DueJob d : dueJobs) {
            try {
                engine.fireTimerJob(d.jobId(), d.tenantId());
            } catch (Exception e) {
                log.error("JobExecutorWorker: uncaught error processing job {}", d.jobId(), e);
            }
        }
    }
}
