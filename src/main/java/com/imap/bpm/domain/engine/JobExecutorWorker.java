// ─── GOLDEN-RULES:BEGIN (auto · golden-rules.json · no editar a mano) ───
// REGLAS DE ORO IMAP — cumplir SIEMPRE (ver IMAP_GUIA_DESARROLLO.md):
//  • HTTP-only entre servicios (+ s2s auth; no SQL cross-service; futuro Kafka)
//  • Names en inglés
//  • UUIDv7 en ids
//  • i18n: idioma del string, no de la fila; datos (UUID, field, idioma)
//  • VtR: único canal con el frontend (front solo ve virtual)
//  • Hexagonal estricto (domain no depende de infra)
//  • No secrets en código (.env en C:\Applications, nunca hardcodear)
//  • Idempotencia en operaciones de negocio (idempotency key)
//  • [bpm] Orquestador central (tipo Temporal/Step Functions)
//  • [bpm] processdef→processversion: snapshot inmutable al activar
//  • [bpm] Las defs de proceso viven acá (no en system)
//  • [bpm] Camunda 8 como norte; interim form-driven en prod
// ─── GOLDEN-RULES:END ───

package com.imap.bpm.domain.engine;

import com.imap.platform.security.BearerTokenHolder;
import com.imap.bpm.infrastructure.security.BpmServiceTokenProvider;
import com.imap.platform.tenant.TenantContextHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

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

    private final JobScanService scanService;
    private final ProcessEngine engine;
    private final BpmServiceTokenProvider serviceTokenProvider;

    public JobExecutorWorker(JobScanService scanService,
                             ProcessEngine engine,
                             BpmServiceTokenProvider serviceTokenProvider) {
        this.scanService = scanService;
        this.engine = engine;
        this.serviceTokenProvider = serviceTokenProvider;
    }

    @Scheduled(fixedDelay = 5000, initialDelay = 5000)
    public void tick() {
        List<JobScanService.DueJob> dueJobs;
        try {
            // Claim atómico (SELECT FOR UPDATE SKIP LOCKED + UPDATE firing):
            // marca los jobs como 'firing' antes de procesar, así
            // cancelBoundaryJobsForToken (que filtra por 'scheduled') no los
            // toca → 0 race con StaleObjectStateException.
            dueJobs = scanService.claimDueJobs(BATCH_SIZE);
        } catch (Exception e) {
            log.error("JobExecutorWorker tick: claimDueJobs failed", e);
            return;
        }
        if (dueJobs.isEmpty()) return;

        log.debug("JobExecutorWorker tick: {} due jobs to process", dueJobs.size());

        // Pre-setear tenant + service token ANTES de invocar fireTimerJob.
        // - Tenant: necesario para que applyToCurrentTransaction (RLS) cargue
        //   el row del job correcto (sin esto, RLS filtra y devuelve null).
        // - Service token: para que ProcessDefinitionLoader/DecisionLoader
        //   puedan hacer s2s call al system si hay cache miss durante
        //   advanceToken (el worker no tiene JWT del user).
        // Importante: llamar engine.fireTimerJob(jobId) — Spring CGLIB proxy
        // intercepta llamadas externas y aplica @Transactional correctamente.
        for (JobScanService.DueJob d : dueJobs) {
            try {
                TenantContextHolder.set(d.tenantId());
                // 3c.1 — service token MIEMBRO del tenant del job (no currentToken,
                // que solo tiene SYSTEM_TENANT). Si advanceToken hace un load()/DMN
                // cache-miss s2s a system, el token debe ser miembro del tenant del
                // job para pasar el TenantContextFilter. tokenForTenant cae a
                // currentToken internamente cuando el tenant es SYSTEM.
                BearerTokenHolder.set(serviceTokenProvider.tokenForTenant(d.tenantId()));
                engine.fireTimerJob(d.jobId());
            } catch (Exception e) {
                log.error("JobExecutorWorker: uncaught error processing job {}", d.jobId(), e);
            } finally {
                TenantContextHolder.clear();
                BearerTokenHolder.clear();
            }
        }
    }
}
