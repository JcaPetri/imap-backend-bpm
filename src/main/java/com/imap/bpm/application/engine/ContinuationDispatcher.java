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

package com.imap.bpm.application.engine;

import com.imap.bpm.infrastructure.security.BpmServiceTokenProvider;
import com.imap.platform.security.BearerTokenHolder;
import com.imap.platform.tenant.TenantContextHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.util.UUID;

/**
 * Immediate-kick de los continuation jobs (4.1 async continuation).
 *
 * Cuando el motor agenda un continuation job (service_task async), en vez de
 * esperar al poll del JobExecutorWorker (@Scheduled cada 5s) lo dispara
 * NEAR-INSTANT apenas commitea la tx que lo agendo. El @Scheduled poll queda
 * como fallback DURABLE (recupera jobs no kickeados: crash, reinicio).
 *
 * Anti-doble-run: kick() hace un claim atomico (scheduled→firing) antes de
 * ejecutar; si el poll u otro kick ya lo tomo, claimJob devuelve false y sale.
 */
@Component
public class ContinuationDispatcher {

    private static final Logger log = LoggerFactory.getLogger(ContinuationDispatcher.class);

    private final JobScanService scanService;
    private final ProcessEngine engine;
    private final BpmServiceTokenProvider serviceTokenProvider;

    /** Self para atravesar el proxy @Async desde el callback afterCommit (sino corre sincrono). */
    @Lazy @Autowired
    private ContinuationDispatcher self;

    public ContinuationDispatcher(JobScanService scanService,
                                  @Lazy ProcessEngine engine,
                                  BpmServiceTokenProvider serviceTokenProvider) {
        this.scanService = scanService;
        this.engine = engine;
        this.serviceTokenProvider = serviceTokenProvider;
    }

    /** Programa el kick para DESPUES del commit de la tx actual (si hay una); sino kickea ya. */
    public void kickAfterCommit(UUID jobId, UUID tenantId) {
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override public void afterCommit() { self.kick(jobId, tenantId); }
            });
        } else {
            self.kick(jobId, tenantId);
        }
    }

    /** Ejecuta el continuation job en un thread aparte (tx nueva). Idempotente via claim atomico. */
    @Async("bpmAsyncExecutor")
    public void kick(UUID jobId, UUID tenantId) {
        try {
            if (!scanService.claimJob(jobId)) return;   // el poll (u otro kick) ya lo tomo → 0 doble-run
            TenantContextHolder.set(tenantId);
            BearerTokenHolder.set(serviceTokenProvider.tokenForTenant(tenantId));
            engine.fireTimerJob(jobId);                 // ve 'firing' → procesa la continuation
        } catch (Exception e) {
            log.error("continuation kick failed for job {}: {}", jobId, e.getMessage(), e);
        } finally {
            TenantContextHolder.clear();
            BearerTokenHolder.clear();
        }
    }
}
