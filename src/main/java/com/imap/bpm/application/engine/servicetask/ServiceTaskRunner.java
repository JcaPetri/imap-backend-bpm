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

package com.imap.bpm.application.engine.servicetask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/**
 * Ejecuta un service task con retry policy. Wraps al {@link ServiceTaskRegistry}.
 *
 * Retry policy (config en application.yml):
 *   - {@code bpm.service-tasks.retry.max-attempts} (default 3): cantidad total de intentos
 *   - {@code bpm.service-tasks.retry.backoff-ms} (default 1000): backoff inicial
 *   - Backoff exponencial × 5: 1000ms → 5000ms → 25000ms (con max-attempts=3, hace 3 calls
 *     con sleeps de 1s, 5s entre ellos. Sin sleep antes del primero.)
 *
 * Cortes de retry:
 *   - Status SUCCESS → returns SUCCESS inmediato
 *   - Status PENDING → returns PENDING inmediato (V2)
 *   - boundaryErrorCode no-null → NO retry, returns inmediato para que el motor dispare boundary
 *   - Status FAILURE → retry hasta max-attempts. Si todos fallan, returns el último FAILURE.
 *
 * El runner emite logs por cada intento — el motor BPM se encarga del audit log.
 */
@Service
public class ServiceTaskRunner {

    private static final Logger log = LoggerFactory.getLogger(ServiceTaskRunner.class);

    private final ServiceTaskRegistry registry;
    private final int maxAttempts;
    private final long backoffMs;

    public ServiceTaskRunner(
            ServiceTaskRegistry registry,
            @Value("${bpm.service-tasks.retry.max-attempts:3}") int maxAttempts,
            @Value("${bpm.service-tasks.retry.backoff-ms:1000}") long backoffMs) {
        this.registry = registry;
        this.maxAttempts = Math.max(1, maxAttempts);
        this.backoffMs = Math.max(0, backoffMs);
    }

    /** true si el serviceCode tiene handler LOCAL (⇒ sync por default); false ⇒ remoto (⇒ async por default). */
    public boolean hasLocalHandler(String serviceCode) {
        return serviceCode != null && registry.hasLocalHandler(serviceCode);
    }

    public ServiceTaskResult runWithRetry(ServiceTaskContext ctx) {
        ServiceTaskResult last = null;
        long currentBackoff = backoffMs;

        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            log.debug("Executing service_task '{}' attempt {}/{}", ctx.serviceCode(), attempt, maxAttempts);
            last = registry.dispatch(ctx);

            // Success or async → return immediately
            if (last.isSuccess() || last.isPending()) return last;

            // Boundary error → no retry, let engine dispatch boundary event
            if (last.boundaryErrorCode() != null) {
                log.info("service_task '{}' returned boundaryErrorCode='{}' — skipping retry",
                    ctx.serviceCode(), last.boundaryErrorCode());
                return last;
            }

            // Failure → log + maybe retry
            if (attempt < maxAttempts) {
                log.warn("service_task '{}' attempt {} failed [errorCode={}, msg={}] — retrying in {}ms",
                    ctx.serviceCode(), attempt, last.errorCode(), last.errorMessage(), currentBackoff);
                try {
                    Thread.sleep(currentBackoff);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return ServiceTaskResult.fail("INTERRUPTED", "Retry interrupted: " + ie.getMessage());
                }
                currentBackoff = currentBackoff * 5;   // exponencial × 5
            } else {
                log.error("service_task '{}' FAILED after {} attempts [errorCode={}, msg={}]",
                    ctx.serviceCode(), maxAttempts, last.errorCode(), last.errorMessage());
            }
        }

        return last;
    }
}
