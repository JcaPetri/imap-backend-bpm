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

package com.imap.bpm.application.engine.servicetask.handlers;

import com.imap.bpm.application.engine.servicetask.ServiceTask;
import com.imap.bpm.application.engine.servicetask.ServiceTaskContext;
import com.imap.bpm.application.engine.servicetask.ServiceTaskHandler;
import com.imap.bpm.application.engine.servicetask.ServiceTaskResult;

import java.util.Map;

/**
 * Test handler: sleeps the configured delayMs (default 2000) and returns ok.
 *
 * Útil para smoke E2E de latencia y de timeout enforcement (poner delayMs > timeout-seconds).
 *
 * Config:
 *   { "serviceCode": "bpm.test.delay", "config": { "delayMs": 2000 } }
 */
@ServiceTask("bpm.test.delay")
public class DelayHandler implements ServiceTaskHandler {

    @Override
    public ServiceTaskResult execute(ServiceTaskContext ctx) {
        Object delayCfg = ctx.config("delayMs");
        long delayMs = 2000;
        if (delayCfg != null) {
            try { delayMs = Long.parseLong(delayCfg.toString()); }
            catch (NumberFormatException ignored) {}
        }
        long start = System.currentTimeMillis();
        try {
            Thread.sleep(delayMs);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            return ServiceTaskResult.fail("INTERRUPTED", "Sleep interrupted");
        }
        long elapsed = System.currentTimeMillis() - start;
        return ServiceTaskResult.ok(Map.of(
            "delayMs", delayMs,
            "actualElapsedMs", elapsed
        ));
    }
}
