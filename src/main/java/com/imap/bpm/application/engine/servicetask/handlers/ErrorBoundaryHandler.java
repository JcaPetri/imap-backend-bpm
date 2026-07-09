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

/**
 * Test handler: falla con boundaryErrorCode='TEST_BOUNDARY_ERROR' (o el código
 * configurado en config.errorCode). El motor NO reintenta — dispara el boundary
 * error event adjunto al service_task.
 *
 * Útil para smoke E2E del flujo: service_task fails → boundary error event
 * triggers → flujo se va por la rama de error.
 *
 * Config:
 *   { "serviceCode": "bpm.test.error_boundary", "config": { "errorCode": "INSUFFICIENT_STOCK" } }
 */
@ServiceTask("bpm.test.error_boundary")
public class ErrorBoundaryHandler implements ServiceTaskHandler {

    @Override
    public ServiceTaskResult execute(ServiceTaskContext ctx) {
        String code = (String) ctx.config("errorCode");
        if (code == null || code.isBlank()) code = "TEST_BOUNDARY_ERROR";
        return ServiceTaskResult.boundaryError(code,
            "ErrorBoundaryHandler raised boundary error (test handler — bpm.test.error_boundary)");
    }
}
