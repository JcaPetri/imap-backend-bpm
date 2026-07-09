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
 * Test handler: always fails with errorCode='TEST_FAILURE'.
 *
 * Útil para smoke E2E del retry policy: verifica que el motor reintenta
 * N veces (default 3) antes de marcar el token como failed.
 *
 * Config opcional:
 *   { "serviceCode": "bpm.test.fail", "config": { "errorCode": "CUSTOM_FAIL" } }
 */
@ServiceTask("bpm.test.fail")
public class FailHandler implements ServiceTaskHandler {

    @Override
    public ServiceTaskResult execute(ServiceTaskContext ctx) {
        String code = (String) ctx.config("errorCode");
        if (code == null || code.isBlank()) code = "TEST_FAILURE";
        return ServiceTaskResult.fail(code, "FailHandler always fails (test handler — bpm.test.fail)");
    }
}
