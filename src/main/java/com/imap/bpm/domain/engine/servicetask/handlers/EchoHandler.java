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

package com.imap.bpm.domain.engine.servicetask.handlers;

import com.imap.bpm.domain.engine.servicetask.ServiceTask;
import com.imap.bpm.domain.engine.servicetask.ServiceTaskContext;
import com.imap.bpm.domain.engine.servicetask.ServiceTaskHandler;
import com.imap.bpm.domain.engine.servicetask.ServiceTaskResult;

import java.util.Map;

/**
 * Test handler: echoes the input variable 'message' as result 'echo'.
 *
 * Uso en processdef:
 *   service_task config: { "serviceCode": "bpm.test.echo" }
 *   Input variable: { "message": "hello world" }
 *   Resultado en processinstance.variables: { "echo": "hello world" }
 *
 * Útil para smoke E2E del ServiceTaskRegistry sin dependencias externas.
 */
@ServiceTask("bpm.test.echo")
public class EchoHandler implements ServiceTaskHandler {

    @Override
    public ServiceTaskResult execute(ServiceTaskContext ctx) {
        Object message = ctx.variable("message");
        Object inputFromConfig = ctx.config("input");   // alternativa: input fijo en el processdef
        Object echoValue = message != null ? message : inputFromConfig;
        return ServiceTaskResult.ok(Map.of(
            "echo", echoValue != null ? echoValue : "(no input)",
            "echoedBy", "bpm.test.echo",
            "echoedAt", java.time.Instant.now().toString()
        ));
    }
}
