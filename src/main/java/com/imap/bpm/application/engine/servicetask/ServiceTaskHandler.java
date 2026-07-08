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

/**
 * Handler de un service_task BPM.
 *
 * Implementaciones deben:
 *   1. Estar anotadas con {@link ServiceTask}(value="module.area.verb")
 *   2. Ser @Component (Spring-managed) — la annotation @ServiceTask es meta-component
 *   3. Implementar {@link #execute(ServiceTaskContext)} retornando un {@link ServiceTaskResult}
 *   4. Ser thread-safe (el motor puede ejecutar múltiples handlers en paralelo)
 *   5. Idempotentes (el motor reintenta failure → no duplicar side effects)
 *
 * Ejemplo:
 * <pre>
 * &#64;ServiceTask("bpm.notify.email")
 * public class EmailNotifyHandler implements ServiceTaskHandler {
 *     &#64;Override
 *     public ServiceTaskResult execute(ServiceTaskContext ctx) {
 *         String to = (String) ctx.config("to");
 *         String subject = (String) ctx.config("subject");
 *         // ... send email ...
 *         return ServiceTaskResult.ok(Map.of("emailSentAt", Instant.now().toString()));
 *     }
 * }
 * </pre>
 */
@FunctionalInterface
public interface ServiceTaskHandler {
    ServiceTaskResult execute(ServiceTaskContext ctx);
}
