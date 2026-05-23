package com.imap.bpm.domain.engine.servicetask;

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
