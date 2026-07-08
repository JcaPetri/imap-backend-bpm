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

import org.springframework.stereotype.Component;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marca una clase como handler de un service_task BPM.
 *
 * Auto-discovery: la {@link ServiceTaskRegistry} escanea al startup todas las
 * @Component que también tengan @ServiceTask y las registra por {@code value()}.
 *
 * Convención de naming del serviceCode: {@code <module>.<area>.<verb>}
 *   bpm.test.echo
 *   bpm.notify.email
 *   inventory.stock.reserve     (este NO va a estar en BPM, va a estar en inventory)
 *   system.entity.create        (este NO va a estar en BPM)
 *
 * Cuando el motor encuentra un service_task con serviceCode='inventory.X.Y' y
 * NO hay handler local, busca por prefix 'inventory' en
 * {@code bpm.service-tasks.remotes.inventory} y rutea via REST.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Component
public @interface ServiceTask {
    /** ServiceCode que maneja este handler. Ej: "bpm.test.echo" */
    String value();
}
