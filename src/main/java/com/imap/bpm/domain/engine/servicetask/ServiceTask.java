package com.imap.bpm.domain.engine.servicetask;

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
