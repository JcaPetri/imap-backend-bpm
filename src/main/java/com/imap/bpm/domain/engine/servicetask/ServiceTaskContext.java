package com.imap.bpm.domain.engine.servicetask;

import com.imap.bpm.domain.engine.ProcessDefinition;
import com.imap.bpm.infrastructure.entity.ProcessInstance;
import com.imap.bpm.infrastructure.entity.Token;

import java.util.Map;
import java.util.UUID;

/**
 * Input que el motor BPM pasa al handler cuando ejecuta un service_task.
 *
 * Contiene todo lo que el handler puede necesitar:
 *   - serviceCode: cuál task se está ejecutando (un mismo handler puede atender N codes si lo registra dinámico)
 *   - flowElement: configuración del task del processdef (campos custom en flowElement.config)
 *   - instance: la ProcessInstance corriendo
 *   - token: el Token activo en este elemento
 *   - userId: usuario que disparó (puede ser null si es un timer / event)
 *   - bearerToken: JWT del usuario, para s2s remote calls (puede ser null)
 *   - variables: snapshot de las processinstance.variables al momento del invoke
 *
 * Inmutable. El handler no debe modificarlo — solo leer y producir un
 * {@link ServiceTaskResult}.
 */
public record ServiceTaskContext(
    String serviceCode,
    ProcessDefinition.FlowElement flowElement,
    ProcessInstance instance,
    Token token,
    UUID userId,
    String bearerToken,
    Map<String, Object> variables
) {
    /** Helper: lee un config del flowElement.config (puede ser null). */
    public Object config(String key) {
        if (flowElement == null || flowElement.config() == null) return null;
        return flowElement.config().get(key);
    }

    /** Helper: lee una variable del processinstance (puede ser null). */
    public Object variable(String key) {
        if (variables == null) return null;
        return variables.get(key);
    }
}
