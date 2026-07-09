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

import com.imap.bpm.application.engine.ProcessDefinition;
import com.imap.bpm.infrastructure.entity.ProcessInstanceEntity;
import com.imap.bpm.infrastructure.entity.TokenEntity;

import java.util.Map;
import java.util.UUID;

/**
 * Input que el motor BPM pasa al handler cuando ejecuta un service_task.
 *
 * Contiene todo lo que el handler puede necesitar:
 *   - serviceCode: cuál task se está ejecutando (un mismo handler puede atender N codes si lo registra dinámico)
 *   - flowElement: configuración del task del processdef (campos custom en flowElement.config)
 *   - instance: la ProcessInstanceEntity corriendo
 *   - token: el TokenEntity activo en este elemento
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
    ProcessInstanceEntity instance,
    TokenEntity token,
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
