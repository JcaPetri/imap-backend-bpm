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
//  • [controller] DTOs, nunca exponer entidades del domain en la API
//  • [bpm] Orquestador central (tipo Temporal/Step Functions)
//  • [bpm] processdef→processversion: snapshot inmutable al activar
//  • [bpm] Las defs de proceso viven acá (no en system)
//  • [bpm] Camunda 8 como norte; interim form-driven en prod
// ─── GOLDEN-RULES:END ───

package com.imap.bpm.infrastructure;

import com.imap.bpm.application.engine.servicetask.ServiceTaskContext;
import com.imap.bpm.application.engine.servicetask.ServiceTaskRegistry;
import com.imap.bpm.application.engine.servicetask.ServiceTaskResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Endpoint standard que cada microservicio que provee service_tasks debe exponer
 * (BPM lo expone para sus handlers locales — sirve como referencia + sanity check
 * desde curl/Postman).
 *
 * Cuando otros microservicios (inventory, system, sales, ...) registran handlers
 * propios, clonan esta controller en su jar — el body shape se mantiene
 * exactamente igual para que BPM pueda hacer s2s con el mismo contrato.
 *
 * POST /v1/service-tasks/execute
 *   Body  : {serviceCode, processInstanceId?, processVersionId?, tenantId?, tokenId?,
 *            flowElementCode?, flowElementConfig?, userId?, variables?}
 *   Output: {status: SUCCESS|FAILURE|PENDING, resultVariables, errorCode, errorMessage,
 *            boundaryErrorCode}
 *
 * NOTA: este endpoint NO crea processinstances ni invoca el motor. Solo dispatcha
 * el handler local correspondiente al serviceCode. Si el handler necesita acceso
 * a la entity {@link com.imap.bpm.infrastructure.entity.ProcessInstance} completa,
 * tiene que consultarla via repository — el body solo trae IDs por simplicidad.
 */
@RestController
@RequestMapping("/v1/service-tasks")
public class ServiceTaskExecuteController {

    private static final Logger log = LoggerFactory.getLogger(ServiceTaskExecuteController.class);

    private final ServiceTaskRegistry registry;

    public ServiceTaskExecuteController(ServiceTaskRegistry registry) {
        this.registry = registry;
    }

    @PostMapping("/execute")
    public ResponseEntity<Map<String, Object>> execute(@RequestBody Map<String, Object> body) {
        String serviceCode = (String) body.get("serviceCode");
        log.info("ServiceTaskExecuteController.execute() serviceCode='{}'", serviceCode);

        // Construir un context "remote" — instance/token van null porque este endpoint
        // se llama típicamente desde BPM (que ya tiene la entity) o desde tests. Los
        // handlers locales que necesitan el entity object deben fetcharlo via repo
        // usando los IDs del body.
        ServiceTaskContext ctx = new ServiceTaskContext(
            serviceCode,
            null,                                                    // flowElement no se reconstruye
            null,                                                    // instance no se reconstruye (los handlers usan tenantId del body)
            null,                                                    // token no se reconstruye
            null,                                                    // userId
            null,                                                    // bearerToken (lo lleva el JWT del request si aplica)
            asMap(body.get("variables"))
        );

        ServiceTaskResult result = registry.dispatch(ctx);
        return ResponseEntity.ok(toResponseMap(result));
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> asMap(Object o) {
        return o instanceof Map ? (Map<String, Object>) o : new LinkedHashMap<>();
    }

    private static Map<String, Object> toResponseMap(ServiceTaskResult r) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("status", r.status().name());
        out.put("resultVariables", r.resultVariables() == null ? new LinkedHashMap<>() : r.resultVariables());
        if (r.errorCode() != null)         out.put("errorCode", r.errorCode());
        if (r.errorMessage() != null)      out.put("errorMessage", r.errorMessage());
        if (r.boundaryErrorCode() != null) out.put("boundaryErrorCode", r.boundaryErrorCode());
        return out;
    }
}
