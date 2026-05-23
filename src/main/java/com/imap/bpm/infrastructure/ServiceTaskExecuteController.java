package com.imap.bpm.infrastructure;

import com.imap.bpm.domain.engine.servicetask.ServiceTaskContext;
import com.imap.bpm.domain.engine.servicetask.ServiceTaskRegistry;
import com.imap.bpm.domain.engine.servicetask.ServiceTaskResult;
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
