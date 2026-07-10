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

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Registry + Dispatcher de service_task handlers.
 *
 * Tres mecanismos de resolución por serviceCode:
 *   1. LOCAL: si hay un &#64;Component &#64;ServiceTask("X.Y.Z") en el classpath del BPM, lo invoca direct.
 *   2. REMOTE: si NO hay local, mira el prefix (parte antes del primer dot) y rutea a
 *      la base URL configurada en {@code bpm.service-tasks.remotes.<prefix>}.
 *      Llama POST {baseUrl}/v1/service-tasks/execute con el body estándar.
 *   3. FALLBACK: si NO hay local NI remote, loguea warning y devuelve SUCCESS (compat
 *      con V1 MVP donde todos los service_tasks eran "log + advance").
 *
 * Auto-discovery de locales: al &#64;PostConstruct escanea beans con &#64;ServiceTask y
 * los indexea en {@code localHandlers}.
 *
 * Remote calls: usan WebClient (reactive client de Spring) con timeout configurable.
 * Si fallan con 5xx → cuenta como FAILURE del task → motor reintenta. Si fallan con
 * 4xx (mal request) → también FAILURE pero típicamente no se reintenta porque va a
 * volver a fallar (V2: distinguir 4xx vs 5xx para retry decisions).
 *
 * El service token (JWT) se inyecta en el header Authorization de los remote calls
 * via {@code BpmServiceTokenProvider} (mismo patrón que ProcessDefinitionLoader).
 */
@Service
public class ServiceTaskRegistry {

    private static final Logger log = LoggerFactory.getLogger(ServiceTaskRegistry.class);

    private final ApplicationContext appContext;
    private final ObjectMapper jackson;
    private final Map<String, String> remoteBaseUrls;
    private final Map<String, ServiceTaskHandler> localHandlers = new HashMap<>();
    private final int timeoutSeconds;
    private final WebClient http;

    public ServiceTaskRegistry(
            ApplicationContext appContext,
            ObjectMapper jackson,
            @Value("#{${bpm.service-tasks.remotes:{:}}}") Map<String, String> remoteBaseUrls,
            @Value("${bpm.service-tasks.timeout-seconds:30}") int timeoutSeconds) {
        this.appContext = appContext;
        this.jackson = jackson;
        this.remoteBaseUrls = remoteBaseUrls == null ? Collections.emptyMap() : remoteBaseUrls;
        this.timeoutSeconds = timeoutSeconds;
        this.http = WebClient.builder().build();
    }

    @PostConstruct
    void discoverLocalHandlers() {
        Map<String, Object> beans = appContext.getBeansWithAnnotation(ServiceTask.class);
        for (Map.Entry<String, Object> entry : beans.entrySet()) {
            Object bean = entry.getValue();
            if (!(bean instanceof ServiceTaskHandler)) {
                log.warn("@ServiceTask bean '{}' does NOT implement ServiceTaskHandler — skipping", entry.getKey());
                continue;
            }
            ServiceTask ann = bean.getClass().getAnnotation(ServiceTask.class);
            if (ann == null) {
                // Could be a CGLIB proxy — try superclass
                Class<?> superClass = bean.getClass().getSuperclass();
                if (superClass != null) ann = superClass.getAnnotation(ServiceTask.class);
            }
            if (ann == null) {
                log.warn("Bean '{}' missing @ServiceTask annotation (proxy issue?) — skipping", entry.getKey());
                continue;
            }
            String code = ann.value();
            ServiceTaskHandler previous = localHandlers.put(code, (ServiceTaskHandler) bean);
            if (previous != null) {
                log.error("Duplicate @ServiceTask('{}') — beans '{}' and the previous one collide. Last wins.", code, entry.getKey());
            }
            log.info("Registered LOCAL service task handler: '{}' → {}", code, bean.getClass().getSimpleName());
        }
        log.info("ServiceTaskRegistry initialized: {} local handlers, {} remote prefixes ({}), timeout={}s",
            localHandlers.size(), remoteBaseUrls.size(), remoteBaseUrls.keySet(), timeoutSeconds);
    }

    /**
     * Dispatch un service task. Resuelve via local → remote → fallback.
     * Este método NO maneja retry: eso lo hace {@link ServiceTaskRunner}.
     */
    public ServiceTaskResult dispatch(ServiceTaskContext ctx) {
        String code = ctx.serviceCode();
        if (code == null || code.isBlank()) {
            log.warn("service_task without serviceCode in flowElement '{}' — fallback SUCCESS",
                ctx.flowElement() != null ? ctx.flowElement().code() : "?");
            return ServiceTaskResult.ok();
        }

        // 1. Local handler?
        ServiceTaskHandler local = localHandlers.get(code);
        if (local != null) {
            log.debug("Dispatching LOCAL handler for '{}'", code);
            return safeInvoke(local, ctx);
        }

        // 2. Remote handler by prefix?
        int dot = code.indexOf('.');
        String prefix = dot > 0 ? code.substring(0, dot) : code;
        String baseUrl = remoteBaseUrls.get(prefix);
        if (baseUrl != null) {
            log.debug("Dispatching REMOTE handler for '{}' to baseUrl={}", code, baseUrl);
            return invokeRemote(baseUrl, ctx);
        }

        // 3. Fallback (compat V1)
        log.warn("No handler for serviceCode '{}' (local nor remote prefix '{}') — fallback SUCCESS",
            code, prefix);
        return ServiceTaskResult.ok();
    }

    private ServiceTaskResult safeInvoke(ServiceTaskHandler handler, ServiceTaskContext ctx) {
        try {
            ServiceTaskResult r = handler.execute(ctx);
            return r != null ? r : ServiceTaskResult.fail("HANDLER_NULL", "Handler returned null");
        } catch (Exception e) {
            log.error("Local handler '{}' threw exception", ctx.serviceCode(), e);
            return ServiceTaskResult.fail("HANDLER_EXCEPTION", e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    private ServiceTaskResult invokeRemote(String baseUrl, ServiceTaskContext ctx) {
        // Body: solo lo serializable (no la entity completa). El destino tiene
        // suficiente info para hacer su trabajo: serviceCode + config + ids.
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("serviceCode", ctx.serviceCode());
        body.put("processInstanceId", ctx.instance() != null ? ctx.instance().getId().toString() : null);
        body.put("processVersionId", ctx.instance() != null ? ctx.instance().getProcessversionId().toString() : null);
        body.put("tenantId", ctx.instance() != null ? ctx.instance().getTenantId().toString() : null);
        body.put("tokenId", ctx.token() != null ? ctx.token().getId().toString() : null);
        body.put("flowElementCode", ctx.flowElement() != null ? ctx.flowElement().code() : null);
        body.put("flowElementConfig", ctx.flowElement() != null ? ctx.flowElement().config() : Collections.emptyMap());
        body.put("userId", ctx.userId() != null ? ctx.userId().toString() : null);
        body.put("variables", ctx.variables() != null ? ctx.variables() : Collections.emptyMap());
        // 4.3 — idempotency-key: viaja en el body + header para que el receptor deduplique
        // (at-least-once + receptor idempotente = exactly-once). Estable entre los retries.
        String idemKey = ctx.idempotencyKey() != null ? ctx.idempotencyKey() : java.util.UUID.randomUUID().toString();
        body.put("idempotencyKey", idemKey);

        try {
            Map<String, Object> respBody = http.post()
                .uri(baseUrl + "/v1/service-tasks/execute")
                .headers(h -> {
                    h.setContentType(MediaType.APPLICATION_JSON);
                    h.set("Idempotency-Key", idemKey);
                    if (ctx.bearerToken() != null) {
                        h.set(HttpHeaders.AUTHORIZATION, "Bearer " + ctx.bearerToken());
                    }
                    if (ctx.instance() != null) {
                        h.set("X-Tenant-Id", ctx.instance().getTenantId().toString());
                    }
                })
                .bodyValue(body)
                .retrieve()
                .bodyToMono(Map.class)
                .timeout(Duration.ofSeconds(timeoutSeconds))
                .block();

            return parseRemoteResponse(respBody);
        } catch (WebClientResponseException e) {
            String errBody = e.getResponseBodyAsString();
            log.error("Remote handler '{}' returned HTTP {}: {}", ctx.serviceCode(), e.getRawStatusCode(), errBody);
            return ServiceTaskResult.fail("REMOTE_HTTP_" + e.getRawStatusCode(), errBody);
        } catch (Exception e) {
            log.error("Remote handler '{}' threw exception", ctx.serviceCode(), e);
            return ServiceTaskResult.fail("REMOTE_EXCEPTION", e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    @SuppressWarnings("unchecked")
    private ServiceTaskResult parseRemoteResponse(Map<String, Object> resp) {
        if (resp == null) return ServiceTaskResult.fail("REMOTE_NULL", "Remote returned null body");
        String statusStr = (String) resp.getOrDefault("status", "SUCCESS");
        ServiceTaskResult.Status status;
        try {
            status = ServiceTaskResult.Status.valueOf(statusStr.toUpperCase());
        } catch (IllegalArgumentException e) {
            return ServiceTaskResult.fail("REMOTE_BAD_STATUS", "Unknown status: " + statusStr);
        }
        Map<String, Object> vars = (Map<String, Object>) resp.getOrDefault("resultVariables", Collections.emptyMap());
        String errorCode = (String) resp.get("errorCode");
        String errorMessage = (String) resp.get("errorMessage");
        String boundaryErrorCode = (String) resp.get("boundaryErrorCode");
        return new ServiceTaskResult(status, vars, errorCode, errorMessage, boundaryErrorCode);
    }

    // ─── Introspección útil para diagnósticos ──────────────────────────────────

    public boolean hasLocalHandler(String serviceCode) { return localHandlers.containsKey(serviceCode); }
    public boolean hasRemotePrefix(String prefix)      { return remoteBaseUrls.containsKey(prefix); }
    public int localHandlerCount()                     { return localHandlers.size(); }
    public Map<String, String> getRemoteBaseUrls()     { return Collections.unmodifiableMap(remoteBaseUrls); }
}
