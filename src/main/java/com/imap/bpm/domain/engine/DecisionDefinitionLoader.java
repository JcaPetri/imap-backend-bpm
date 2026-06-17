package com.imap.bpm.domain.engine;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.imap.platform.security.BearerTokenHolder;
import com.imap.bpm.infrastructure.security.BpmServiceTokenProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

import java.time.Duration;
import java.util.*;

/**
 * Carga `DecisionDefinition` desde el endpoint del system + cache Caffeine.
 *
 * Llama: GET {system}/v1/admin/bpm/decisiondef/by-code/{code}/full
 *
 * Cache key = decisionCode (string). TTL infinito porque las decisiondefs
 * se versionan por code+revision en futuras iteraciones; cuando admin
 * publique una nueva versión, invalidamos explícitamente.
 *
 * NOTA: el endpoint del system devuelve `inputSchema`, `outputSchema`, y
 * los `inputs`/`outputs` de cada rule como JSON strings (porque vienen del
 * cell-store como text). Acá los parseamos a objetos tipados.
 */
@Service
public class DecisionDefinitionLoader {

    private static final Logger log = LoggerFactory.getLogger(DecisionDefinitionLoader.class);

    private final WebClient http;
    private final ObjectMapper jackson;
    private final Cache<String, DecisionDefinition> cache;
    private final BpmServiceTokenProvider serviceTokenProvider;

    public DecisionDefinitionLoader(@Value("${imap.system.base-url:http://localhost:8092/imap/system}")
                                    String systemBaseUrl,
                                    ObjectMapper jackson,
                                    BpmServiceTokenProvider serviceTokenProvider) {
        this.http = WebClient.builder().baseUrl(systemBaseUrl).build();
        this.jackson = jackson;
        this.serviceTokenProvider = serviceTokenProvider;
        this.cache = Caffeine.newBuilder()
            .maximumSize(200)
            .expireAfterWrite(Duration.ofDays(365))
            .build();
    }

    public DecisionDefinition load(String decisionCode, String bearerToken, UUID tenantId) {
        DecisionDefinition cached = cache.getIfPresent(decisionCode);
        if (cached != null) {
            log.debug("DecisionDefinitionLoader cache HIT: {}", decisionCode);
            return cached;
        }
        log.info("DecisionDefinitionLoader cache MISS — fetching '{}' from system", decisionCode);

        // S2S al system con SERVICE TOKEN — endpoint requiere system.admin que
        // el user request original puede no tener. Fix 2026-05-19, mismo
        // patrón que ProcessDefinitionLoader.
        // 3c.1 — tokenForTenant (no currentToken): el endpoint /v1/admin/bpm/**
        // valida membership por X-Tenant-Id; currentToken solo tiene SYSTEM_TENANT.
        final String svcToken = tenantId != null
            ? serviceTokenProvider.tokenForTenant(tenantId)
            : serviceTokenProvider.currentToken();
        final String effectiveBearer = svcToken != null
            ? svcToken
            : (bearerToken != null ? bearerToken : BearerTokenHolder.get());
        Map<String, Object> resp = http.get()
            .uri("/v1/admin/bpm/decisiondef/by-code/{code}/full", decisionCode)
            .headers(h -> {
                if (effectiveBearer != null) h.set(HttpHeaders.AUTHORIZATION, "Bearer " + effectiveBearer);
                if (tenantId != null) h.set("X-Tenant-Id", tenantId.toString());
            })
            .retrieve()
            .bodyToMono(Map.class)
            .block();

        if (resp == null || resp.get("error") != null) {
            throw new IllegalStateException("system returned error for decisionCode '" + decisionCode
                + "': " + (resp == null ? "no response" : resp.get("error")));
        }

        DecisionDefinition def = parse(resp);
        cache.put(decisionCode, def);
        log.info("Cached decisiondef '{}' (rules={}, hitPolicy={})",
            decisionCode, def.rules().size(), def.hitPolicy());
        return def;
    }

    public void invalidate(String decisionCode) { cache.invalidate(decisionCode); }
    public long cacheSize() { return cache.estimatedSize(); }

    // ─────────────────────────────────────────────────────────────────────────

    @SuppressWarnings("unchecked")
    private DecisionDefinition parse(Map<String, Object> resp) {
        UUID id = resp.get("id") == null ? null : UUID.fromString(resp.get("id").toString());
        String code = (String) resp.get("code");
        String name = (String) resp.get("name");
        String description = (String) resp.get("description");
        String hitPolicy = (String) resp.getOrDefault("hitPolicy", "first");

        List<DecisionDefinition.SchemaEntry> inputs  = parseSchema((String) resp.get("inputSchema"));
        List<DecisionDefinition.SchemaEntry> outputs = parseSchema((String) resp.get("outputSchema"));

        List<DecisionDefinition.Rule> rules = new ArrayList<>();
        for (Map<String, Object> raw : (List<Map<String, Object>>) resp.getOrDefault("rules", List.of())) {
            UUID rId = raw.get("id") == null ? null : UUID.fromString(raw.get("id").toString());
            int priority = intVal(raw.get("priority"), Integer.MAX_VALUE);

            List<DecisionDefinition.RuleInput> ruleInputs = new ArrayList<>();
            for (Map<String, Object> entry : parseList((String) raw.get("inputs"))) {
                ruleInputs.add(new DecisionDefinition.RuleInput(
                    (String) entry.get("var_name"),
                    (String) entry.get("operator"),
                    entry.get("value")
                ));
            }
            List<DecisionDefinition.RuleOutput> ruleOutputs = new ArrayList<>();
            for (Map<String, Object> entry : parseList((String) raw.get("outputs"))) {
                ruleOutputs.add(new DecisionDefinition.RuleOutput(
                    (String) entry.get("var_name"),
                    entry.get("value")
                ));
            }
            rules.add(new DecisionDefinition.Rule(
                rId, priority, ruleInputs, ruleOutputs, (String) raw.get("description")
            ));
        }
        return new DecisionDefinition(id, code, name, description, hitPolicy, inputs, outputs, rules);
    }

    private List<DecisionDefinition.SchemaEntry> parseSchema(String json) {
        List<DecisionDefinition.SchemaEntry> out = new ArrayList<>();
        for (Map<String, Object> entry : parseList(json)) {
            out.add(new DecisionDefinition.SchemaEntry(
                (String) entry.get("var_name"),
                (String) entry.get("type")
            ));
        }
        return out;
    }

    private List<Map<String, Object>> parseList(String json) {
        if (json == null || json.isBlank()) return List.of();
        try {
            return jackson.readValue(json, new TypeReference<List<Map<String, Object>>>() {});
        } catch (Exception e) {
            log.warn("DecisionDefinitionLoader: could not parse JSON list: {}", json);
            return List.of();
        }
    }

    private int intVal(Object o, int dflt) {
        if (o == null) return dflt;
        try { return Integer.parseInt(o.toString()); }
        catch (NumberFormatException e) { return dflt; }
    }
}
