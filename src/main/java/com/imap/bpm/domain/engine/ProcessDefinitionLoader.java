package com.imap.bpm.domain.engine;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

import java.time.Duration;
import java.util.*;

/**
 * Carga `ProcessDefinition` desde el endpoint del system y cachea en Caffeine.
 *
 * TTL infinito: las processversions son LOCK-once-used (no cambian). Solo
 * invalidamos manualmente si admin re-publica una version (raro). Cache max
 * 200 entries (más que suficiente para un MVP).
 *
 * Llama: GET https://imaps.com.ar/imap/system/v1/admin/bpm/processversion/{id}/full
 *
 * El JWT se propaga del request actual via Authorization header del WebClient
 * (Spring WebFlux). En MVP usamos el header del request bpm (no service-account).
 */
@Service
public class ProcessDefinitionLoader {

    private static final Logger log = LoggerFactory.getLogger(ProcessDefinitionLoader.class);

    private final WebClient http;
    private final ObjectMapper jackson;
    private final Cache<UUID, ProcessDefinition> cache;

    public ProcessDefinitionLoader(@Value("${imap.system.base-url:http://localhost:8092/imap/system}")
                                   String systemBaseUrl,
                                   ObjectMapper jackson) {
        this.http = WebClient.builder().baseUrl(systemBaseUrl).build();
        this.jackson = jackson;
        this.cache = Caffeine.newBuilder()
            .maximumSize(200)
            .expireAfterWrite(Duration.ofDays(365))   // efectivamente infinito
            .build();
    }

    /**
     * Devuelve la ProcessDefinition cacheada o la fetcha de system si miss.
     * Pasa el Authorization + X-Tenant-Id del request actual (propagación JWT).
     */
    public ProcessDefinition load(UUID processVersionId, String bearerToken, UUID tenantId) {
        ProcessDefinition cached = cache.getIfPresent(processVersionId);
        if (cached != null) {
            log.debug("ProcessDefinitionLoader cache HIT: {}", processVersionId);
            return cached;
        }
        log.info("ProcessDefinitionLoader cache MISS — fetching {} from system", processVersionId);

        Map<String, Object> resp = http.get()
            .uri("/v1/admin/bpm/processversion/{id}/full", processVersionId.toString())
            .headers(h -> {
                if (bearerToken != null) h.set(HttpHeaders.AUTHORIZATION, "Bearer " + bearerToken);
                if (tenantId != null)    h.set("X-Tenant-Id", tenantId.toString());
            })
            .retrieve()
            .bodyToMono(Map.class)
            .block();

        if (resp == null || resp.get("error") != null) {
            throw new IllegalStateException("system returned error for processVersion " + processVersionId
                + ": " + (resp == null ? "no response" : resp.get("error")));
        }

        ProcessDefinition def = parse(processVersionId, resp);
        cache.put(processVersionId, def);
        log.info("Cached processVersion {} ({} flowElements, {} sequenceFlows, {} taskForms)",
            processVersionId, def.flowElements().size(), def.sequenceFlows().size(), def.taskForms().size());
        return def;
    }

    /** Invalida una entry del cache (uso futuro: cuando admin re-publica). */
    public void invalidate(UUID processVersionId) {
        cache.invalidate(processVersionId);
    }

    /** Stats para debugging / metrics. */
    public long cacheSize() { return cache.estimatedSize(); }

    // ─────────────────────────────────────────────────────────────────────────

    @SuppressWarnings("unchecked")
    private ProcessDefinition parse(UUID processVersionId, Map<String, Object> resp) {
        Map<String, Object> pd = (Map<String, Object>) resp.get("processdef");
        UUID processdefId = pd != null && pd.get("id") != null
            ? UUID.fromString(pd.get("id").toString()) : null;
        String processdefCode = pd != null ? (String) pd.get("code") : null;
        String processdefName = pd != null ? (String) pd.get("name") : null;
        int version = resp.get("version") == null ? 0
            : Integer.parseInt(resp.get("version").toString());

        List<ProcessDefinition.FlowElement> fes = new ArrayList<>();
        for (Map<String, Object> raw : (List<Map<String, Object>>) resp.getOrDefault("flowElements", List.of())) {
            Map<String, Object> config = parseJson((String) raw.get("config"));
            fes.add(new ProcessDefinition.FlowElement(
                UUID.fromString((String) raw.get("id")),
                (String) raw.get("code"),
                (String) raw.get("type"),
                (String) raw.get("name"),
                config,
                intVal(raw.get("sortOrder"), Integer.MAX_VALUE)
            ));
        }

        List<ProcessDefinition.SequenceFlow> sfs = new ArrayList<>();
        for (Map<String, Object> raw : (List<Map<String, Object>>) resp.getOrDefault("sequenceFlows", List.of())) {
            sfs.add(new ProcessDefinition.SequenceFlow(
                UUID.fromString((String) raw.get("id")),
                UUID.fromString((String) raw.get("sourceId")),
                UUID.fromString((String) raw.get("targetId")),
                (String) raw.get("sourceCode"),
                (String) raw.get("targetCode"),
                (String) raw.get("conditionExpr"),
                intVal(raw.get("sortOrder"), Integer.MAX_VALUE)
            ));
        }

        List<ProcessDefinition.TaskForm> tfs = new ArrayList<>();
        for (Map<String, Object> raw : (List<Map<String, Object>>) resp.getOrDefault("taskForms", List.of())) {
            tfs.add(new ProcessDefinition.TaskForm(
                UUID.fromString((String) raw.get("flowElementId")),
                (String) raw.get("flowElementCode"),
                UUID.fromString((String) raw.get("entityDefId")),
                (String) raw.get("entityDefCode"),
                (String) raw.get("mode")
            ));
        }

        return new ProcessDefinition(
            processVersionId, processdefId, processdefCode, processdefName, version,
            fes, sfs, tfs
        );
    }

    private Map<String, Object> parseJson(String json) {
        if (json == null || json.isBlank()) return Map.of();
        try {
            return jackson.readValue(json, Map.class);
        } catch (Exception e) {
            log.warn("Could not parse JSON config: {}", json);
            return Map.of();
        }
    }

    private int intVal(Object o, int dflt) {
        if (o == null) return dflt;
        try { return Integer.parseInt(o.toString()); }
        catch (NumberFormatException e) { return dflt; }
    }
}
