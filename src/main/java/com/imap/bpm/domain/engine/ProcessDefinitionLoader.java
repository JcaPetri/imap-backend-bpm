package com.imap.bpm.domain.engine;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.imap.bpm.infrastructure.entity.MessageStartSubscription;
import com.imap.bpm.infrastructure.repository.MessageStartSubscriptionRepository;
import com.imap.platform.security.BearerTokenHolder;
import com.imap.bpm.infrastructure.security.BpmServiceTokenProvider;
import com.imap.platform.tenant.TenantContextHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.reactive.function.client.WebClient;

import java.time.Duration;
import java.time.OffsetDateTime;
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
 * Autenticación s2s: usa SIEMPRE el service token de BpmServiceTokenProvider
 * (no el bearer del request original). El endpoint del system está bajo
 * `/v1/admin/**` que requiere `system.admin`; un user normal lanzando una
 * instance NO tiene ese permiso, pero el service token sí. Fix 2026-05-19
 * commit que cierra bug: usuarios sin system.admin recibían 403 al arrancar
 * instances aunque tuvieran membership válida en el tenant del motor.
 *
 * Si service token está DISABLED (jwt.access.secret no configurado), fallback
 * al bearer del request — preserva comportamiento legacy de smoke tests.
 */
@Service
public class ProcessDefinitionLoader {

    private static final Logger log = LoggerFactory.getLogger(ProcessDefinitionLoader.class);

    private static final UUID DEFAULT_STATE_ACTIVE =
        UUID.fromString("019dc6f0-aa83-7333-8888-000000000001");

    private final WebClient http;
    private final ObjectMapper jackson;
    private final Cache<UUID, ProcessDefinition> cache;
    private final BpmServiceTokenProvider serviceTokenProvider;
    private final MessageStartSubscriptionRepository msgStartSubRepo;
    private final com.imap.eav.engine.context.EavTenantSession tenantSession;

    /**
     * Self-injection (lazy) para invocar syncMessageStartSubscriptions desde load()
     * de manera que el proxy de Spring aplique correctamente la @Transactional
     * (sin self, la self-invocation directa no abre tx y el tenantSession MANDATORY
     * falla + RLS deja el query EAV con 0 rows).
     */
    @org.springframework.context.annotation.Lazy
    @org.springframework.beans.factory.annotation.Autowired
    private ProcessDefinitionLoader self;

    public ProcessDefinitionLoader(@Value("${imap.system.base-url:http://localhost:8092/imap/system}")
                                   String systemBaseUrl,
                                   ObjectMapper jackson,
                                   BpmServiceTokenProvider serviceTokenProvider,
                                   MessageStartSubscriptionRepository msgStartSubRepo,
                                   com.imap.eav.engine.context.EavTenantSession tenantSession) {
        this.http = WebClient.builder().baseUrl(systemBaseUrl).build();
        this.jackson = jackson;
        this.serviceTokenProvider = serviceTokenProvider;
        this.msgStartSubRepo = msgStartSubRepo;
        this.tenantSession = tenantSession;
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
        log.info("ProcessDefinitionLoader cache MISS — fetching {} from system (tenantId={})",
            processVersionId, tenantId);

        // S2S al system con SERVICE TOKEN (no el bearer del request original).
        // El endpoint requiere system.admin que el service token siempre tiene.
        // Fallback al bearer del request si service token disabled (legacy).
        // tokenForTenant (no currentToken): el endpoint /v1/admin/bpm/** pasa por el
        // TenantContextFilter de system que valida membership por X-Tenant-Id. currentToken()
        // solo tiene SYSTEM_TENANT → el service account no es miembro del tenant del request
        // → 403 en cache-miss (hoy enmascarado por el cache Caffeine). Mismo fix que listProcessdefs.
        final String svcToken = tenantId != null
            ? serviceTokenProvider.tokenForTenant(tenantId)
            : serviceTokenProvider.currentToken();
        final String effectiveBearer = svcToken != null
            ? svcToken
            : (bearerToken != null ? bearerToken : BearerTokenHolder.get());
        Map<String, Object> resp = http.get()
            .uri("/v1/admin/bpm/processversion/{id}/full", processVersionId.toString())
            .headers(h -> {
                if (effectiveBearer != null) h.set(HttpHeaders.AUTHORIZATION, "Bearer " + effectiveBearer);
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

        // Fase 3 Día 4: populate message_start_subscription si hay startEvents con messageCode.
        // self.* para que el proxy de Spring aplique @Transactional + tenantSession (sin
        // proxy la self-invocation directa no abre tx).
        try {
            self.syncMessageStartSubscriptions(def, tenantId);
        } catch (Exception e) {
            // No queremos fallar el load si la sincro de subscriptions falla
            // (la subscription es opcional — el processdef sigue arrancable por versionId-start).
            log.warn("syncMessageStartSubscriptions failed for processVersion {}: {}",
                processVersionId, e.getMessage());
        }

        return def;
    }

    /** Invalida una entry del cache (uso futuro: cuando admin re-publica). */
    public void invalidate(UUID processVersionId) {
        cache.invalidate(processVersionId);
    }

    /** Stats para debugging / metrics. */
    public long cacheSize() { return cache.estimatedSize(); }

    /**
     * WorkHub — catálogo de processdefs del tenant (para "startable processes").
     * GET /v1/admin/bpm/processdef con SERVICE TOKEN (igual que load()). Cada
     * entry: {processdefId, code, name, description, lifecycle, currentVersionId,
     * [startPermission]}. `startPermission` es authoritative cuando system lo
     * agregue como atributo EAV del processdef; hasta entonces el caller usa una
     * convención. No se cachea (el catálogo cambia más que las versions).
     */
    @SuppressWarnings("unchecked")
    public List<Map<String, Object>> listProcessdefs(UUID tenantId) {
        // tokenForTenant (no currentToken): el catálogo de processdefs es tenant-scoped
        // y el TenantContextFilter de system valida membership por X-Tenant-Id. El
        // currentToken() solo tiene SYSTEM_TENANT → el service account no es "miembro"
        // del tenant del user → 403. tokenForTenant mintea un token miembro del tenant.
        final String svcToken = tenantId != null
            ? serviceTokenProvider.tokenForTenant(tenantId)
            : serviceTokenProvider.currentToken();
        final String effectiveBearer = svcToken != null ? svcToken : BearerTokenHolder.get();
        List<Map<String, Object>> resp = http.get()
            .uri("/v1/admin/bpm/processdef")
            .headers(h -> {
                if (effectiveBearer != null) h.set(HttpHeaders.AUTHORIZATION, "Bearer " + effectiveBearer);
                if (tenantId != null)        h.set("X-Tenant-Id", tenantId.toString());
            })
            .retrieve()
            .bodyToMono(List.class)
            .block();
        return resp == null ? List.of() : resp;
    }

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

    /**
     * Fase 3 Día 4: para cada startEvent del processdef con config.message.messageCode,
     * UPSERT una subscription en bpm_pro_message_start_subscription_tbl y desactiva
     * las viejas del mismo (tenant, message_code, processdef_id) (V1: solo current
     * version dispara por message-start).
     *
     * Si tenantId es null → skip (load anonymous probablemente, no tiene contexto
     * tenant para asociar la subscription).
     */
    /**
     * REQUIRES_NEW: abre tx nueva (no read-only) para los UPSERT/UPDATE de subscriptions.
     * Necesario porque load() se invoca desde endpoints @Transactional(readOnly=true)
     * como listMyTasks y getInstance — sin REQUIRES_NEW falla con
     * "cannot execute UPDATE in a read-only transaction" + rollback de la tx outer.
     */
    @org.springframework.transaction.annotation.Transactional(
        propagation = org.springframework.transaction.annotation.Propagation.REQUIRES_NEW)
    public void syncMessageStartSubscriptions(ProcessDefinition def, UUID tenantId) {
        if (tenantId == null) tenantId = TenantContextHolder.get();
        if (tenantId == null || def.processdefId() == null) return;

        // RLS: tabla bpm_pro_message_start_subscription_tbl tiene RLS habilitada
        // (Fase 4 Día 0). Sin aplicar tenantSession los queries devuelven 0 rows.
        tenantSession.applyToCurrentTransaction();

        for (ProcessDefinition.FlowElement fe : def.flowElements()) {
            if (!"start_event".equals(fe.type())) continue;

            String messageCode = extractMessageCode(fe.config());
            if (messageCode == null) continue;

            // 1. UPSERT: si ya existe la subscription para esta version+startEvent, marcarla active.
            //    Si no existe, crearla.
            Optional<MessageStartSubscription> existing = msgStartSubRepo.findForUpsert(
                tenantId, messageCode, def.processVersionId(), fe.id());
            MessageStartSubscription sub = existing.orElseGet(() -> {
                MessageStartSubscription s = new MessageStartSubscription();
                s.setId(UUID.randomUUID());
                s.setTenantId(TenantContextHolder.get());
                s.setMessageCode(messageCode);
                s.setProcessdefId(def.processdefId());
                s.setProcessversionId(def.processVersionId());
                s.setStartFlowElementId(fe.id());
                s.setStateId(DEFAULT_STATE_ACTIVE);
                s.setCreatedAt(OffsetDateTime.now());
                return s;
            });
            sub.setActive(true);
            sub.setUpdatedAt(OffsetDateTime.now());
            msgStartSubRepo.save(sub);

            // 2. Deactivate older subscriptions of same (tenant, messageCode, processdefId)
            //    pero con una processversionId distinta.
            int deactivated = msgStartSubRepo.deactivateOldVersions(
                tenantId, messageCode, def.processdefId(), def.processVersionId());
            if (deactivated > 0) {
                log.info("Deactivated {} older message-start subscriptions for processdef {} message '{}'",
                    deactivated, def.processdefCode(), messageCode);
            }

            log.info("Synced message-start subscription: tenant {} message '{}' → processVersion {} (startEvent {})",
                tenantId, messageCode, def.processVersionId(), fe.id());
        }
    }

    /**
     * Extrae el message code de un config flowElement.
     * Soporta: config.message.messageCode (anidado) y config.messageCode (flat).
     */
    @SuppressWarnings("unchecked")
    private String extractMessageCode(Map<String, Object> config) {
        if (config == null || config.isEmpty()) return null;
        Object messageObj = config.get("message");
        if (messageObj instanceof Map) {
            Object code = ((Map<String, Object>) messageObj).get("messageCode");
            if (code != null) return code.toString();
        }
        Object flat = config.get("messageCode");
        return flat == null ? null : flat.toString();
    }

    private int intVal(Object o, int dflt) {
        if (o == null) return dflt;
        try { return Integer.parseInt(o.toString()); }
        catch (NumberFormatException e) { return dflt; }
    }
}
