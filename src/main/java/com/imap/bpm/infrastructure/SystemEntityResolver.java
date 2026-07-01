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

package com.imap.bpm.infrastructure;

import com.imap.bpm.infrastructure.security.BpmServiceTokenProvider;
import com.imap.platform.security.BearerTokenHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Resuelve un entity_def de system (code → UUID) por HTTP s2s.
 *
 * bpm guarda en bpm_hum_taskform_tbl.entitydef_id la FK LÓGICA cross-service a
 * system.sys_entity_def. Como el code es la referencia natural que llega en el
 * payload del builder, acá lo resolvemos a su uuid vía el endpoint read-only de
 * system `GET /v1/entities/{code}` (devuelve EntityDefDto con campo `id`).
 *
 * Mismo patrón de auth s2s que ProcessDefinitionLoader: SERVICE TOKEN de
 * BpmServiceTokenProvider (tokenForTenant si hay tenant, si no currentToken) +
 * header X-Tenant-Id. Los entity_defs son estables (catálogo dogfooding), así
 * que cacheamos code→id en un ConcurrentHashMap simple.
 *
 * `resolveId` devuelve null si el entity_def no existe (404) — el caller decide
 * si eso es un error de validación.
 */
@Component
public class SystemEntityResolver {

    private static final Logger log = LoggerFactory.getLogger(SystemEntityResolver.class);

    private final WebClient http;
    private final BpmServiceTokenProvider serviceTokenProvider;
    private final ConcurrentHashMap<String, UUID> cache = new ConcurrentHashMap<>();

    public SystemEntityResolver(@Value("${imap.system.base-url:http://localhost:8092/imap/system}")
                                String systemBaseUrl,
                                BpmServiceTokenProvider serviceTokenProvider) {
        this.http = WebClient.builder().baseUrl(systemBaseUrl).build();
        this.serviceTokenProvider = serviceTokenProvider;
    }

    /**
     * Resuelve entityCode → entityDefId, o null si no existe.
     * Cacheado por code (los entity_defs no cambian de id).
     */
    @SuppressWarnings("unchecked")
    public UUID resolveId(String entityCode, UUID tenantId) {
        if (entityCode == null || entityCode.isBlank()) return null;
        UUID cached = cache.get(entityCode);
        if (cached != null) return cached;

        final String svcToken = tenantId != null
            ? serviceTokenProvider.tokenForTenant(tenantId)
            : serviceTokenProvider.currentToken();
        final String effectiveBearer = svcToken != null ? svcToken : BearerTokenHolder.get();

        Map<String, Object> resp;
        try {
            resp = http.get()
                .uri("/v1/entities/{code}", entityCode)
                .headers(h -> {
                    if (effectiveBearer != null) h.set(HttpHeaders.AUTHORIZATION, "Bearer " + effectiveBearer);
                    if (tenantId != null)        h.set("X-Tenant-Id", tenantId.toString());
                })
                .retrieve()
                .bodyToMono(Map.class)
                .block();
        } catch (org.springframework.web.reactive.function.client.WebClientResponseException.NotFound nf) {
            // 404 → entity_def no existe. No es error de infra: el caller valida.
            return null;
        } catch (Exception e) {
            log.warn("SystemEntityResolver: fallo resolviendo entityCode='{}' (tenant={}): {}",
                entityCode, tenantId, e.toString());
            return null;
        }

        if (resp == null || resp.get("id") == null) return null;
        UUID id;
        try {
            id = UUID.fromString(resp.get("id").toString());
        } catch (IllegalArgumentException e) {
            return null;
        }
        cache.put(entityCode, id);
        return id;
    }
}
