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

package com.imap.bpm.domain.engine;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.UUID;

/**
 * Carga `DecisionDefinition` (DMN) desde las TABLAS RELACIONALES locales de bpm
 * (via {@link LocalDefinitionReader}) + cache Caffeine.
 *
 * Post-cutover de ownership BPM (2026-07-01): las decisiondefs viven en bpm
 * (bpm_dmn_decisiondef_tbl / bpm_dmn_rule_tbl, V015). Ya NO hay HTTP a system.
 */
@Service
public class DecisionDefinitionLoader {

    private static final Logger log = LoggerFactory.getLogger(DecisionDefinitionLoader.class);

    private final Cache<String, DecisionDefinition> cache;
    private final LocalDefinitionReader localReader;

    public DecisionDefinitionLoader(LocalDefinitionReader localReader) {
        this.localReader = localReader;
        this.cache = Caffeine.newBuilder()
            .maximumSize(200)
            .expireAfterWrite(Duration.ofDays(365))
            .build();
    }

    /**
     * Devuelve la DecisionDefinition cacheada o la lee de las tablas relacionales
     * locales. `bearerToken` queda por compatibilidad de firma (ya no se usa).
     * Corre bajo la tx del caller (RLS por tenant; resuelve tenant propio o SYSTEM).
     */
    public DecisionDefinition load(String decisionCode, String bearerToken, UUID tenantId) {
        DecisionDefinition cached = cache.getIfPresent(decisionCode);
        if (cached != null) {
            log.debug("DecisionDefinitionLoader cache HIT: {}", decisionCode);
            return cached;
        }
        log.info("DecisionDefinitionLoader cache MISS — leyendo '{}' de las tablas relacionales bpm", decisionCode);

        DecisionDefinition def = localReader.loadDecisionDefinition(tenantId, decisionCode);
        if (def == null) {
            throw new IllegalStateException("decisiondef '" + decisionCode + "' no encontrada (local)");
        }
        cache.put(decisionCode, def);
        log.info("Cached decisiondef '{}' (rules={}, hitPolicy={})",
            decisionCode, def.rules().size(), def.hitPolicy());
        return def;
    }

    public void invalidate(String decisionCode) { cache.invalidate(decisionCode); }
    public long cacheSize() { return cache.estimatedSize(); }
}
