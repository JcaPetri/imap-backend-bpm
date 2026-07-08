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

package com.imap.bpm.application.engine;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.imap.bpm.infrastructure.repository.MessageStartSubscriptionRepository;
import com.imap.platform.tenant.TenantContextHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.UUID;

/**
 * Carga `ProcessDefinition` desde las TABLAS RELACIONALES locales de bpm
 * (via {@link LocalDefinitionReader}) y cachea en Caffeine.
 *
 * Post-cutover de ownership BPM (2026-07-01): bpm es dueño de sus defs
 * (schema bpm, V015). Ya NO hay HTTP a system ni fuente conmutable — la lectura
 * es siempre local. El cache-invalidate manual sigue disponible para cuando el
 * admin re-publica una version.
 */
@Service
public class ProcessDefinitionLoader {

    private static final Logger log = LoggerFactory.getLogger(ProcessDefinitionLoader.class);

    private static final UUID DEFAULT_STATE_ACTIVE =
        UUID.fromString("019dc6f0-aa83-7333-8888-000000000001");

    private final Cache<UUID, ProcessDefinition> cache;
    private final MessageStartSubscriptionRepository msgStartSubRepo;
    private final com.imap.eav.engine.context.EavTenantSession tenantSession;
    private final LocalDefinitionReader localReader;

    /**
     * Self-injection (lazy) para invocar syncMessageStartSubscriptions desde load()
     * de manera que el proxy de Spring aplique correctamente la @Transactional
     * (sin self, la self-invocation directa no abre tx y el tenantSession MANDATORY
     * falla + RLS deja el query con 0 rows).
     */
    @org.springframework.context.annotation.Lazy
    @org.springframework.beans.factory.annotation.Autowired
    private ProcessDefinitionLoader self;

    public ProcessDefinitionLoader(MessageStartSubscriptionRepository msgStartSubRepo,
                                   com.imap.eav.engine.context.EavTenantSession tenantSession,
                                   LocalDefinitionReader localReader) {
        this.msgStartSubRepo = msgStartSubRepo;
        this.tenantSession = tenantSession;
        this.localReader = localReader;
        this.cache = Caffeine.newBuilder()
            .maximumSize(200)
            .expireAfterWrite(Duration.ofDays(365))   // efectivamente infinito
            .build();
    }

    /**
     * Devuelve la ProcessDefinition cacheada o la lee de las tablas relacionales
     * locales si miss. `bearerToken` queda por compatibilidad de firma (ya no se usa
     * — no hay HTTP). Corre bajo la tx del caller (RLS por tenant).
     */
    public ProcessDefinition load(UUID processVersionId, String bearerToken, UUID tenantId) {
        ProcessDefinition cached = cache.getIfPresent(processVersionId);
        if (cached != null) {
            log.debug("ProcessDefinitionLoader cache HIT: {}", processVersionId);
            return cached;
        }
        log.info("ProcessDefinitionLoader cache MISS — leyendo {} de las tablas relacionales bpm", processVersionId);

        ProcessDefinition def = localReader.loadProcessDefinition(processVersionId);
        if (def == null) {
            throw new IllegalStateException("processVersion " + processVersionId + " no encontrado (local)");
        }
        cache.put(processVersionId, def);
        log.info("Cached processVersion {} ({} flowElements, {} sequenceFlows, {} taskForms)",
            processVersionId, def.flowElements().size(), def.sequenceFlows().size(), def.taskForms().size());

        // Fase 3 Día 4: populate message_start_subscription si hay startEvents con messageCode.
        // self.* para que el proxy de Spring aplique @Transactional + tenantSession.
        try {
            self.syncMessageStartSubscriptions(def, tenantId);
        } catch (Exception e) {
            log.warn("syncMessageStartSubscriptions failed for processVersion {}: {}",
                processVersionId, e.getMessage());
        }

        return def;
    }

    /** Invalida una entry del cache (cuando admin re-publica una version). */
    public void invalidate(UUID processVersionId) {
        cache.invalidate(processVersionId);
    }

    /** Stats para debugging / metrics. */
    public long cacheSize() { return cache.estimatedSize(); }

    /**
     * Para cada startEvent del processdef con config.message.messageCode, UPSERT una
     * subscription en bpm_pro_message_start_subscription_tbl y desactiva las viejas
     * del mismo (tenant, message_code, processdef_id). Si tenantId es null → skip.
     *
     * REQUIRES_NEW: abre tx nueva (no read-only) para los UPSERT/UPDATE; load() se
     * invoca desde endpoints @Transactional(readOnly=true) (listMyTasks, getInstance).
     */
    @org.springframework.transaction.annotation.Transactional(
        propagation = org.springframework.transaction.annotation.Propagation.REQUIRES_NEW)
    public void syncMessageStartSubscriptions(ProcessDefinition def, UUID tenantId) {
        if (tenantId == null) tenantId = TenantContextHolder.get();
        if (tenantId == null || def.processdefId() == null) return;

        tenantSession.applyToCurrentTransaction();

        for (ProcessDefinition.FlowElement fe : def.flowElements()) {
            if (!"start_event".equals(fe.type())) continue;

            String messageCode = extractMessageCode(fe.config());
            if (messageCode == null) continue;

            msgStartSubRepo.upsertActive(
                UUID.randomUUID(), tenantId, messageCode, def.processdefId(),
                def.processVersionId(), fe.id(), DEFAULT_STATE_ACTIVE);

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

    /** Extrae el message code de un config flowElement (anidado config.message.messageCode o flat config.messageCode). */
    @SuppressWarnings("unchecked")
    private String extractMessageCode(java.util.Map<String, Object> config) {
        if (config == null || config.isEmpty()) return null;
        Object messageObj = config.get("message");
        if (messageObj instanceof java.util.Map) {
            Object code = ((java.util.Map<String, Object>) messageObj).get("messageCode");
            if (code != null) return code.toString();
        }
        Object flat = config.get("messageCode");
        return flat == null ? null : flat.toString();
    }
}
