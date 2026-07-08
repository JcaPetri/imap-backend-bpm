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

package com.imap.bpm.infrastructure.persistence;

import com.imap.bpm.domain.model.WhbTenantConfig;
import com.imap.bpm.infrastructure.entity.WhbTenantConfigEntity;
import org.springframework.stereotype.Component;

/** Traduce entre WhbTenantConfigEntity (JPA) y el modelo de dominio WhbTenantConfig. */
@Component
public class WhbTenantConfigMapper {

    public WhbTenantConfig toDomain(WhbTenantConfigEntity e) {
        return new WhbTenantConfig(e.getId(), e.getTenantId(), e.getAggregationMode(),
            e.getWeightGravity(), e.getWeightUrgency(), e.getWeightTrend(),
            e.getThresholdHighPct(), e.getThresholdMediumPct(), e.getStateId(),
            e.getCreatedAt(), e.getCreatedById(), e.getUpdatedAt(), e.getUpdatedById(), e.getOwnedById());
    }

    /** Copia los campos del modelo sobre la entity (nueva o gestionada). Audit updatable=false los ignora Hibernate en UPDATE. */
    public void apply(WhbTenantConfig c, WhbTenantConfigEntity e) {
        e.setId(c.getId());
        e.setTenantId(c.getTenantId());
        e.setAggregationMode(c.getAggregationMode());
        e.setWeightGravity(c.getWeightGravity());
        e.setWeightUrgency(c.getWeightUrgency());
        e.setWeightTrend(c.getWeightTrend());
        e.setThresholdHighPct(c.getThresholdHighPct());
        e.setThresholdMediumPct(c.getThresholdMediumPct());
        e.setStateId(c.getStateId());
        e.setCreatedAt(c.getCreatedAt());
        e.setCreatedById(c.getCreatedById());
        e.setUpdatedAt(c.getUpdatedAt());
        e.setUpdatedById(c.getUpdatedById());
        e.setOwnedById(c.getOwnedById());
    }
}
