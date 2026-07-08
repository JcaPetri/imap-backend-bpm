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

import com.imap.bpm.domain.model.WhbClassification;
import com.imap.bpm.infrastructure.entity.WhbClassificationEntity;
import org.springframework.stereotype.Component;

/** Traduce entre WhbClassificationEntity (JPA) y el modelo de dominio WhbClassification. */
@Component
public class WhbClassificationMapper {

    public WhbClassification toDomain(WhbClassificationEntity e) {
        return new WhbClassification(e.getId(), e.getTenantId(), e.getProcessdefId(),
            e.getProcessversionId(), e.getFlowelementId(), e.getGravity(), e.getUrgency(), e.getTrend(),
            e.getStateId(), e.getCreatedAt(), e.getCreatedById(), e.getUpdatedAt(),
            e.getUpdatedById(), e.getOwnedById());
    }

    public void apply(WhbClassification c, WhbClassificationEntity e) {
        e.setId(c.getId());
        e.setTenantId(c.getTenantId());
        e.setProcessdefId(c.getProcessdefId());
        e.setProcessversionId(c.getProcessversionId());
        e.setFlowelementId(c.getFlowelementId());
        e.setGravity(c.getGravity());
        e.setUrgency(c.getUrgency());
        e.setTrend(c.getTrend());
        e.setStateId(c.getStateId());
        e.setCreatedAt(c.getCreatedAt());
        e.setCreatedById(c.getCreatedById());
        e.setUpdatedAt(c.getUpdatedAt());
        e.setUpdatedById(c.getUpdatedById());
        e.setOwnedById(c.getOwnedById());
    }
}
