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

import com.imap.bpm.domain.model.Sequenceflow;
import com.imap.bpm.infrastructure.entity.SequenceflowEntity;
import org.springframework.stereotype.Component;

/** Traduce entre SequenceflowEntity (JPA) y el modelo de dominio Sequenceflow. */
@Component
public class SequenceflowMapper {

    public Sequenceflow toDomain(SequenceflowEntity e) {
        return new Sequenceflow(e.getId(), e.getTenantId(), e.getProcessversionId(), e.getSourceId(),
            e.getTargetId(), e.getConditionExpr(), e.getSortOrder(), e.getDescription(), e.getStateId(),
            e.getCreatedAt(), e.getUpdatedAt(), e.getCreatedById(), e.getUpdatedById(), e.getOwnedById());
    }

    /** Copia los campos del modelo sobre la entity (nueva o gestionada). Audit updatable=false los ignora Hibernate en UPDATE. */
    public void apply(Sequenceflow m, SequenceflowEntity e) {
        e.setId(m.getId());
        e.setTenantId(m.getTenantId());
        e.setProcessversionId(m.getProcessversionId());
        e.setSourceId(m.getSourceId());
        e.setTargetId(m.getTargetId());
        e.setConditionExpr(m.getConditionExpr());
        e.setSortOrder(m.getSortOrder());
        e.setDescription(m.getDescription());
        e.setStateId(m.getStateId());
        e.setCreatedAt(m.getCreatedAt());
        e.setUpdatedAt(m.getUpdatedAt());
        e.setCreatedById(m.getCreatedById());
        e.setUpdatedById(m.getUpdatedById());
        e.setOwnedById(m.getOwnedById());
    }
}
