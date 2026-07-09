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

import com.imap.bpm.domain.model.Migrationrule;
import com.imap.bpm.infrastructure.entity.MigrationruleEntity;
import org.springframework.stereotype.Component;

/** Traduce entre MigrationruleEntity (JPA) y el modelo de dominio Migrationrule. */
@Component
public class MigrationruleMapper {

    public Migrationrule toDomain(MigrationruleEntity e) {
        return new Migrationrule(e.getId(), e.getTenantId(), e.getMigrationplanId(),
            e.getSourceFlowelementCode(), e.getTargetFlowelementCode(), e.getAction(),
            e.getSortOrder(), e.getNotes(), e.getStateId(),
            e.getCreatedAt(), e.getUpdatedAt(), e.getCreatedById(), e.getUpdatedById(), e.getOwnedById());
    }

    /** Copia los campos del modelo sobre la entity (nueva o gestionada). Audit updatable=false los ignora Hibernate en UPDATE. */
    public void apply(Migrationrule r, MigrationruleEntity e) {
        e.setId(r.getId());
        e.setTenantId(r.getTenantId());
        e.setMigrationplanId(r.getMigrationplanId());
        e.setSourceFlowelementCode(r.getSourceFlowelementCode());
        e.setTargetFlowelementCode(r.getTargetFlowelementCode());
        e.setAction(r.getAction());
        e.setSortOrder(r.getSortOrder());
        e.setNotes(r.getNotes());
        e.setStateId(r.getStateId());
        e.setCreatedAt(r.getCreatedAt());
        e.setUpdatedAt(r.getUpdatedAt());
        e.setCreatedById(r.getCreatedById());
        e.setUpdatedById(r.getUpdatedById());
        e.setOwnedById(r.getOwnedById());
    }
}
