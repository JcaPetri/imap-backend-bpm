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

import com.imap.bpm.domain.model.Migrationplan;
import com.imap.bpm.infrastructure.entity.MigrationplanEntity;
import org.springframework.stereotype.Component;

/** Traduce entre MigrationplanEntity (JPA) y el modelo de dominio Migrationplan. */
@Component
public class MigrationplanMapper {

    public Migrationplan toDomain(MigrationplanEntity e) {
        return new Migrationplan(e.getId(), e.getTenantId(), e.getCode(), e.getDescription(),
            e.getSourceProcessversionId(), e.getTargetProcessversionId(), e.getStatus(),
            e.getAppliedAt(), e.getAppliedBy(), e.getStats(), e.getStateId(),
            e.getCreatedAt(), e.getUpdatedAt(), e.getCreatedById(), e.getUpdatedById(), e.getOwnedById());
    }

    /** Copia los campos del modelo sobre la entity (nueva o gestionada). Audit updatable=false los ignora Hibernate en UPDATE. */
    public void apply(Migrationplan p, MigrationplanEntity e) {
        e.setId(p.getId());
        e.setTenantId(p.getTenantId());
        e.setCode(p.getCode());
        e.setDescription(p.getDescription());
        e.setSourceProcessversionId(p.getSourceProcessversionId());
        e.setTargetProcessversionId(p.getTargetProcessversionId());
        e.setStatus(p.getStatus());
        e.setAppliedAt(p.getAppliedAt());
        e.setAppliedBy(p.getAppliedBy());
        e.setStats(p.getStats());
        e.setStateId(p.getStateId());
        e.setCreatedAt(p.getCreatedAt());
        e.setUpdatedAt(p.getUpdatedAt());
        e.setCreatedById(p.getCreatedById());
        e.setUpdatedById(p.getUpdatedById());
        e.setOwnedById(p.getOwnedById());
    }
}
