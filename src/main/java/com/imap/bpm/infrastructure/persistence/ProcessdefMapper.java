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

import com.imap.bpm.domain.model.Processdef;
import com.imap.bpm.infrastructure.entity.ProcessdefEntity;
import org.springframework.stereotype.Component;

/** Traduce entre ProcessdefEntity (JPA) y el modelo de dominio Processdef. */
@Component
public class ProcessdefMapper {

    public Processdef toDomain(ProcessdefEntity e) {
        return new Processdef(e.getId(), e.getTenantId(), e.getCode(), e.getName(), e.getDescription(),
            e.getCurrentversionId(), e.getLifecycle(), e.getStartPermission(), e.getStateId(),
            e.getCreatedAt(), e.getUpdatedAt(), e.getCreatedById(), e.getUpdatedById(), e.getOwnedById());
    }

    /** Copia los campos del modelo sobre la entity (nueva o gestionada). Audit updatable=false los ignora Hibernate en UPDATE. */
    public void apply(Processdef m, ProcessdefEntity e) {
        e.setId(m.getId());
        e.setTenantId(m.getTenantId());
        e.setCode(m.getCode());
        e.setName(m.getName());
        e.setDescription(m.getDescription());
        e.setCurrentversionId(m.getCurrentversionId());
        e.setLifecycle(m.getLifecycle());
        e.setStartPermission(m.getStartPermission());
        e.setStateId(m.getStateId());
        e.setCreatedAt(m.getCreatedAt());
        e.setUpdatedAt(m.getUpdatedAt());
        e.setCreatedById(m.getCreatedById());
        e.setUpdatedById(m.getUpdatedById());
        e.setOwnedById(m.getOwnedById());
    }
}
