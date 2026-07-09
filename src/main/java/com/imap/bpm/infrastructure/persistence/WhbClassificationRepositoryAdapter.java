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
import com.imap.bpm.domain.port.out.WhbClassificationRepository;
import com.imap.bpm.infrastructure.entity.WhbClassificationEntity;
import com.imap.bpm.infrastructure.repository.WhbClassificationJpaRepository;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

/** Adapter de salida de WhbClassification. Envuelve el Spring Data repo con load-and-mutate. */
@Component
public class WhbClassificationRepositoryAdapter implements WhbClassificationRepository {

    private final WhbClassificationJpaRepository jpa;
    private final WhbClassificationMapper mapper;

    public WhbClassificationRepositoryAdapter(WhbClassificationJpaRepository jpa, WhbClassificationMapper mapper) {
        this.jpa = jpa;
        this.mapper = mapper;
    }

    @Override
    public Optional<WhbClassification> findCurrentProcessLevel(UUID tenantId, UUID processdefId) {
        return jpa.findByTenantIdAndProcessdefIdAndFlowelementIdIsNullAndProcessversionIdIsNull(tenantId, processdefId)
            .map(mapper::toDomain);
    }

    @Override
    public Optional<WhbClassification> findCurrentForFlowelement(UUID tenantId, UUID processdefId, UUID flowelementId) {
        return jpa.findByTenantIdAndProcessdefIdAndFlowelementIdAndProcessversionIdIsNull(tenantId, processdefId, flowelementId)
            .map(mapper::toDomain);
    }

    @Override
    public List<WhbClassification> findCurrentByTenant(UUID tenantId) {
        return jpa.findByTenantIdAndProcessversionIdIsNull(tenantId).stream().map(mapper::toDomain).toList();
    }

    @Override
    public WhbClassification save(WhbClassification c) {
        WhbClassificationEntity e = c.getId() != null
            ? jpa.findById(c.getId()).orElseGet(WhbClassificationEntity::new)
            : new WhbClassificationEntity();
        mapper.apply(c, e);
        return mapper.toDomain(jpa.save(e));
    }
}
