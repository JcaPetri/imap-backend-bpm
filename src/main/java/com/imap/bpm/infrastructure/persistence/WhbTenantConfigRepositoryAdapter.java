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
import com.imap.bpm.domain.port.out.WhbTenantConfigRepository;
import com.imap.bpm.infrastructure.entity.WhbTenantConfigEntity;
import com.imap.bpm.infrastructure.repository.WhbTenantConfigJpaRepository;
import org.springframework.stereotype.Component;

import java.util.Optional;
import java.util.UUID;

/** Adapter de salida de WhbTenantConfig. Envuelve el Spring Data repo con load-and-mutate. */
@Component
public class WhbTenantConfigRepositoryAdapter implements WhbTenantConfigRepository {

    private final WhbTenantConfigJpaRepository jpa;
    private final WhbTenantConfigMapper mapper;

    public WhbTenantConfigRepositoryAdapter(WhbTenantConfigJpaRepository jpa, WhbTenantConfigMapper mapper) {
        this.jpa = jpa;
        this.mapper = mapper;
    }

    @Override
    public Optional<WhbTenantConfig> findByTenantId(UUID tenantId) {
        return jpa.findByTenantId(tenantId).map(mapper::toDomain);
    }

    @Override
    public WhbTenantConfig save(WhbTenantConfig c) {
        WhbTenantConfigEntity e = c.getId() != null
            ? jpa.findById(c.getId()).orElseGet(WhbTenantConfigEntity::new)
            : new WhbTenantConfigEntity();
        mapper.apply(c, e);
        return mapper.toDomain(jpa.save(e));
    }
}
