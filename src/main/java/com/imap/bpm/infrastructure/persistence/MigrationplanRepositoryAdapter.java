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
import com.imap.bpm.domain.port.out.MigrationplanRepository;
import com.imap.bpm.infrastructure.entity.MigrationplanEntity;
import com.imap.bpm.infrastructure.repository.MigrationplanJpaRepository;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

/** Adapter de salida de Migrationplan. Envuelve el Spring Data repo con load-and-mutate. */
@Component
public class MigrationplanRepositoryAdapter implements MigrationplanRepository {

    private final MigrationplanJpaRepository jpa;
    private final MigrationplanMapper mapper;

    public MigrationplanRepositoryAdapter(MigrationplanJpaRepository jpa, MigrationplanMapper mapper) {
        this.jpa = jpa;
        this.mapper = mapper;
    }

    @Override
    public List<Migrationplan> findByTenantIdOrderByCreatedAtDesc(UUID tenantId) {
        return jpa.findByTenantIdOrderByCreatedAtDesc(tenantId).stream().map(mapper::toDomain).toList();
    }

    @Override
    public Optional<Migrationplan> findByTenantIdAndCode(UUID tenantId, String code) {
        return jpa.findByTenantIdAndCode(tenantId, code).map(mapper::toDomain);
    }

    @Override
    public Optional<Migrationplan> findById(UUID id) {
        return jpa.findById(id).map(mapper::toDomain);
    }

    @Override
    public Migrationplan save(Migrationplan p) {
        MigrationplanEntity e = p.getId() != null
            ? jpa.findById(p.getId()).orElseGet(MigrationplanEntity::new)
            : new MigrationplanEntity();
        mapper.apply(p, e);
        return mapper.toDomain(jpa.save(e));
    }
}
