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
import com.imap.bpm.domain.port.out.MigrationruleRepository;
import com.imap.bpm.infrastructure.entity.MigrationruleEntity;
import com.imap.bpm.infrastructure.repository.MigrationruleJpaRepository;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.UUID;

/** Adapter de salida de Migrationrule. Envuelve el Spring Data repo con load-and-mutate. */
@Component
public class MigrationruleRepositoryAdapter implements MigrationruleRepository {

    private final MigrationruleJpaRepository jpa;
    private final MigrationruleMapper mapper;

    public MigrationruleRepositoryAdapter(MigrationruleJpaRepository jpa, MigrationruleMapper mapper) {
        this.jpa = jpa;
        this.mapper = mapper;
    }

    @Override
    public List<Migrationrule> findByMigrationplanIdOrderBySortOrder(UUID migrationplanId) {
        return jpa.findByMigrationplanIdOrderBySortOrder(migrationplanId).stream().map(mapper::toDomain).toList();
    }

    @Override
    public int deleteByMigrationplanId(UUID migrationplanId) {
        return jpa.deleteByMigrationplanId(migrationplanId);
    }

    @Override
    public Migrationrule save(Migrationrule r) {
        MigrationruleEntity e = r.getId() != null
            ? jpa.findById(r.getId()).orElseGet(MigrationruleEntity::new)
            : new MigrationruleEntity();
        mapper.apply(r, e);
        return mapper.toDomain(jpa.save(e));
    }
}
