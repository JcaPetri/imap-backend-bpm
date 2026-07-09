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

import com.imap.bpm.domain.model.Processversion;
import com.imap.bpm.domain.port.out.ProcessversionRepository;
import com.imap.bpm.infrastructure.entity.ProcessversionEntity;
import com.imap.bpm.infrastructure.repository.ProcessversionJpaRepository;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

/** Adapter de salida de Processversion. Envuelve el Spring Data repo con load-and-mutate. */
@Component
public class ProcessversionRepositoryAdapter implements ProcessversionRepository {

    private final ProcessversionJpaRepository jpa;
    private final ProcessversionMapper mapper;

    public ProcessversionRepositoryAdapter(ProcessversionJpaRepository jpa, ProcessversionMapper mapper) {
        this.jpa = jpa;
        this.mapper = mapper;
    }

    @Override
    public List<Processversion> findByProcessdefIdOrderByVersionDesc(UUID processdefId) {
        return jpa.findByProcessdefIdOrderByVersionDesc(processdefId).stream().map(mapper::toDomain).toList();
    }

    @Override
    public Optional<Processversion> findByTenantIdAndProcessdefIdAndVersion(UUID tenantId, UUID processdefId, Integer version) {
        return jpa.findByTenantIdAndProcessdefIdAndVersion(tenantId, processdefId, version).map(mapper::toDomain);
    }

    @Override
    public Optional<Processversion> findById(UUID id) {
        return jpa.findById(id).map(mapper::toDomain);
    }

    @Override
    public Processversion save(Processversion m) {
        ProcessversionEntity e = m.getId() != null
            ? jpa.findById(m.getId()).orElseGet(ProcessversionEntity::new)
            : new ProcessversionEntity();
        mapper.apply(m, e);
        return mapper.toDomain(jpa.save(e));
    }
}
