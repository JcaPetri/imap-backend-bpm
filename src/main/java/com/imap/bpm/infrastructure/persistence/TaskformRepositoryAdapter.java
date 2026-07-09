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

import com.imap.bpm.domain.model.Taskform;
import com.imap.bpm.domain.port.out.TaskformRepository;
import com.imap.bpm.infrastructure.entity.TaskformEntity;
import com.imap.bpm.infrastructure.repository.TaskformJpaRepository;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

/** Adapter de salida de Taskform. Envuelve el Spring Data repo con load-and-mutate. */
@Component
public class TaskformRepositoryAdapter implements TaskformRepository {

    private final TaskformJpaRepository jpa;
    private final TaskformMapper mapper;

    public TaskformRepositoryAdapter(TaskformJpaRepository jpa, TaskformMapper mapper) {
        this.jpa = jpa;
        this.mapper = mapper;
    }

    @Override
    public List<Taskform> findByFlowelementIdIn(List<UUID> flowelementIds) {
        return jpa.findByFlowelementIdIn(flowelementIds).stream().map(mapper::toDomain).toList();
    }

    @Override
    public int deleteByFlowelementIdIn(List<UUID> flowelementIds) {
        return jpa.deleteByFlowelementIdIn(flowelementIds);
    }

    @Override
    public Optional<Taskform> findById(UUID id) {
        return jpa.findById(id).map(mapper::toDomain);
    }

    @Override
    public Taskform save(Taskform m) {
        TaskformEntity e = m.getId() != null
            ? jpa.findById(m.getId()).orElseGet(TaskformEntity::new)
            : new TaskformEntity();
        mapper.apply(m, e);
        return mapper.toDomain(jpa.save(e));
    }
}
