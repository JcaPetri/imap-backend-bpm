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

package com.imap.bpm.infrastructure.repository;

import com.imap.bpm.infrastructure.entity.JobExecutorEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

public interface JobExecutorRepository extends JpaRepository<JobExecutorEntity, UUID> {

    /**
     * Jobs vencidos esperando firing. Worker scheduled lo polea cada N seg.
     * Limita resultado a evitar lock-storms si hubiera cola gigante (en MVP
     * el volumen es bajo, igual lockeamos por seguridad).
     *
     * NOTE: usa LIMIT 50 — overflow se procesa en la próxima iter del worker.
     */
    @Query("""
        SELECT j FROM JobExecutorEntity j
         WHERE j.lifecycle = 'scheduled'
           AND j.fireAt <= :now
         ORDER BY j.fireAt
        """)
    List<JobExecutorEntity> findDueJobs(@Param("now") OffsetDateTime now,
                                  org.springframework.data.domain.Pageable pageable);

    List<JobExecutorEntity> findByProcessinstanceIdAndLifecycle(UUID processinstanceId, String lifecycle);
    List<JobExecutorEntity> findByTokenIdAndLifecycle(UUID tokenId, String lifecycle);

    /**
     * Cascade DELETE de instance (admin cleanup).
     *
     * Bulk DELETE via JPQL @Modifying (no entity lifecycle), evita
     * StaleObjectStateException en race con worker tick: el derived
     * deleteByXxx hace findAll + em.remove → optimistic lock check por
     * row. Esta versión hace un solo DELETE SQL atómico.
     */
    @Modifying
    @Transactional
    @Query("DELETE FROM JobExecutorEntity j WHERE j.processinstanceId = :pid")
    int deleteByProcessinstanceId(@Param("pid") UUID processinstanceId);
}
