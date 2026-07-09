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

import com.imap.bpm.infrastructure.entity.TokenEntity;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface TokenRepository extends JpaRepository<TokenEntity, UUID> {
    List<TokenEntity> findByProcessinstanceIdAndLifecycleIn(UUID processinstanceId, List<String> lifecycles);

    /**
     * A1 — Usada por parallel_gateway JOIN para sincronizar siblings:
     * cuenta cuántos tokens hermanos (misma parentTokenId) ya llegaron al
     * mismo gateway con lifecycle=waiting.
     */
    List<TokenEntity> findByProcessinstanceIdAndCurrentElementIdAndLifecycle(
        UUID processinstanceId, UUID currentElementId, String lifecycle);

    /**
     * Multi-instance JOIN por cardinalidad: cuenta los tokens hijos (mismo
     * parentTokenId = token ancla) que ya terminaron el cuerpo de la activity
     * (currentElementId = la activity multi-instanciada) con un lifecycle dado.
     */
    int countByParentTokenIdAndCurrentElementIdAndLifecycle(
        UUID parentTokenId, UUID currentElementId, String lifecycle);

    /**
     * Multi-instance anti-race: lock pesimista del token ancla. Serializa las
     * completaciones concurrentes de los hijos para que exactamente una cruce
     * el umbral N y avance (mismo espíritu que el claim atómico de jobs).
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT t FROM TokenEntity t WHERE t.id = :id")
    Optional<TokenEntity> findByIdForUpdate(@Param("id") UUID id);

    /** Para cascade DELETE de instance (admin cleanup). */
    long deleteByProcessinstanceId(UUID processinstanceId);
}
