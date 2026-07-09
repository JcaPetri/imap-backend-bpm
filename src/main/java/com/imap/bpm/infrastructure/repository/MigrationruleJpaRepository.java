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

import com.imap.bpm.infrastructure.entity.MigrationruleEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface MigrationruleJpaRepository extends JpaRepository<MigrationruleEntity, UUID> {
    List<MigrationruleEntity> findByMigrationplanIdOrderBySortOrder(UUID migrationplanId);

    /** F4-mgmt updateRules: borra las rules del plan antes de recrearlas.
     *  Bulk @Modifying (ejecución inmediata, devuelve int — NO long, Spring Data lo rechaza)
     *  — un deleteBy derivado es find-then-remove diferido y Hibernate ordena INSERTS antes
     *  que DELETES en el flush → colisión de unique key al recrear. */
    @org.springframework.data.jpa.repository.Modifying
    @org.springframework.data.jpa.repository.Query("delete from MigrationruleEntity r where r.migrationplanId = ?1")
    int deleteByMigrationplanId(UUID migrationplanId);
}
