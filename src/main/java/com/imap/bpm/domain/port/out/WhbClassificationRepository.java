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

package com.imap.bpm.domain.port.out;

import com.imap.bpm.domain.model.WhbClassification;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

/** Puerto de salida: clasificaciones G/U/T del WorkHub. Devuelve dominio. */
public interface WhbClassificationRepository {

    /** Clasificación "current" a nivel proceso (flowelement NULL, version NULL). */
    Optional<WhbClassification> findCurrentProcessLevel(UUID tenantId, UUID processdefId);

    /** Clasificación "current" de un user_task específico (flowelement, version NULL). */
    Optional<WhbClassification> findCurrentForFlowelement(UUID tenantId, UUID processdefId, UUID flowelementId);

    /** Todas las "current" del tenant (para la pantalla admin). */
    List<WhbClassification> findCurrentByTenant(UUID tenantId);

    WhbClassification save(WhbClassification classification);
}
