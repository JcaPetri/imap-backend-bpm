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

package com.imap.bpm;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * IMAP BPM Microservice — entry point.
 *
 * Responsabilidades:
 *   - Orquestador central de procesos para todos los módulos IMAP
 *   - Estado de ejecución: process_instance, token, task_instance, variable,
 *     audit_log, job_executor, message_correlation, compensation
 *   - Definitions de procesos viven en system (microservice_code='system');
 *     bpm las consume via API + cache local (próximo iter)
 *
 * Bare minimum scaffold (Paso B):
 *   - Spring Boot Web + JPA + Actuator + Prometheus + JWT
 *   - Endpoint GET /v1/ping + /actuator/health
 *   - NO incluye motor V→R ni endpoints de ejecución todavía
 *
 * Puerto: 8093 | Context path: /imap/bpm | Schema postgres: bpm
 */
@SpringBootApplication(scanBasePackages = {
    "com.imap.platform",
    "com.imap.bpm",
    "com.imap.eav.engine"   // lib eav-engine: EavTenantSession (RLS bridge)
})
@EnableScheduling   // habilita JobExecutorWorker (A2 — timers via @Scheduled)
public class BpmApplication {

    public static void main(String[] args) {
        SpringApplication.run(BpmApplication.class, args);
    }
}
