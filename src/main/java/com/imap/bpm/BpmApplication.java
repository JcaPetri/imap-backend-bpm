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
