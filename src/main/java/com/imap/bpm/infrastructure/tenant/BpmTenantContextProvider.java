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

package com.imap.bpm.infrastructure.tenant;
import com.imap.platform.tenant.TenantContextHolder;

import com.imap.eav.engine.context.TenantContextProvider;
import org.springframework.stereotype.Component;

import java.util.UUID;

/**
 * Implementación bpm de TenantContextProvider (interface de la lib eav-engine).
 *
 * Lee el ThreadLocal `TenantContextHolder` que el `TenantContextFilter` setea
 * por cada request. Si no hay tenant activo (anonymous), retorna SYSTEM_TENANT_ID.
 *
 * Esto es lo que permite que ValidatorEngine + EavTenantSession + cualquier
 * código de la lib funcione sin conocer nada del filter HTTP del bpm.
 */
@Component
public class BpmTenantContextProvider implements TenantContextProvider {

    @Override
    public UUID getCurrentTenantId() {
        return TenantContextHolder.get();
    }
}
