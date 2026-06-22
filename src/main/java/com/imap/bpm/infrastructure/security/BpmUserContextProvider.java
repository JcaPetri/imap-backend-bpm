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

package com.imap.bpm.infrastructure.security;
import com.imap.platform.security.UserContext;
import com.imap.platform.security.UserContextHolder;

import com.imap.eav.engine.context.UserContextProvider;
import org.springframework.stereotype.Component;

import java.util.UUID;

/**
 * Implementación bpm de UserContextProvider (interface de la lib eav-engine).
 *
 * Lee el ThreadLocal `UserContextHolder` que el `JwtAuthFilter` popula con
 * los claims del JWT. Devuelve null si la request es anonymous.
 */
@Component
public class BpmUserContextProvider implements UserContextProvider {

    @Override
    public UUID getCurrentUserId() {
        UserContext ctx = UserContextHolder.get();
        return ctx != null ? ctx.userId() : null;
    }

    @Override
    public String getCurrentUserEmail() {
        UserContext ctx = UserContextHolder.get();
        return ctx != null ? ctx.email() : null;
    }
}
