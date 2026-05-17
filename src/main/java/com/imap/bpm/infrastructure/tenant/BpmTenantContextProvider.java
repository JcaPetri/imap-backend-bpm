package com.imap.bpm.infrastructure.tenant;

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
