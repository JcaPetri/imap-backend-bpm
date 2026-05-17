package com.imap.bpm.infrastructure.tenant;

import java.util.UUID;

/**
 * ThreadLocal del tenant_id activo en la request actual.
 *
 * Lo setea TenantContextFilter al entrar la request (lee del header
 * X-Tenant-Id, default = SYSTEM_TENANT) y lo limpia al salir.
 *
 * Lo lee TenantContextService para aplicar SET LOCAL app.current_tenant_id
 * antes de queries/INSERTs (RLS multi-tenant).
 *
 * Iter 5: el TenantContextFilter va a leer del JWT claim en lugar del header
 * (X-Tenant-Id queda solo como override de development).
 */
public final class TenantContextHolder {

    public static final UUID SYSTEM_TENANT_ID =
        UUID.fromString("00000000-0000-0000-0000-000000000001");

    private static final ThreadLocal<UUID> HOLDER = new ThreadLocal<>();

    private TenantContextHolder() {}

    public static void set(UUID tenantId) {
        HOLDER.set(tenantId);
    }

    public static UUID get() {
        UUID v = HOLDER.get();
        return v == null ? SYSTEM_TENANT_ID : v;
    }

    public static void clear() {
        HOLDER.remove();
    }
}
