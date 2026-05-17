package com.imap.bpm.infrastructure.security;

/**
 * ThreadLocal del UserContext de la request actual.
 *
 * Lo setea JwtAuthFilter cuando el header Authorization: Bearer <token> es
 * válido. Lo limpia el mismo filter en finally para evitar leak entre requests
 * (Tomcat reusa threads).
 *
 * Si la request NO trae JWT válido → get() devuelve null (anonymous request).
 * Las llamadas internas (system→system) o health checks pueden ser anonymous.
 *
 * Lo lee:
 *   - DataUserResolver (=current_user) — para resolver creator/updater
 *   - VariableResolver (=current_tenant) si lo extendemos en Iter 5.3
 *   - Loggers / auditing
 *
 * NO confundir con TenantContextHolder (tenant_id activo). El tenant viene del
 * header X-Tenant-Id (o JWT claim futuro), el user viene SIEMPRE del JWT.
 */
public final class UserContextHolder {

    private static final ThreadLocal<UserContext> HOLDER = new ThreadLocal<>();

    private UserContextHolder() {}

    public static void set(UserContext ctx) {
        HOLDER.set(ctx);
    }

    /** Returns the current user context, or null if request is anonymous. */
    public static UserContext get() {
        return HOLDER.get();
    }

    public static boolean isAuthenticated() {
        return HOLDER.get() != null;
    }

    public static void clear() {
        HOLDER.remove();
    }
}
