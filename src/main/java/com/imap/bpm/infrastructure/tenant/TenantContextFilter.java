package com.imap.bpm.infrastructure.tenant;

import com.imap.bpm.infrastructure.security.UserContext;
import com.imap.bpm.infrastructure.security.UserContextHolder;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.UUID;

/**
 * Filter HTTP que resuelve el tenant_id activo y lo publica en
 * TenantContextHolder para SET LOCAL app.current_tenant_id (RLS).
 *
 * Resolución:
 *   1. Header X-Tenant-Id presente y válido → se usa ese valor.
 *      Si hay UserContext (JWT autenticado) → valida que el user sea miembro
 *      del tenant solicitado (403 si no). SIN excepción para SYSTEM tenant:
 *      operar contra SYSTEM requiere membership explícita (service token o
 *      super-admin), evitando que users de otros tenants escriban/lean en él.
 *   2. Header ausente y user autenticado con UN solo tenant → se usa ese
 *      (auto-pick de single-tenant users).
 *   3. Header ausente y user con múltiples tenants → 400 (debe especificar).
 *   4. Header ausente y user autenticado con CERO tenants → 403 (no es member
 *      de nada, no puede operar contra ningún tenant). Cierra agujero: antes
 *      caía al else legacy y recibía SYSTEM como default.
 *   5. Header ausente y SIN user (anonymous) → default SYSTEM_TENANT_ID
 *      (comportamiento legacy, requerido por smoke tests + system-internal).
 *
 * Order: HIGHEST_PRECEDENCE+20 — corre DESPUÉS de JwtAuthFilter (+10) para
 * que UserContextHolder ya esté seteado.
 *
 * El holder se LIMPIA en el finally para evitar leak entre requests
 * (Tomcat reusa threads).
 *
 * Historial:
 *  - 2026-05-19: fix tenant isolation bypass — usuarios sin membership en
 *    SYSTEM podían operar contra SYSTEM enviando X-Tenant-Id: SYSTEM
 *    (línea 71 tenía bypass !SYSTEM_TENANT_ID.equals(tenantId)) Y usuarios
 *    con permissionsByTenant vacío caían al else legacy y recibían SYSTEM
 *    como default.
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE + 20)
public class TenantContextFilter extends OncePerRequestFilter {

    private static final Logger log = LoggerFactory.getLogger(TenantContextFilter.class);
    public static final String HEADER = "X-Tenant-Id";

    @Override
    protected boolean shouldNotFilter(HttpServletRequest req) {
        String path = req.getRequestURI();
        if (path == null) return false;
        if (path.startsWith("/actuator")) return true;
        return "OPTIONS".equalsIgnoreCase(req.getMethod());
    }

    @Override
    protected void doFilterInternal(HttpServletRequest req,
                                    HttpServletResponse res,
                                    FilterChain chain) throws ServletException, IOException {
        String raw = req.getHeader(HEADER);
        UserContext userCtx = UserContextHolder.get();

        UUID tenantId;
        if (raw != null && !raw.isBlank()) {
            try {
                tenantId = UUID.fromString(raw.trim());
            } catch (IllegalArgumentException e) {
                res.sendError(HttpServletResponse.SC_BAD_REQUEST,
                    "Invalid X-Tenant-Id header (expected UUID): " + raw);
                return;
            }
            // Validación estricta — SIN excepción para SYSTEM tenant. Si el caller
            // necesita operar contra SYSTEM (super-admin, service token) tiene que
            // tener membership explícita en él. Cierra agujero descubierto 2026-05-19.
            if (userCtx != null && !userCtx.isMemberOf(tenantId)) {
                log.warn("User {} not member of tenant {} — request denied", userCtx.userId(), tenantId);
                res.sendError(HttpServletResponse.SC_FORBIDDEN,
                    "User is not a member of tenant " + tenantId);
                return;
            }
        } else if (userCtx != null
                && userCtx.permissionsByTenant() != null
                && userCtx.permissionsByTenant().size() == 1) {
            String onlyTenant = userCtx.permissionsByTenant().keySet().iterator().next();
            try {
                tenantId = UUID.fromString(onlyTenant);
            } catch (IllegalArgumentException e) {
                log.error("JWT permissionsByTenant key is not a UUID: {}", onlyTenant);
                res.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
                    "Malformed JWT — tenant id is not UUID");
                return;
            }
        } else if (userCtx != null
                && userCtx.permissionsByTenant() != null
                && userCtx.permissionsByTenant().size() > 1) {
            res.sendError(HttpServletResponse.SC_BAD_REQUEST,
                "User belongs to multiple tenants — header X-Tenant-Id required");
            return;
        } else if (userCtx != null) {
            // User autenticado pero permissionsByTenant null/vacío → es huérfano,
            // no es member de NINGÚN tenant. Rechazo explícito 403, NO fallback
            // legacy a SYSTEM (que sería un escalamiento de privilegio).
            log.warn("Authenticated user {} has no tenant memberships — request denied", userCtx.userId());
            res.sendError(HttpServletResponse.SC_FORBIDDEN,
                "User has no tenant memberships");
            return;
        } else {
            // Anonymous (sin JWT) → mantiene default SYSTEM (smoke tests + s2s sin token).
            tenantId = TenantContextHolder.SYSTEM_TENANT_ID;
        }

        TenantContextHolder.set(tenantId);
        if (log.isDebugEnabled()) {
            log.debug("Request {} {} tenant={} user={}",
                req.getMethod(), req.getRequestURI(), tenantId,
                userCtx == null ? "anonymous" : userCtx.userId());
        }
        try {
            chain.doFilter(req, res);
        } finally {
            TenantContextHolder.clear();
        }
    }
}
