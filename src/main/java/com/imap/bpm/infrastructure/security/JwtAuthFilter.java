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
import com.imap.platform.security.JwtAccessTokenValidator;
import com.imap.platform.security.BearerTokenHolder;
import com.imap.platform.security.UserContextHolder;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.JwtException;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Filter HTTP que extrae el access token JWT del header Authorization, lo
 * valida con JwtAccessTokenValidator y publica el UserContext en
 * UserContextHolder.
 *
 * Comportamiento (Iter 5.2 — JWT opcional, no obligatorio):
 *   1. Sin header Authorization        → request anonymous, UserContext=null,
 *                                        sigue al chain (compatibilidad con
 *                                        smoke tests + curl manual)
 *   2. Header presente + token válido  → UserContext seteado, sigue al chain
 *   3. Header presente + token inválido → 401 Unauthorized, NO sigue al chain
 *   4. JWT_ACCESS_SECRET no configurado → siempre comportamiento (1) — el
 *                                         validator está disabled, log warning
 *
 * Order: HIGHEST_PRECEDENCE+10. Corre ANTES que TenantContextFilter y
 * CorrelationIdFilter para que ellos puedan consultar UserContext si lo
 * necesitan (en Iter 5.3 el TenantContextFilter va a validar membership).
 *
 * Endpoints que NO requieren auth (whitelist):
 *   - /actuator/**            (health, metrics)
 *   - OPTIONS preflight CORS
 *
 * En Iter 6 cuando agreguemos protección obligatoria, este filter va a
 * rechazar requests sin Authorization para los endpoints protegidos.
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE + 10)
public class JwtAuthFilter extends OncePerRequestFilter {

    private static final Logger log = LoggerFactory.getLogger(JwtAuthFilter.class);

    private static final String HEADER          = "Authorization";
    private static final String BEARER_PREFIX   = "Bearer ";

    private final JwtAccessTokenValidator validator;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public JwtAuthFilter(JwtAccessTokenValidator validator) {
        this.validator = validator;
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest req) {
        String path = req.getRequestURI();
        if (path == null) return false;
        // Whitelist: actuator + CORS preflight
        if (path.startsWith("/actuator")) return true;
        return "OPTIONS".equalsIgnoreCase(req.getMethod());
    }

    @Override
    protected void doFilterInternal(HttpServletRequest req,
                                    HttpServletResponse res,
                                    FilterChain chain) throws ServletException, IOException {
        String authHeader = req.getHeader(HEADER);

        // Caso (1): sin header → anonymous, sigue
        if (authHeader == null || !authHeader.startsWith(BEARER_PREFIX)) {
            chain.doFilter(req, res);
            return;
        }

        // Caso (4): validator disabled → ignorar el token (degradar a anonymous)
        if (!validator.isEnabled()) {
            log.warn("Authorization header present but JWT validator disabled — request {}: {} → anonymous",
                     req.getMethod(), req.getRequestURI());
            chain.doFilter(req, res);
            return;
        }

        String token = authHeader.substring(BEARER_PREFIX.length()).trim();

        // Caso (2)/(3): validar
        Claims claims;
        try {
            claims = validator.validate(token);
        } catch (JwtException e) {
            log.warn("JWT validation failed for {} {}: {}",
                     req.getMethod(), req.getRequestURI(), e.getMessage());
            sendUnauthorized(res, "invalid_token", e.getMessage());
            return;
        }

        UserContext ctx;
        try {
            ctx = buildContext(claims);
        } catch (IllegalArgumentException e) {
            log.warn("JWT structure invalid for {} {}: {}",
                     req.getMethod(), req.getRequestURI(), e.getMessage());
            sendUnauthorized(res, "invalid_token_structure", e.getMessage());
            return;
        }

        UserContextHolder.set(ctx);
        // B3 — guardar el bearer original para propagar a llamadas s2s al system
        // (ProcessDefinitionLoader / DecisionDefinitionLoader leen del holder
        // cuando hacen cache MISS sin tener el token explícito en la cadena).
        BearerTokenHolder.set(token);
        if (log.isDebugEnabled()) {
            log.debug("Authenticated user={} ({}), tenants={}, impersonation={}",
                      ctx.userId(), ctx.email(),
                      ctx.permissionsByTenant() == null ? 0 : ctx.permissionsByTenant().size(),
                      ctx.impersonation());
        }
        try {
            chain.doFilter(req, res);
        } finally {
            UserContextHolder.clear();
            BearerTokenHolder.clear();
        }
    }

    @SuppressWarnings("unchecked")
    private UserContext buildContext(Claims claims) {
        String sub = claims.getSubject();
        if (sub == null || sub.isBlank()) {
            throw new IllegalArgumentException("missing 'sub' claim");
        }
        UUID userId;
        try {
            userId = UUID.fromString(sub);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("'sub' is not a UUID: " + sub);
        }
        String email = claims.get("email", String.class);
        boolean impersonation = Boolean.TRUE.equals(claims.get("impersonation", Boolean.class));

        Map<String, List<String>> permsByTenant = Collections.emptyMap();
        Object raw = claims.get("permissionsByTenant");
        if (raw instanceof Map<?, ?> rawMap) {
            permsByTenant = new LinkedHashMap<>();
            for (Map.Entry<?, ?> e : rawMap.entrySet()) {
                if (!(e.getKey() instanceof String tenantId)) continue;
                Object v = e.getValue();
                List<String> perms = new ArrayList<>();
                if (v instanceof List<?> rawList) {
                    for (Object item : rawList) {
                        if (item instanceof String s) perms.add(s);
                    }
                }
                permsByTenant.put(tenantId, perms);
            }
        }
        return new UserContext(userId, email, permsByTenant, impersonation);
    }

    private void sendUnauthorized(HttpServletResponse res, String error, String message) throws IOException {
        res.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
        res.setContentType("application/json;charset=UTF-8");
        objectMapper.writeValue(res.getWriter(), Map.of(
            "error", error,
            "message", message
        ));
    }
}
