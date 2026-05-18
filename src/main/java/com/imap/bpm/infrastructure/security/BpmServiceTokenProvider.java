package com.imap.bpm.infrastructure.security;

import com.imap.bpm.infrastructure.tenant.TenantContextHolder;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.crypto.SecretKey;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Emite un JWT firmado con el shared secret (`jwt.access.secret`) para
 * autenticar llamadas service-to-service desde el bpm al system, en
 * threads que NO vienen de un request HTTP (ej JobExecutorWorker).
 *
 * El JWT tiene:
 *   - sub: BPM_SERVICE_USER_ID (UUID fijo identificable)
 *   - email: bpm@imap.internal
 *   - permissionsByTenant: { SYSTEM_TENANT: [system.admin] }
 *   - exp: now + 1h
 *
 * Cachea el token en memoria; regenera cuando faltan menos de 5 min para
 * expiración (para no servir tokens al borde y dejar margen para latencia
 * de red).
 *
 * NOTA: este patrón requiere que iam/system reconozcan al BPM_SERVICE_USER_ID
 * como usuario válido. Como no creamos el row en iam_user_tbl, el system
 * solo valida la firma JWT + permissionsByTenant (que es lo único que
 * usa para autorizar). El JWT NO necesita pasar por `/v1/auth/login`.
 */
@Component
public class BpmServiceTokenProvider {

    private static final Logger log = LoggerFactory.getLogger(BpmServiceTokenProvider.class);
    private static final Duration TOKEN_TTL = Duration.ofHours(1);
    private static final Duration RENEW_BEFORE_EXPIRY = Duration.ofMinutes(5);

    /** UUID determinístico del "service user" del bpm. Sintético, no en BD. */
    private static final UUID BPM_SERVICE_USER_ID =
        UUID.nameUUIDFromBytes("bpm-service-account".getBytes(StandardCharsets.UTF_8));

    @Value("${jwt.access.secret:}")
    private String accessSecret;

    private SecretKey key;
    private volatile String cachedToken;
    private volatile Instant cachedExpiry;

    @PostConstruct
    void init() {
        if (accessSecret == null || accessSecret.isBlank()) {
            log.warn("BpmServiceTokenProvider DISABLED — jwt.access.secret not set. " +
                "JobExecutorWorker s2s calls al system fallarán con 401 si requieren cache miss.");
            return;
        }
        this.key = Keys.hmacShaKeyFor(accessSecret.getBytes(StandardCharsets.UTF_8));
        log.info("BpmServiceTokenProvider enabled — service user id={}", BPM_SERVICE_USER_ID);
    }

    /**
     * Devuelve un token vigente. Genera nuevo si el cacheado expira en < 5 min.
     * Thread-safe (volatile + double-checked sin lock — colisión solo emite
     * un par de tokens extra, sin efecto).
     */
    public String currentToken() {
        if (key == null) return null;
        Instant now = Instant.now();
        if (cachedToken != null && cachedExpiry != null
                && cachedExpiry.isAfter(now.plus(RENEW_BEFORE_EXPIRY))) {
            return cachedToken;
        }
        return regenerate(now);
    }

    private synchronized String regenerate(Instant now) {
        // double-check inside lock
        if (cachedToken != null && cachedExpiry != null
                && cachedExpiry.isAfter(now.plus(RENEW_BEFORE_EXPIRY))) {
            return cachedToken;
        }
        Instant expiry = now.plus(TOKEN_TTL);
        Map<String, List<String>> permissions = Map.of(
            TenantContextHolder.SYSTEM_TENANT_ID.toString(), List.of("system.admin")
        );
        String token = Jwts.builder()
            .subject(BPM_SERVICE_USER_ID.toString())
            .claim("email", "bpm@imap.internal")
            .claim("impersonation", false)
            .claim("permissionsByTenant", permissions)
            .issuedAt(java.util.Date.from(now))
            .expiration(java.util.Date.from(expiry))
            .signWith(key)
            .compact();
        this.cachedToken = token;
        this.cachedExpiry = expiry;
        log.debug("Regenerated bpm service token (exp={})", expiry);
        return token;
    }
}
