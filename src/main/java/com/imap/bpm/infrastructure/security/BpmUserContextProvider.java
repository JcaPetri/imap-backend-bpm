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
