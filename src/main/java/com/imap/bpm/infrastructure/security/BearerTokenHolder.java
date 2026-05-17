package com.imap.bpm.infrastructure.security;

/**
 * ThreadLocal del Authorization Bearer token de la request HTTP actual.
 *
 * Lo setea JwtAuthFilter al entrar la request, lo limpia al salir.
 * Lo lee el motor BPM (ProcessDefinitionLoader / DecisionDefinitionLoader)
 * para propagar la auth al system microservice cuando hace cache MISS.
 *
 * NOTA: para threads que NO vienen de un request HTTP (ej JobExecutorWorker
 * @Scheduled), el holder está vacío. En esos casos los loaders deberían
 * usar definitions ya cacheadas. Si requieren un cache miss desde un
 * scheduled thread, hay que pensar service-to-service auth (futuro).
 */
public final class BearerTokenHolder {

    private static final ThreadLocal<String> HOLDER = new ThreadLocal<>();

    private BearerTokenHolder() {}

    public static void set(String bearer) { HOLDER.set(bearer); }
    public static String get() { return HOLDER.get(); }
    public static void clear() { HOLDER.remove(); }
}
