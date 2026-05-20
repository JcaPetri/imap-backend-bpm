package com.imap.bpm.infrastructure.sse;

import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Bus de eventos in-memory + registry de SseEmitters activos del microservicio BPM.
 *
 * Copia 1:1 del SseEventBus del IAM (commit 2ef146f). Cada microservicio
 * tiene su propio bus + endpoint → zero cross-service coupling. El frontend
 * abre N conexiones (1 por microservicio que emita eventos).
 *
 * Use cases registrados en BPM al 2026-05-20:
 *   - bpm.token.entered      — ProcessEngine.advanceToken al entrar a un flowElement
 *   - bpm.task.created       — al crear una user_task
 *   - bpm.task.completed     — al completar una task
 *   - bpm.instance.completed — al cerrar una instance (end_event alcanzado)
 *   - bpm.instance.cancelled — al cancelar (soft) una instance
 *
 * Frontend reacciona en ProcessDiagram filtrando por instanceId — actualiza
 * highlighting live sin refresh.
 *
 * Patrones idénticos al IAM: heartbeat 25s, cleanup automático, multi-replica
 * caveat (Redis Pub/Sub para escalar).
 */
@Component
public class SseEventBus {

    private static final Logger log = LoggerFactory.getLogger(SseEventBus.class);
    private static final long EMITTER_TIMEOUT_MS = 60 * 60 * 1000L;   // 1h
    private static final long HEARTBEAT_INTERVAL_SECONDS = 25L;

    private final Map<String, SseEmitter> emitters = new ConcurrentHashMap<>();
    private final ScheduledExecutorService heartbeatExecutor = Executors.newSingleThreadScheduledExecutor(
        r -> {
            Thread t = new Thread(r, "bpm-sse-heartbeat");
            t.setDaemon(true);
            return t;
        });

    public SseEventBus() {
        heartbeatExecutor.scheduleAtFixedRate(this::sendHeartbeats,
            HEARTBEAT_INTERVAL_SECONDS, HEARTBEAT_INTERVAL_SECONDS, TimeUnit.SECONDS);
        log.info("BPM SseEventBus initialized — heartbeat every {}s, emitter timeout {}ms",
            HEARTBEAT_INTERVAL_SECONDS, EMITTER_TIMEOUT_MS);
    }

    public SseEmitter subscribe() {
        String id = UUID.randomUUID().toString();
        SseEmitter emitter = new SseEmitter(EMITTER_TIMEOUT_MS);
        emitter.onCompletion(() -> {
            emitters.remove(id);
            log.debug("BPM SSE emitter {} completed (total active: {})", id, emitters.size());
        });
        emitter.onTimeout(() -> {
            emitters.remove(id);
            log.debug("BPM SSE emitter {} timed out (total active: {})", id, emitters.size());
        });
        emitter.onError(e -> {
            emitters.remove(id);
            log.debug("BPM SSE emitter {} errored: {} (total active: {})", id, e.getMessage(), emitters.size());
        });
        emitters.put(id, emitter);

        try {
            emitter.send(SseEmitter.event()
                .name("connected")
                .data(Map.of("emitterId", id, "service", "bpm", "ts", System.currentTimeMillis())));
        } catch (IOException e) {
            log.warn("Failed to send initial 'connected' to BPM SSE emitter {}", id);
        }
        log.info("BPM SSE client subscribed — emitterId={} (total active: {})", id, emitters.size());
        return emitter;
    }

    public void broadcast(String eventName, Object data) {
        if (emitters.isEmpty()) {
            log.debug("BPM broadcast '{}' skipped — no active emitters", eventName);
            return;
        }
        int sent = 0;
        int failed = 0;
        for (Map.Entry<String, SseEmitter> entry : emitters.entrySet()) {
            try {
                entry.getValue().send(SseEmitter.event().name(eventName).data(data));
                sent++;
            } catch (IOException e) {
                emitters.remove(entry.getKey());
                failed++;
            }
        }
        log.debug("BPM broadcast '{}' — sent={} failed={} (total active after: {})",
            eventName, sent, failed, emitters.size());
    }

    private void sendHeartbeats() {
        if (emitters.isEmpty()) return;
        long ts = System.currentTimeMillis();
        for (Map.Entry<String, SseEmitter> entry : emitters.entrySet()) {
            try {
                entry.getValue().send(SseEmitter.event().name("heartbeat").data(ts));
            } catch (IOException e) {
                emitters.remove(entry.getKey());
            }
        }
    }

    public int activeConnections() {
        return emitters.size();
    }

    @PreDestroy
    public void shutdown() {
        log.info("BPM SseEventBus shutting down — closing {} active emitters", emitters.size());
        for (SseEmitter emitter : emitters.values()) {
            try { emitter.complete(); } catch (Exception ignored) {}
        }
        emitters.clear();
        heartbeatExecutor.shutdown();
    }
}
