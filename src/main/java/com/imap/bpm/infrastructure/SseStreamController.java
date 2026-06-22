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
//  • [controller] DTOs, nunca exponer entidades del domain en la API
//  • [bpm] Orquestador central (tipo Temporal/Step Functions)
//  • [bpm] processdef→processversion: snapshot inmutable al activar
//  • [bpm] Las defs de proceso viven acá (no en system)
//  • [bpm] Camunda 8 como norte; interim form-driven en prod
// ─── GOLDEN-RULES:END ───

package com.imap.bpm.infrastructure;

import com.imap.bpm.infrastructure.sse.SseEventBus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.Map;

/**
 * Endpoint SSE del microservicio BPM.
 *
 * Eventos emitidos por el motor:
 *   - bpm.token.entered      — { instanceId, flowElementCode, ts }
 *   - bpm.task.created       — { instanceId, taskId, flowElementCode, assignedUserId, ts }
 *   - bpm.task.completed     — { instanceId, taskId, flowElementCode, ts }
 *   - bpm.instance.completed — { instanceId, ts }
 *   - bpm.instance.cancelled — { instanceId, reason, ts }
 *
 * Frontend en ProcessDiagram filtra por instanceId y actualiza markers live.
 *
 * Auth: público en esta iter (broadcast generic, no filtra por tenant aún).
 * TODO V2: filtrar broadcast por tenant — actualmente todos los clientes
 * conectados ven todos los eventos. Si dos tenants distintos están viendo
 * processdefs simultáneamente, ven los eventos del otro (no es bug crítico
 * porque solo expone metadata mínima — instanceId, flowElementCode — no
 * data sensible — pero merece fix antes de SaaS multi-tenant real).
 */
@RestController
@RequestMapping("/v1/events")
public class SseStreamController {

    private final SseEventBus bus;

    public SseStreamController(SseEventBus bus) {
        this.bus = bus;
    }

    @GetMapping(value = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter stream() {
        return bus.subscribe();
    }

    @GetMapping("/stream/stats")
    public Map<String, Object> stats() {
        return Map.of(
            "service", "bpm",
            "activeConnections", bus.activeConnections(),
            "timestamp", System.currentTimeMillis()
        );
    }
}
