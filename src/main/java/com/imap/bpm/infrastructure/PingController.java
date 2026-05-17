package com.imap.bpm.infrastructure;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * PingController — endpoint trivial para validar que el scaffold arrancó OK.
 *
 *   GET /imap/bpm/v1/ping → { service, version, status }
 *
 * Este controller es el "hello world" del microservicio. Se elimina cuando
 * lleguen los endpoints reales (POST /v1/process/{code}/start, etc.).
 */
@RestController
@RequestMapping("/v1")
public class PingController {

    @GetMapping("/ping")
    public Map<String, Object> ping() {
        return Map.of(
            "service", "bpm",
            "version", "0.0.1-SNAPSHOT",
            "status",  "OK"
        );
    }
}
