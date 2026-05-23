package com.imap.bpm.domain.engine.servicetask.handlers;

import com.imap.bpm.domain.engine.servicetask.ServiceTask;
import com.imap.bpm.domain.engine.servicetask.ServiceTaskContext;
import com.imap.bpm.domain.engine.servicetask.ServiceTaskHandler;
import com.imap.bpm.domain.engine.servicetask.ServiceTaskResult;

/**
 * Test handler: falla con boundaryErrorCode='TEST_BOUNDARY_ERROR' (o el código
 * configurado en config.errorCode). El motor NO reintenta — dispara el boundary
 * error event adjunto al service_task.
 *
 * Útil para smoke E2E del flujo: service_task fails → boundary error event
 * triggers → flujo se va por la rama de error.
 *
 * Config:
 *   { "serviceCode": "bpm.test.error_boundary", "config": { "errorCode": "INSUFFICIENT_STOCK" } }
 */
@ServiceTask("bpm.test.error_boundary")
public class ErrorBoundaryHandler implements ServiceTaskHandler {

    @Override
    public ServiceTaskResult execute(ServiceTaskContext ctx) {
        String code = (String) ctx.config("errorCode");
        if (code == null || code.isBlank()) code = "TEST_BOUNDARY_ERROR";
        return ServiceTaskResult.boundaryError(code,
            "ErrorBoundaryHandler raised boundary error (test handler — bpm.test.error_boundary)");
    }
}
