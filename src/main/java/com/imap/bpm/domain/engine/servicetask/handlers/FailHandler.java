package com.imap.bpm.domain.engine.servicetask.handlers;

import com.imap.bpm.domain.engine.servicetask.ServiceTask;
import com.imap.bpm.domain.engine.servicetask.ServiceTaskContext;
import com.imap.bpm.domain.engine.servicetask.ServiceTaskHandler;
import com.imap.bpm.domain.engine.servicetask.ServiceTaskResult;

/**
 * Test handler: always fails with errorCode='TEST_FAILURE'.
 *
 * Útil para smoke E2E del retry policy: verifica que el motor reintenta
 * N veces (default 3) antes de marcar el token como failed.
 *
 * Config opcional:
 *   { "serviceCode": "bpm.test.fail", "config": { "errorCode": "CUSTOM_FAIL" } }
 */
@ServiceTask("bpm.test.fail")
public class FailHandler implements ServiceTaskHandler {

    @Override
    public ServiceTaskResult execute(ServiceTaskContext ctx) {
        String code = (String) ctx.config("errorCode");
        if (code == null || code.isBlank()) code = "TEST_FAILURE";
        return ServiceTaskResult.fail(code, "FailHandler always fails (test handler — bpm.test.fail)");
    }
}
