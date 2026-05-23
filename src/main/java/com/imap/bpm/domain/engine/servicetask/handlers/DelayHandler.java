package com.imap.bpm.domain.engine.servicetask.handlers;

import com.imap.bpm.domain.engine.servicetask.ServiceTask;
import com.imap.bpm.domain.engine.servicetask.ServiceTaskContext;
import com.imap.bpm.domain.engine.servicetask.ServiceTaskHandler;
import com.imap.bpm.domain.engine.servicetask.ServiceTaskResult;

import java.util.Map;

/**
 * Test handler: sleeps the configured delayMs (default 2000) and returns ok.
 *
 * Útil para smoke E2E de latencia y de timeout enforcement (poner delayMs > timeout-seconds).
 *
 * Config:
 *   { "serviceCode": "bpm.test.delay", "config": { "delayMs": 2000 } }
 */
@ServiceTask("bpm.test.delay")
public class DelayHandler implements ServiceTaskHandler {

    @Override
    public ServiceTaskResult execute(ServiceTaskContext ctx) {
        Object delayCfg = ctx.config("delayMs");
        long delayMs = 2000;
        if (delayCfg != null) {
            try { delayMs = Long.parseLong(delayCfg.toString()); }
            catch (NumberFormatException ignored) {}
        }
        long start = System.currentTimeMillis();
        try {
            Thread.sleep(delayMs);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            return ServiceTaskResult.fail("INTERRUPTED", "Sleep interrupted");
        }
        long elapsed = System.currentTimeMillis() - start;
        return ServiceTaskResult.ok(Map.of(
            "delayMs", delayMs,
            "actualElapsedMs", elapsed
        ));
    }
}
