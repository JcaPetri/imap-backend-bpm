package com.imap.bpm.domain.engine.servicetask.handlers;

import com.imap.bpm.domain.engine.servicetask.ServiceTask;
import com.imap.bpm.domain.engine.servicetask.ServiceTaskContext;
import com.imap.bpm.domain.engine.servicetask.ServiceTaskHandler;
import com.imap.bpm.domain.engine.servicetask.ServiceTaskResult;

import java.util.Map;

/**
 * Test handler: echoes the input variable 'message' as result 'echo'.
 *
 * Uso en processdef:
 *   service_task config: { "serviceCode": "bpm.test.echo" }
 *   Input variable: { "message": "hello world" }
 *   Resultado en processinstance.variables: { "echo": "hello world" }
 *
 * Útil para smoke E2E del ServiceTaskRegistry sin dependencias externas.
 */
@ServiceTask("bpm.test.echo")
public class EchoHandler implements ServiceTaskHandler {

    @Override
    public ServiceTaskResult execute(ServiceTaskContext ctx) {
        Object message = ctx.variable("message");
        Object inputFromConfig = ctx.config("input");   // alternativa: input fijo en el processdef
        Object echoValue = message != null ? message : inputFromConfig;
        return ServiceTaskResult.ok(Map.of(
            "echo", echoValue != null ? echoValue : "(no input)",
            "echoedBy", "bpm.test.echo",
            "echoedAt", java.time.Instant.now().toString()
        ));
    }
}
