package com.imap.bpm.domain.engine.servicetask;

import java.util.Collections;
import java.util.Map;

/**
 * Resultado de ejecutar un service_task.
 *
 * Estados:
 *   - SUCCESS: el handler completó OK. Si trae {@code resultVariables}, se mergean a la
 *     processinstance.variables (útil para que el siguiente step las lea).
 *   - FAILURE: el handler falló. El motor reintenta según la retry policy. Si después
 *     de todos los intentos sigue FAILURE, el token va a lifecycle='failed' y se
 *     audita. Si el handler quiere triggear un boundary error event específico, debe
 *     setear {@code boundaryErrorCode} y el motor lo dispara en vez de marcar failed.
 *   - PENDING (V2, NO IMPLEMENTADO MVP): el handler arrancó algo asíncrono. El motor
 *     pone el token en lifecycle='waiting' y espera un evento de complete.
 *
 * Inmutable.
 */
public record ServiceTaskResult(
    Status status,
    Map<String, Object> resultVariables,
    String errorCode,
    String errorMessage,
    String boundaryErrorCode
) {
    public enum Status { SUCCESS, FAILURE, PENDING }

    // ─── Factory methods (sintáctica más limpia que el constructor) ────────────

    public static ServiceTaskResult ok() {
        return new ServiceTaskResult(Status.SUCCESS, Collections.emptyMap(), null, null, null);
    }

    public static ServiceTaskResult ok(Map<String, Object> vars) {
        return new ServiceTaskResult(Status.SUCCESS, vars == null ? Collections.emptyMap() : vars, null, null, null);
    }

    public static ServiceTaskResult fail(String errorCode, String errorMessage) {
        return new ServiceTaskResult(Status.FAILURE, Collections.emptyMap(), errorCode, errorMessage, null);
    }

    /** Failure que NO reintenta — dispara un boundary error event en su lugar. */
    public static ServiceTaskResult boundaryError(String boundaryErrorCode, String errorMessage) {
        return new ServiceTaskResult(Status.FAILURE, Collections.emptyMap(), boundaryErrorCode, errorMessage, boundaryErrorCode);
    }

    public static ServiceTaskResult pending() {
        return new ServiceTaskResult(Status.PENDING, Collections.emptyMap(), null, null, null);
    }

    public boolean isSuccess()  { return status == Status.SUCCESS; }
    public boolean isFailure()  { return status == Status.FAILURE; }
    public boolean isPending()  { return status == Status.PENDING; }
}
