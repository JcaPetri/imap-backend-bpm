package com.imap.bpm.domain.workhub;

/**
 * Banda de color del semáforo del WorkHub. Orden ascendente de severidad
 * (GREEN &lt; YELLOW &lt; RED) para poder combinar dos señales con max().
 */
public enum SemaphoreColor {
    GREEN, YELLOW, RED;

    /** Devuelve la más severa de las dos. */
    public SemaphoreColor max(SemaphoreColor other) {
        return this.ordinal() >= other.ordinal() ? this : other;
    }
}
