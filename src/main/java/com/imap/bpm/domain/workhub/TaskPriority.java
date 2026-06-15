package com.imap.bpm.domain.workhub;

/**
 * Resultado del cálculo de prioridad de una tarea para la bandeja del WorkHub.
 *
 * @param prioridadPct  score normalizado a % del máximo (0–100). 0 si la tarea
 *                      no tiene clasificación.
 * @param color         banda del semáforo (combina prioridad% con estado de SLA).
 * @param classified    true si el processdef/user_task tiene clasificación G/U/T
 *                      cargada; false si el % proviene solo del default (0).
 */
public record TaskPriority(double prioridadPct, SemaphoreColor color, boolean classified) {}
