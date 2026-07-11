# imap-backend-bpm

Microservicio IMAP — **Orquestador de Procesos (BPM)**. Motor BPMN central que coordina los flujos de trabajo de todos los módulos de la plataforma.

- **Puerto**: 8093 · **Context path**: `/imap/bpm` · **Schema**: `bpm` · **Prefijo**: `bpm_`
- **Package**: `com.imap.bpm` · Stack estándar IMAP (Spring Boot 3.2.3, Java 17, hexagonal estricto, PostgreSQL, Flyway).

## Estado

**En producción — hexagonal estricto ✅ (grade A).** Motor BPMN completo con instancias, tokens, tareas, variables y auditoría.
124 clases Java, 18 migraciones Flyway (V001–V018), RLS activo en todas las tablas. Suite de smokes E2E contra prod
(`smoke_*.ps1`, uno por familia de features). Definiciones de proceso ya en tablas relacionales propias de `bpm`
(migradas desde el EAV de `system`).

**Vocabulario BPMN ampliado (2026-07-09):** se cerró el roadmap de brecha vs Camunda 8 — 9 features nuevas en prod
(multi-instance, boundary-error-auto sobre service_task, event-based gateway, compensation/saga, inclusive gateway OR,
terminate end, parallel M×N, timer cíclico). El motor cubre hoy el subset BPMN que tiene valor de negocio real.
Detalle + ejemplos de aplicación: `IMAP_MOTOR_BPM.md`.

## Para qué sirve

Es el cerebro que orquesta el trabajo de la empresa. Modela cada proceso de negocio (una compra, una venta, una
liquidación de impuestos, un onboarding) como un flujo BPMN y lo ejecuta paso a paso: crea tareas para las personas,
dispara acciones automáticas en otros módulos, evalúa reglas de decisión y deja trazabilidad completa de cada paso.
Un mismo motor gobierna todos los módulos, garantizando que los procesos se ejecuten siempre igual, auditados y
por tenant.

## Servicios

| Servicio | Qué hace | Quién lo usa |
|---|---|---|
| ProcessEngine | Motor central que ejecuta la máquina de estados BPMN avanzando los tokens y persistiendo el estado. Entiende el vocabulario completo (ver «Capacidades» abajo): eventos de inicio/fin, tareas de usuario y de servicio, los 4 gateways (exclusivo/paralelo/inclusivo/event-based), eventos intermedios y de borde (timer/message/signal, cíclicos), subprocesos, reglas DMN, multi-instance, compensación/saga y terminate. | API de procesos + internamente el worker de timers y el bus de notificaciones SSE. |
| ProcessdefManagementService | Alta y edición de definiciones de proceso en tablas relacionales: crea la definición, su versión, elementos de flujo y secuencias en una transacción atómica con validación de topología. | Endpoint de administración de definiciones. |
| ServiceTaskRegistry | Despachador de acciones automáticas: descubre handlers locales y rutea los remotos vía HTTP seguro a otros microservicios, con reintentos y timeout configurable. | El motor, al ejecutar una tarea de servicio. |
| MigrationPlanManagementService | Gestión de planes de migración de una versión de proceso a otra: crea, edita y valida las reglas de transformación. | Endpoint de administración de migraciones. |
| MigrationApplyService | Aplica un plan de migración sobre instancias en curso: reubica los tokens a la nueva versión, aplica las reglas, audita los cambios y emite notificaciones. | Endpoint de aplicación de migraciones. |
| JobExecutorWorker | Worker programado que revisa periódicamente los trabajos pendientes (timers) y dispara los eventos temporizados cuando llega su hora. | Planificador de tareas en segundo plano (sin HTTP directo). |
| TaskAssignmentService | Resuelve a quién se asigna cada tarea de usuario al crearse (persona real o, si el iniciador es una cuenta de servicio, fallback a administrador). | El motor, al crear una tarea de usuario. |
| ScoreService + WorkHubConfigService | Cálculo de prioridad y semáforo de color de las tareas en la bandeja de trabajo (WorkHub) según reglas. | API de procesos + administración de WorkHub. |
| DmnEvaluator | Evaluador de reglas de decisión (DMN): aplica operadores declarativos y política de resolución sobre las tablas de decisión. | El motor, al ejecutar una tarea de regla de negocio. |
| SystemEntityResolver | Resuelve códigos de entidad de `system` vía HTTP seguro y cachea la relación código→UUID. | Validación de definiciones de proceso que referencian entidades. |

## Capacidades del motor (vocabulario BPMN soportado)

El `ProcessEngine` despacha por tipo de `flow_element` + marcadores en su `config`. Lo que el motor sabe ejecutar hoy:

| Capacidad | Tipo / marcador | Qué hace |
|---|---|---|
| Inicio / Fin | `start_event` · `end_event` | Arranca / cierra la instancia. |
| Tarea humana | `user_task` | Crea una tarea en la bandeja (WorkHub) con su form dinámico; espera la completación. |
| Tarea de servicio | `service_task` | Invoca un handler (local o remoto por HTTP S2S) con retry + timeout. |
| Gateway exclusivo (XOR) | `exclusive_gateway` | Toma **una** rama según condición JEXL (+ default). |
| Gateway paralelo (AND) | `parallel_gateway` | Split/join de todas las ramas; soporta **M-in/N-out** (join-then-split). |
| Gateway inclusivo (OR) | `inclusive_gateway` | Activa **todas** las ramas cuya condición es true; el join espera solo a las activadas. |
| Gateway por eventos | `event_based_gateway` | Carrera: arma N eventos, gana el primero, cancela el resto. |
| Evento intermedio | `intermediate_event` (timer/message/signal) | Espera un tiempo, un mensaje correlacionado o una señal broadcast. |
| Evento de borde | `boundary_event` (timer/error/escalation) | Interrumpe o acompaña una activity: timeout, error, escalación. **Cíclico** (recordatorios) con `repeatEverySeconds`+`maxRepeats`. |
| Boundary error auto | `service_task` + boundary error | Una falla de service_task (timeout/5xx) enruta a la rama de excepción (catch-all `*`). |
| Subproceso | `sub_process` (call activity) | Ejecuta otro processdef como hijo (wait o fire-and-forget). |
| Regla de negocio | `business_rule_task` | Evalúa una tabla de decisión **DMN** (7 hit policies). |
| Multi-instance | marcador `config.multiInstance` en `user_task`/`sub_process` | Ejecuta la activity N veces (una por ítem de una colección); join por cardinalidad. |
| Compensación / Saga | handler `config.compensationFor` + `end_event config.compensate=true` | Deshace en LIFO lo ya completado (rollback de negocio sin 2PC). |
| Terminate | `end_event config.terminate=true` | Aborta la instancia matando todas las ramas vivas. |
| Message-start (chaining) | `BpmMessageEmitter` (s2s) | Un handler emite un mensaje que arranca otro processdef → orquestación event-driven. |

Cross-cutting: cache de definiciones (Caffeine), timers multi-réplica (`FOR UPDATE SKIP LOCKED`), auth S2S
(`BpmServiceTokenProvider`), notificaciones live por SSE, migración de instancias entre versiones, y WorkHub
(priorización + semáforo de tareas).

**Expresiones**: condiciones de gateway y evaluación DMN usan **Apache Commons JEXL3** (`${var == 'x'}`), no FEEL.
FEEL (el lenguaje de expresiones estándar de Camunda/DMN) está **diferido por decisión explícita** (2026-07-11):
solo se implementaría si aparece la necesidad de **importar BPMN/DMN estándar de Camunda Modeler**; JEXL3 cubre
toda la autoría propia. Ver `IMAP_MOTOR_BPM_PLAN.md §7.2`.

## Integraciones

Orquesta y habla **por HTTP seguro (S2S con JWT)** con el resto de la plataforma: rutea tareas de servicio a
cualquier microservicio (purchase, sale, inventory, tax-ar, accounting, treasury, etc.) vía
`{baseUrl}/v1/service-tasks/execute`, resuelve entidades y carga definiciones desde `system`, y asigna tareas a
usuarios de `iam`. Cero acceso SQL cross-schema — todo cross-service es HTTP. Es él mismo el **orquestador** que
gobierna los procesos de los demás módulos.

## Conformidad hexagonal

Hexagonal estricto **grade A**: domain puro (POJOs sin `@Entity` ni imports de infraestructura), `@Entity` solo en
`infrastructure/entity` (21), ports devuelven modelos de dominio, controllers siempre vía application services.
Audit-7 en las 21 entidades (con `updatable=false` en columnas núcleo) y RLS por tenant en todas las tablas.
**Exenciones documentadas**: el motor BPM (`ProcessEngine`) accede a repositorios de entidad directamente por ser la
máquina de estados central (§F.6); el cleanup bulk de tablas de infraestructura (jobs) usa DELETE físico; y
`bpm_inbox_event_tbl` va sin RLS por deduplicación fuera de la transacción. **Gaps**: solo menores (BAJA — falta
Bean Validation declarativa en DTOs, sin i18n en errores de negocio, sin suite JUnit) y uno MEDIA de
mantenibilidad (`ProcessEngine` monolítico, candidato a patrón Strategy por tipo de elemento). Sin gaps ALTA.
