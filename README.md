# imap-backend-bpm

Microservicio IMAP — **Orquestador de Procesos (BPM)**. Motor BPMN central que coordina los flujos de trabajo de todos los módulos de la plataforma.

- **Puerto**: 8093 · **Context path**: `/imap/bpm` · **Schema**: `bpm` · **Prefijo**: `bpm_`
- **Package**: `com.imap.bpm` · Stack estándar IMAP (Spring Boot 3.2.3, Java 17, hexagonal estricto, PostgreSQL, Flyway).

## Estado

**En producción — hexagonal estricto ✅ (grade A).** Motor BPMN completo con instancias, tokens, tareas, variables y auditoría.
133 clases Java, 23 migraciones Flyway (V001–V023), 24 `@Entity`, RLS activo en todas las tablas. Suite de 21 smokes
E2E contra prod (`smoke_*.ps1`, uno por familia de features). Definiciones de proceso ya en tablas relacionales
propias de `bpm` (migradas desde el EAV de `system`).

**Roadmap de brecha vs Camunda 8 — CERRADO (Olas 1 a 7.1, 2026-07-11):** además de las primeras 9 features
(multi-instance, boundary-error-auto sobre service_task, event-based gateway, compensation/saga, inclusive gateway
OR, terminate end, parallel M×N, timer cíclico, message-start) se sumaron y están en prod: **event_sub_process**
con sus 4 triggers (signal/error/message/timer, interrumpiendo o no), **eventos throw** (signal/message/compensate)
además de los catch, **propagación de error/escalation** de un sub-process hijo hacia el boundary del padre,
**DMN con DRD chaining** (`required_decisions`, resolución en orden topológico), **exactly-once** real en las
tareas de servicio remotas (Idempotency-Key determinística + inbox de deduplicación en cada receptor), **async
continuation** de service_task remoto (job + kick inmediato), **inclusive gateway** con join por reachability
estructural (no solo cardinalidad), **sincronización de subscripción message-start** al activar el processdef, y el
subsistema de **incidentes con retry/resolve** manual. El motor cubre hoy el subset BPMN completo con valor de
negocio real. Detalle + ejemplos de aplicación: `IMAP_MOTOR_BPM.md`.

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
| DmnEvaluator | Evaluador de reglas de decisión (DMN): aplica operadores declarativos y política de resolución sobre las tablas de decisión, incluyendo **DRD chaining** (resuelve `required_decisions` en orden topológico antes de evaluar la decisión dependiente). | El motor, al ejecutar una tarea de regla de negocio. |
| DecisiondefManagementService | Alta, edición y borrado de tablas de decisión DMN (con `required_decisions` para DRD chaining). | Endpoint de administración de decisiones (`POST/DELETE /v1/bpm/admin/decisiondef`). |
| SystemEntityResolver | Resuelve códigos de entidad de `system` vía HTTP seguro y cachea la relación código→UUID. | Validación de definiciones de proceso que referencian entidades. |
| EventSubscriptionRepository | Persiste y consulta las suscripciones activas de `event_sub_process` (trigger signal/error/message/timer + `correlationKey`) en `bpm_pro_event_subscription_tbl`. | El motor, al despachar un signal/message/error/timer que puede activar un event sub-process en alguna instancia viva. |
| IncidentRepository (panel de incidentes) | Persiste `bpm_pro_incident_tbl`: cuando una tarea de servicio falla en forma terminal (sin boundary error que la capture), el motor abre un incidente en vez de perder el token; expone consulta y las acciones de retry/resolve manual. | `GET /v1/bpm/incidents`, `POST /incident/{id}/retry`, `POST /incident/{id}/resolve` + panel de incidentes en `ProcessAdminPage` (Ola 4.2 UI). |
| ServiceTaskInboxService | Deduplicación **exactly-once del lado receptor**: ante un `POST /v1/service-tasks/execute` intenta `INSERT ... ON CONFLICT DO NOTHING` sobre `svctask_inbox` usando la Idempotency-Key entrante; si ya existía, devuelve el resultado cacheado sin re-ejecutar el efecto de negocio. **No vive en `bpm`**: es un helper clonable que se copia tal cual en cada microservicio receptor de tareas de servicio (purchase, sale, inventory, tax-ar, accounting, treasury, etc.). | El endpoint `/v1/service-tasks/execute` de cada micro receptor, antes de ejecutar el handler de negocio. |

## Capacidades del motor (vocabulario BPMN soportado)

El `ProcessEngine` despacha por tipo de `flow_element` + marcadores en su `config`. Lo que el motor sabe ejecutar hoy:

| # | Capacidad | Tipo / marcador | Config (shape exacto) | Qué hace |
|---|---|---|---|---|
| 1 | Inicio | `start_event` | `{}` normal · `{ message:{ messageCode:"X" } }` message-start | Arranca la instancia. El message-start suscribe el processdef al `messageCode` en forma **sincrónica al activar** la versión (no al primer arranque). |
| 2 | Fin | `end_event` | `{}` normal · `{terminate:true}` · `{compensate:true}` · `{error:{errorCode:"X"}}` · `{escalation:{escalationCode:"X"}}` | Cierra la instancia o la rama; según el marcador dispara terminate, compensación, o propaga error/escalation hacia arriba. |
| 3 | Tarea de servicio | `service_task` | `{ serviceCode:"inventory.stock.reserve" }` (+ `errorCode`, `async`, `compensationFor`, `multiInstance`) | Invoca un handler local o remoto por HTTP S2S con retry + timeout. |
| 4 | Async continuation | `service_task` remoto en modo async | marcador `config.async` (opt-in) o default remoto | El motor no bloquea el token esperando la respuesta HTTP: encola un job y lo continúa con un "kick" inmediato al completarse. |
| 5 | Exactly-once | cualquier `service_task` remoto | — (automático) | bpm genera una Idempotency-Key determinística (`nameUUID(token:element:encarnación)`); el receptor deduplica vía `svctask_inbox` (claim `INSERT ON CONFLICT` + cache-and-return). Ver «Servicios» → `ServiceTaskInboxService`. |
| 6 | Tarea humana | `user_task` | `{ candidateGroup:"deposito_ba" }` · `{ candidateGroupDecision:"wh_route_by_branch" }` (resuelto por DMN) | Crea una tarea en la bandeja (WorkHub) con form EAV dinámico; claim CAS (409 si ya la tomó otro) y espera la completación. |
| 7 | Gateway exclusivo (XOR) | `exclusive_gateway` | `conditionExpr:"${monto < 500000}"` por rama + rama default | Toma **una** rama según la primera condición JEXL verdadera (o el default). |
| 8 | Gateway paralelo (AND) | `parallel_gateway` | topología del grafo (sin config propio) | Split/join de todas las ramas; soporta **M-in/N-out** (join-then-split), topology-driven. |
| 9 | Gateway inclusivo (OR) | `inclusive_gateway` | `conditionExpr` por rama + rama default | Activa **todas** las ramas cuya condición es true; el join espera solo a las activadas, resuelto por **reachability estructural** (no solo cardinalidad). |
| 10 | Gateway por eventos | `event_based_gateway` | ramas con evento intermedio catch | Carrera: arma N eventos, gana el primero, cancela el resto. |
| 11 | Evento intermedio CATCH | `intermediate_event` | timer `{ timer:{ delaySeconds:5 } }` · message `{ message:{ messageCode:"PAY_OK", correlationKey:"${orderId}" } }` · signal `{ signal:{ signalCode:"S" } }` | Espera un tiempo, un mensaje correlacionado o una señal broadcast. |
| 12 | Evento intermedio THROW | `intermediate_event` | `{ throw:"signal", signalCode:"S" }` · `{ throw:"message", messageCode:"X", correlationKey:"K" }` · `{ throw:"compensate" }` | Emite (en vez de esperar) una señal, un mensaje correlacionado, o dispara compensación. |
| 13 | Evento de borde | `boundary_event` | timer `{ boundary:{ attachedTo:"task", interrupting:true }, timer:{ delaySeconds:172800 } }` · cíclico `{ boundary:{..., interrupting:false}, timer:{ delaySeconds:86400, repeatEverySeconds:86400, maxRepeats:3 } }` · error `{ boundary:{...}, error:{ errorCode:"*" } }` · escalation `{ boundary:{...}, escalation:{ escalationCode:"ESC" } }` · compensation `{ boundary:{ attachedTo:"svc_a" }, compensation:true }` | Interrumpe o acompaña una activity: timeout (una vez o cíclico con recordatorios), error, escalación, o dispara compensación. |
| 14 | Boundary error auto | `service_task` + boundary error `*` | `{ boundary:{...}, error:{ errorCode:"*" } }` | Una falla de service_task (timeout/5xx) enruta automáticamente a la rama de excepción catch-all. |
| 15 | Propagación error/escalation sub→parent | `sub_process` hijo con `end_event` error/escalation | — (automático) | Un error-end o escalation-end dentro de un child sub_process se propaga hacia el `boundary_event` del `sub_process` padre que lo contiene. |
| 16 | Subproceso | `sub_process` (call activity) | `{ callActivity:{ calledProcessversionId:"<uuid>", waitForCompletion:true, passVariables:[], returnVariables:[] } }` | Ejecuta otro processdef como hijo (espera o fire-and-forget), con paso de variables explícito. |
| 17 | Event sub-process | `event_sub_process` | `{ eventSubProcess:{ trigger:"signal|error|message|timer", code:"X", correlationKey:"K", delaySeconds:N, interrupting:true }, callActivity:{ calledProcessversionId:"<uuid>" } }` | Sub-proceso latente dentro de la instancia que se dispara al llegar su trigger (4 tipos), interrumpiendo o no el flujo principal. Suscripción en `bpm_pro_event_subscription_tbl`. |
| 18 | Multi-instance | marcador `config.multiInstance` en `user_task`/`service_task`/`sub_process` | `{ multiInstance:{ collection:"${lines}", elementVar:"line", mode:"parallel\|sequential", outputCollection:"reviews", completionCondition:"${nrOfCompletedInstances >= 3}" } }` | Ejecuta la activity N veces (una por ítem de una colección), en paralelo o secuencial; join por cardinalidad o por `completionCondition`. |
| 19 | Regla de negocio | `business_rule_task` | `{ decisionRef:"credit_approval" }` | Evalúa una tabla de decisión **DMN** (7 hit policies). |
| 20 | DMN DRD chaining | `bpm_dmn_decisiondef_tbl.required_decisions` | columna `required_decisions` (lista de codes) | Una decisión declara de qué otras decisiones depende; el `DmnEvaluator` las resuelve en orden topológico antes de evaluarla. Autoría: `POST /v1/bpm/admin/decisiondef` (+ `DELETE /decisiondef/{code}`). |
| 21 | Compensación / Saga | `config.compensationFor` (+ boundary compensation) | `{ compensationFor:"svc_a" }` en la activity compensadora; disparo con `end_event { compensate:true }` o `intermediate_event { throw:"compensate" }` | Deshace en LIFO lo ya completado (rollback de negocio sin 2PC). |
| 22 | Terminate | `end_event` | `{ terminate:true }` | Aborta la instancia matando todas las ramas vivas. |
| 23 | Incidentes + retry | falla terminal no capturada | — (automático) | Si un `service_task` falla sin boundary error que lo capture, se abre un registro en `bpm_pro_incident_tbl` en vez de perder el token. `GET /v1/bpm/incidents`, `POST /incident/{id}/retry`, `POST /incident/{id}/resolve`. |

Cross-cutting: cache de definiciones (Caffeine), timers multi-réplica (`FOR UPDATE SKIP LOCKED`), auth S2S
(`BpmServiceTokenProvider`), notificaciones live por SSE, migración de instancias entre versiones, y WorkHub
(priorización + semáforo de tareas).

**Expresiones**: condiciones de gateway y evaluación DMN usan **Apache Commons JEXL3** (`${var == 'x'}`), no FEEL.
FEEL (el lenguaje de expresiones estándar de Camunda/DMN) está **diferido por decisión explícita** (2026-07-11):
solo se implementaría si aparece la necesidad de **importar BPMN/DMN estándar de Camunda Modeler**; JEXL3 cubre
toda la autoría propia. Ver `IMAP_MOTOR_BPM_PLAN.md §7.2`.

## Cómo se implementan en este micro

### (a) Patrón `service_task`: handler local vs receptor remoto

`ServiceTaskRegistry` resuelve cada `serviceCode` en dos posibles caminos:

- **Handler local**: una clase anotada `@ServiceTask` dentro del propio `bpm` (p. ej. lógica interna del motor).
- **Receptor remoto**: la gran mayoría. Cada microservicio de negocio expone su propio endpoint
  `POST /v1/service-tasks/execute {serviceCode, variables, idempotencyKey, tenantId, ...}` y responde
  `{status, resultVariables, errorCode?, boundaryErrorCode?}`. `ServiceTaskRegistry` resuelve la `baseUrl` del
  receptor por prefijo del `serviceCode` vía config `bpm.service-tasks.remotes.<prefix>` (ej.
  `bpm.service-tasks.remotes.inventory=https://.../imap/inventory`), y hace el POST S2S con JWT de servicio
  (`BpmServiceTokenProvider`), con retry + timeout configurable.

### (b) Exactly-once

`bpm` genera una **Idempotency-Key determinística** por intento de ejecución: `nameUUID(token:element:encarnación)`
(mismo token+elemento+encarnación → misma key, siempre). El receptor remoto usa `ServiceTaskInboxService` (ver
«Servicios»): al llegar la request intenta un `INSERT ... ON CONFLICT DO NOTHING` sobre su propia tabla
`svctask_inbox` con esa key a modo de *claim*; si el insert no tomó el claim (ya existía), devuelve el resultado ya
cacheado sin volver a ejecutar el efecto de negocio. Esto se copia **igual, en cada micro receptor** — no es algo
que `bpm` centraliza, porque el punto de deduplicación tiene que vivir del lado que ejecuta el efecto.

### (c) Endpoints (autoría, ejecución, mensajes, incidentes)

| Grupo | Endpoint |
|---|---|
| Autoría de procesos | `POST/PUT/DELETE /v1/bpm/admin/processdef` · `POST /processdef/{id}/versions` |
| Autoría de decisiones DMN | `POST/DELETE /v1/bpm/admin/decisiondef` |
| Ejecución | `POST /v1/bpm/process/{versionId}/start` |
| Mensajes | `POST /v1/bpm/messages/start {messageCode, variables, eventId?}` · `POST /v1/bpm/messages/correlate {messageCode, correlationKey, payload}` · `POST /v1/bpm/signals/broadcast {signalCode, payload}` |
| Instancias | `GET /v1/bpm/instance/{id}` · `POST /instance/{id}/cancel` · `DELETE /instance/{id}?force=true` |
| Tareas (WorkHub) | `GET /v1/bpm/me/tasks?view=` · `GET /v1/bpm/me/startable` · `POST /task/{id}/claim` · `POST /task/{id}/complete {outputData}` · `POST /task/{id}/raise-error` |
| Incidentes | `GET /v1/bpm/incidents?lifecycle=open` · `POST /incident/{id}/retry` · `POST /incident/{id}/resolve` |
| Overlay de procesos por tenant | `POST /v1/bpm/tenant-process/{code}/enable` · `DELETE /{code}` (disable) · `PUT /{code}/config` · `GET /v1/bpm/tenant-process` |
| Receptor (cada micro, no `bpm`) | `POST /v1/service-tasks/execute {serviceCode, variables, idempotencyKey, tenantId, ...}` → `{status, resultVariables, errorCode?, boundaryErrorCode?}` |

### (d) Tablas nuevas

- **`bpm_pro_incident_tbl`**: incidentes abiertos por fallas terminales de `service_task` no capturadas por boundary
  error; soporta el ciclo `open → retried/resolved`.
- **`bpm_pro_event_subscription_tbl`**: suscripciones activas de `event_sub_process` (trigger + code +
  correlationKey), consultadas por el motor al despachar signal/message/error/timer.
- **`bpm_dmn_decisiondef_tbl.required_decisions`**: columna nueva en la tabla de definición de decisiones DMN, para
  DRD chaining (lista de decisiones prerequisito, resueltas en orden topológico).
- **`bpm_pro_tenant_process_tbl`** (overlay de procesos por tenant, gemelo de `acc_tenant_account` del plan de
  cuentas): `enabled` (Nivel 1 — el tenant adopta el proceso del catálogo) + `config` jsonb (Nivel 2 — parámetros
  que el motor inyecta como la variable `config` al arrancar; gateways/DMN la leen). Ver `IMAP_BPM_PROCESS_CATALOG.md §1`.

## Integraciones

Orquesta y habla **por HTTP seguro (S2S con JWT)** con el resto de la plataforma: rutea tareas de servicio a
cualquier microservicio (purchase, sale, inventory, tax-ar, accounting, treasury, etc.) vía
`{baseUrl}/v1/service-tasks/execute`, resuelve entidades y carga definiciones desde `system`, y asigna tareas a
usuarios de `iam`. Cero acceso SQL cross-schema — todo cross-service es HTTP. Es él mismo el **orquestador** que
gobierna los procesos de los demás módulos.

## Conformidad hexagonal

Hexagonal estricto **grade A**: domain puro (POJOs sin `@Entity` ni imports de infraestructura), `@Entity` solo en
`infrastructure/entity` (24), ports devuelven modelos de dominio, controllers siempre vía application services.
Audit-7 en las 24 entidades (con `updatable=false` en columnas núcleo) y RLS por tenant en todas las tablas.
**Exenciones documentadas**: el motor BPM (`ProcessEngine`) accede a repositorios de entidad directamente por ser la
máquina de estados central (§F.6); las tablas de estado de runtime del roadmap Camunda-8
(`bpm_pro_incident_tbl`, `bpm_pro_event_subscription_tbl`) entran en la misma exención §F.6 por ser estado interno
del motor, no agregados de dominio con reglas de negocio propias; el cleanup bulk de tablas de infraestructura
(jobs) usa DELETE físico; y `bpm_inbox_event_tbl` va sin RLS por deduplicación fuera de la transacción. **Gaps**:
solo menores (BAJA — falta Bean Validation declarativa en DTOs, sin i18n en errores de negocio, sin suite JUnit) y
uno MEDIA de mantenibilidad (`ProcessEngine` monolítico, candidato a patrón Strategy por tipo de elemento). Sin
gaps ALTA.
