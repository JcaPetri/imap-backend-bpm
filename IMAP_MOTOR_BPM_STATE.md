# IMAP — Estado del Motor BPM vs Camunda 8 y vs BPMN 2.0 (la norma)

> **Foto: 2026-07-11.** Motor en prod (`bpm`, 8093, schema `bpm`, 129 clases, 22 migraciones V001–V022).
> Roadmap de brecha vs Camunda 8 **cerrado** (Olas 1–7.1, todas en prod con smokes E2E verdes).
> Docs relacionados: `IMAP_MOTOR_BPM.md` (catálogo + ejemplos), `IMAP_MOTOR_BPM_PLAN.md` (plan por olas),
> `imap.backend/bpm/README.md` (implementación), `IMAP_GUIA_DESARROLLO.md §5` (receta de integración).

---

## 0. TL;DR (veredicto)

- **vs BPMN 2.0 (la norma):** IMAP implementa el **subset EJECUTABLE** de BPMN 2.0 que tiene valor de negocio real
  en un ERP — la clase de conformidad relevante para un motor es **Process Execution Conformance** (semántica de
  ejecución de los elementos), no la notación gráfica. Cubierto: **los 4 gateways** (XOR/AND/OR/event-based), la
  mayoría de **eventos** (start/intermediate/boundary/end en message/timer/signal/error/escalation/compensation/
  terminate), **call activity** + **event sub-process** (4 triggers), **multi-instance** (parallel/sequential +
  completionCondition), **compensation/Saga**, **DMN** + **DRD chaining**, e **incidentes+retry**. Fuera: elementos
  redundantes (script/manual/send/receive task), conditional/link events, transaction/ad-hoc subprocess, y las capas
  de **diagramación** (pools/lanes/choreography/collaboration) — parte por **diseño** (no-goal), parte **on-demand**.
- **vs Camunda 8 (Zeebe):** **paridad funcional** en todo lo que un ERP necesita ejecutar. Diferencias por decisión
  consciente: **JEXL3** en vez de **FEEL** (diferido, sin driver); arquitectura **relacional** (Postgres+RLS), no
  **event-sourced/particionada** tipo Zeebe (no-goal — esa escala no se necesita en años); tooling propio
  (form-driven creator + viewer + WorkHub) en vez de Modeler/Operate/Tasklist/Optimize.
- **Dónde IMAP está ADELANTE:** multi-tenancy **RLS por tenant + idioma por usuario** (más granular que Camunda),
  **"todo es un proceso"** con **forms EAV zero-code** por tarea, orquestación **HTTP-only de N micros** con audit-7
  + RLS en cada tabla, **WorkHub** (bandeja gobernada por proceso con priorización gravedad×urgencia×tendencia).
- **Dónde está ATRÁS / diferido:** FEEL, event-sourcing/partición (escala Zeebe), Modeler drag-drop, analytics
  tipo Optimize, y un puñado de elementos BPMN nicho (conditional/link events, transaction subprocess).

**Resumen en una línea:** el motor está **a la par de Camunda 8 en capacidad de ejecución para un ERP**, con
decisiones de arquitectura distintas (relacional multi-tenant en vez de event-sourced) y algunas piezas de
tooling/escala conscientemente diferidas.

---

## 1. Alcance y metodología

- **Qué se compara:** la **semántica de ejecución** (runtime) — qué flujos puede modelar y correr el motor — no la
  fidelidad de la **notación gráfica** ni la interoperabilidad de archivos `.bpmn`/`.dmn` (eso último es la brecha
  FEEL/import, ver §5).
- **La "norma":** BPMN 2.0 de OMG define varias clases de conformidad. Para un motor la que aplica es **Process
  Execution Conformance**: implementar la semántica de ejecución de los elementos del subset ejecutable. IMAP apunta
  a esa clase para los elementos de uso real; NO apunta a Modeling/Choreography Conformance (notación completa).
- **Leyenda de estado:**
  | Símbolo | Significado |
  |---|---|
  | ✅ | Soportado y en prod (con smoke E2E). |
  | 🟡 | Soportado con una variante/limitación (se indica). |
  | 🔵 | No implementado, **on-demand** (sin driver hoy; se agrega cuando aparezca un caso). |
  | ⛔ | **No-goal consciente** (no se implementará salvo cambio de estrategia; se explica por qué). |

---

## 2. IMAP vs BPMN 2.0 — cobertura elemento por elemento

### 2.1 Eventos

**Start events**
| BPMN 2.0 | IMAP | Nota |
|---|---|---|
| None (arranque manual) | ✅ | `start_event` sin config; `POST /process/{ver}/start`. |
| Message | ✅ | `config.message.messageCode`; suscripción sincronizada **al activar** el processdef; arranca por `POST /messages/start` (outbox micros→bpm) o `BpmMessageEmitter`. |
| Timer | 🔵 | Start-por-timer (cron/schedule) no está; hoy los timers son intermediate/boundary. Los cron externos (systemd) llaman a message-start. |
| Signal | 🔵 | Start-por-signal broadcast no está (el broadcast reactiva catches, no arranca defs). |
| Conditional | 🔵 | — |
| Error / Escalation / Compensation (start de event sub-process) | ✅ | Vía `event_sub_process` con trigger error/message/signal/timer (§2.2). |

**Intermediate catch events**
| BPMN 2.0 | IMAP | Nota |
|---|---|---|
| Timer | ✅ | `config.timer.delaySeconds`. |
| Message | ✅ | `config.message.messageCode` + `correlationKey`. |
| Signal | ✅ | `config.signal.signalCode` (broadcast). |
| Conditional | 🔵 | — |
| Link | 🔵 | "Goto" intra-diagrama; se modela con sequence flows. |

**Intermediate throw events**
| BPMN 2.0 | IMAP | Nota |
|---|---|---|
| Signal (throw) | ✅ | `config.throw:"signal"` → broadcast a los catches armados. |
| Message (throw) | ✅ | `config.throw:"message"` → con `correlationKey` correla a una instancia viva; sin key arranca subscripciones message-start. |
| Compensation (throw) | ✅ | `config.throw:"compensate"` → compensación LIFO mid-flow (sin terminar la instancia). |
| Escalation (throw) | 🟡 | Se emite vía `end_event escalation` con propagación al boundary del padre (§2.2); no como throw intermedio standalone. |
| None / Link | 🔵 | — |

**Boundary events** (interrupting / non-interrupting)
| BPMN 2.0 | IMAP | Nota |
|---|---|---|
| Timer | ✅ | Interrupting + non-interrupting + **cíclico** (`repeatEverySeconds`+`maxRepeats`). |
| Error | ✅ | Auto desde falla de `service_task` (catch-all `*` o code exacto). |
| Escalation | ✅ | Interrupting o non-interrupting. |
| Compensation | ✅ | `config.compensation:true` sobre la activity; su flow saliente = handler. |
| Message | 🔵 | Boundary message (esperar un mensaje mientras la activity corre) no está; se modela con event-based gateway o event sub-process. |
| Signal / Conditional | 🔵 | — |

**End events**
| BPMN 2.0 | IMAP | Nota |
|---|---|---|
| None | ✅ | Cierra el token/instancia. |
| Terminate | ✅ | `config.terminate:true` — aborta todas las ramas vivas. |
| Error | ✅ | `config.error.errorCode` — propaga al boundary error del sub_process padre. |
| Escalation | ✅ | `config.escalation.escalationCode` — propaga (interrupting o paralelo). |
| Compensation | ✅ | `config.compensate:true` — dispara la compensación LIFO antes de cerrar. |
| Message / Signal (end) | 🟡 | Se logra con el patrón throw (un `intermediate throw` antes del end). |

### 2.2 Actividades (tasks + subprocess + markers)

**Tasks**
| BPMN 2.0 | IMAP | Nota |
|---|---|---|
| Service Task | ✅ | `service_task` + `serviceCode` → handler local (`@ServiceTask`) o remoto (HTTP S2S), retry+timeout, **async continuation**, **exactly-once**. |
| User Task | ✅ | `user_task` + WorkHub (form EAV dinámico, candidateGroup/DMN routing, claim CAS). |
| Business Rule Task | ✅ | `business_rule_task` + DMN (7 hit policies) + **DRD chaining**. |
| Send Task | 🟡 | Redundante: un `service_task` o un `intermediate throw message`. |
| Receive Task | 🟡 | Redundante: un `intermediate catch message`. |
| Manual Task | 🔵 | Trivial (un `user_task` sin acción de sistema). |
| Script Task | ⛔ | **No-goal por seguridad** — ejecutar scripts arbitrarios en el motor es superficie de ataque; la lógica va en handlers tipados (service_task) o DMN. |

**Subprocess**
| BPMN 2.0 | IMAP | Nota |
|---|---|---|
| Call Activity (reusable) | ✅ | `sub_process` + `callActivity.calledProcessversionId` (wait o fire-and-forget). |
| Event Sub-Process | ✅ | `event_sub_process` dormante, **4 triggers** (signal/error/message/timer), interrupting o no. Scope de instancia. |
| Embedded Sub-Process (inline scope) | 🟡 | Se modela como call activity a otra processversion (scope compartido), no como scope inline anidado — **scope anidado diferido**. |
| Transaction Sub-Process | 🔵 | Semántica de transacción BPMN (compensación al abortar) se cubre parcialmente con Saga/compensation. |
| Ad-Hoc Sub-Process | 🔵 | — |

**Markers**
| BPMN 2.0 | IMAP | Nota |
|---|---|---|
| Multi-Instance (parallel) | ✅ | `config.multiInstance` sobre user_task/sub_process. |
| Multi-Instance (sequential) | ✅ | `mode:"sequential"` + `completionCondition` (quórum/fail-fast). |
| Loop (standard) | 🔵 | While-loop; se modela con un gateway cíclico. MI cubre el 99% de los casos reales. |
| Compensation | ✅ | `compensationFor` / boundary compensation + disparo por end/throw compensate. |

### 2.3 Gateways
| BPMN 2.0 | IMAP | Nota |
|---|---|---|
| Exclusive (XOR) | ✅ | `exclusive_gateway`, condición JEXL + default. |
| Parallel (AND) | ✅ | `parallel_gateway`, split/join, **M×N** (join-then-split). |
| Inclusive (OR) | ✅ | `inclusive_gateway`; join estructurado (cardinalidad) **o full-reachability** (dead-path elimination para topologías no-estructuradas). |
| Event-Based | ✅ | `event_based_gateway` — carrera de eventos, gana el primero. |
| Complex | ⛔ | Semántica ambigua del spec, casi nunca usada; **no-goal**. Se modela con inclusive + condiciones. |
| Parallel Event-Based | 🔵 | — |

### 2.4 Datos, flujos, diagramación
| BPMN 2.0 | IMAP | Nota |
|---|---|---|
| Sequence Flow (condicional / default) | ✅ | `conditionExpr` JEXL en el flow + default. |
| Data Objects / Data Stores | 🟡 **por diseño** | IMAP usa **variables de proceso** (contexto JEXL) para el estado del flujo; los **datos de negocio viven en los micros** (Regla de Oro: definitions↔execution, datos en el dominio). No hay data objects BPMN de primera clase. |
| Message Flow (entre pools) | 🟡 | El cruce entre procesos se hace por **message-start / correlate / signal**, no por message flow de diagrama. Funcionalmente equivalente. |
| Association (compensación / anotación) | 🟡 | La asociación de compensación se declara con `compensationFor` / boundary compensation. |
| Pools / Lanes (swimlanes) | ⛔ **por diseño** | La asignación no se modela con lanes sino con **candidateGroup + DMN routing + IAM** (roles/grupos). Más flexible para multi-tenant. |
| Choreography / Collaboration / Conversation | ⛔ **no-goal** | Otro paradigma (interacción entre participantes). IMAP orquesta por HTTP + message-start chaining. |
| Groups / Text Annotations | ⛔ | Cosmético de diagrama; no aplica a un motor de ejecución. |

---

## 3. IMAP vs Camunda 8 (Zeebe) — por dimensión

| Dimensión | Camunda 8 (Zeebe) | IMAP | Veredicto |
|---|---|---|---|
| **Arquitectura de ejecución** | Event-sourced, **particionado** (consenso Raft), horizontalmente escalable, streaming de jobs por gRPC. | **Relacional** (PostgreSQL), estado en tablas (instancia/token/job/task), single-writer por instancia, **RLS multi-tenant**. | **Distinto por diseño.** IMAP no persigue la escala/HA de Zeebe (**no-goal** — no se necesita en años; implicaría reescribir el motor). Para el volumen de un ERP, el modelo relacional alcanza y es más simple de operar. |
| **Patrón de "workers"** | External job workers **pull/stream** jobs (gRPC). | **Push** por HTTP S2S al receptor `/v1/service-tasks/execute` de cada micro + handlers locales `@ServiceTask`. | **Paridad funcional.** IMAP HTTP-only (Regla de Oro; futuro Kafka como seam). |
| **Exactly-once** | Garantizado por event-sourcing. | **At-least-once + receptor idempotente** = exactly-once efectivo: Idempotency-Key **determinística** (`nameUUID(token:element:encarnación)`, estable ante crash/restart) + dedup en el receptor (`svctask_inbox`). | **Paridad práctica** (distinto mecanismo, mismo resultado de negocio). |
| **Expresiones** | **FEEL** (lenguaje estándar DMN/BPMN). | **Apache Commons JEXL3** (`${...}`). | **Atrás en interoperabilidad** (no importa `.bpmn`/`.dmn` de Camunda Modeler tal cual). FEEL **diferido** (§5). JEXL cubre toda la autoría propia. |
| **DMN** | Camunda DMN (FEEL), DRD. | Motor DMN propio: **7 hit policies** (unique/first/priority/any/collect/rule-order/output-order) + **DRD chaining** (orden topológico) + autoría por API. | **Paridad** en capacidad; expresiones en JEXL. |
| **Timers** | Sí (multi-partición). | Sí, **multi-réplica** (`FOR UPDATE SKIP LOCKED`), cíclicos. | Paridad. |
| **Message correlation** | Sí (buffering, TTL). | Sí (correlate por key, broadcast de signals, message-start con dedup de `eventId`). | Paridad (sin buffering/TTL configurable — on-demand). |
| **Multi-instance** | parallel/sequential + completionCondition. | parallel/sequential + completionCondition (quórum/fail-fast). | Paridad. |
| **Compensación / Saga** | Sí. | Sí (compensationFor / boundary + end/throw compensate, LIFO). | Paridad. |
| **Event sub-process** | 4 triggers, interrupting/non-interrupting. | 4 triggers, interrupting/non-interrupting (scope de instancia; **scope anidado diferido**). | Paridad en scope de instancia. |
| **Incidentes** | Incidents + retry (Operate). | `bpm_pro_incident_tbl` + `POST /incident/{id}/retry` /resolve + panel en ProcessAdminPage. | Paridad. |
| **Migración de instancias** | Process instance migration. | Planes de migración (reubica tokens a la nueva versión + reglas). | Paridad. |
| **Multi-tenancy** | Multi-tenancy (agregado reciente). | **RLS por tenant desde el diseño** + **idioma por usuario** (no por tenant) + audit-7 en toda tabla. | **IMAP adelante** (más granular e integrado). |
| **Tooling — autoría** | **Modeler** drag-drop (BPMN/DMN). | Form-driven **Processdef Creator** + **BPMN.js viewer** read-only + autoría DMN por API. | **Atrás** (Modeler drag-drop es backlog `bpm_modeler`). |
| **Tooling — monitoreo** | **Operate** (instancias, incidentes, heatmap). | ProcessAdminPage: viewer + **token highlighting live (SSE)** + panel de incidentes. | Parcial (cubre lo esencial; sin heatmap/analytics). |
| **Tooling — tareas humanas** | **Tasklist**. | **WorkHub** (bandeja por procesos, 3 zonas, claim CAS, **priorización gravedad×urgencia×tendencia + semáforo**, i18n). | **IMAP a la par o adelante** (scoring/prioridad gobernada por proceso). |
| **Tooling — analytics** | **Optimize**. | — (Prometheus + WorkHub). | **Atrás** (sin Optimize; **no-goal** por ahora). |
| **Connectors** | Marketplace de connectors out-of-the-box. | HTTP S2S a los propios micros + **exchangedata-engine** (Telegram/WhatsApp/AFIP/email/PDF) como librería de E/S. | **Distinto modelo** (integración por micros propios, no marketplace). |
| **Forms** | Camunda Forms / embedded. | **Forms EAV zero-code** por tarea (renderer federado de uxdesign desde el `EntityDef`). | **IMAP adelante** (zero-code, dinámico por tenant). |

---

## 4. Dónde IMAP está a la par / adelante / atrás (síntesis)

**A la par (capacidad de ejecución para un ERP):**
- Los 4 gateways · eventos core (message/timer/signal/error/escalation/compensation/terminate) · call activity ·
  event sub-process (4 triggers) · multi-instance (parallel/sequential + completionCondition) · compensation/Saga ·
  DMN (7 hit policies + DRD chaining) · incidentes+retry · timers multi-réplica · exactly-once efectivo ·
  migración de instancias.

**Adelante:**
- **Multi-tenancy RLS por tenant + idioma por usuario** (aislamiento más fino que el tenant model de Camunda).
- **"Todo es un proceso" + forms EAV zero-code** por tarea (diferencial que Flowable/Activiti/Camunda no traen out-of-the-box).
- **Orquestación HTTP-only de N micros** con audit-7 + RLS en cada tabla + soft-delete + idempotencia.
- **WorkHub** — bandeja gobernada por proceso con scoring/prioridad, a la par o adelante de Tasklist.

**Atrás / diferido (con justificación):**
- **FEEL** — diferido explícito (sin driver de import de BPMN/DMN estándar; JEXL alcanza). Ver §5.
- **Event-sourcing / partición (escala Zeebe)** — no-goal consciente (esa escala no se necesita en años).
- **Modeler drag-drop** — backlog (`bpm_modeler`); hoy form-driven creator + viewer read-only.
- **Analytics tipo Optimize** — sin driver.
- **Elementos BPMN nicho** — conditional events, link events, transaction/ad-hoc subprocess, complex gateway,
  message/signal boundary, start por timer/signal, scope embebido anidado, message-throw... → on-demand.

---

## 5. No-goals conscientes (no se construye sin cambio de estrategia)

| No-goal | Por qué |
|---|---|
| **Zeebe / event-sourcing / partición** | La escala/HA de Zeebe (miles de instancias/seg, clúster particionado) no se necesita en años; adoptarla implicaría reescribir el motor. El modelo relacional multi-tenant es más simple de operar y suficiente para el volumen de un ERP. |
| **FEEL** | Costo alto (implementar un lenguaje completo). Solo pagaría si aparece la necesidad de **importar BPMN/DMN estándar de Camunda Modeler**. JEXL3 cubre toda la autoría propia. Diferido explícito (2026-07-11). |
| **Script Task** | Ejecutar scripts arbitrarios en el motor = superficie de ataque. La lógica va en handlers tipados (service_task) o DMN. |
| **Choreography / Collaboration / Conversation** | Otro paradigma (interacción entre participantes). IMAP orquesta por HTTP + message-start chaining. |
| **Pools / Lanes** | La asignación se modela con candidateGroup + DMN routing + IAM (roles/grupos), más flexible para multi-tenant. |
| **Complex gateway** | Semántica ambigua del spec, casi nunca usada; se modela con inclusive + condiciones. |
| **Optimize-style analytics** | Prometheus + WorkHub alcanzan; sin driver de negocio. |

---

## 6. Conclusión

El motor BPM de IMAP alcanzó **paridad funcional con Camunda 8 en la capacidad de EJECUCIÓN que un ERP necesita**,
cubriendo el subset ejecutable de **BPMN 2.0** (Process Execution Conformance) para todos los elementos de uso real.
Las diferencias con Camunda son **decisiones de arquitectura conscientes** — relacional multi-tenant en vez de
event-sourced/particionado, JEXL en vez de FEEL, tooling propio (Creator + WorkHub) en vez de Modeler/Operate/
Tasklist/Optimize — y en **multi-tenancy, forms zero-code y "todo es proceso"** IMAP está por delante.

Lo que queda es **on-demand sin driver** (elementos BPMN nicho, FEEL, Modeler drag-drop, analytics) o **no-goal
consciente** (escala Zeebe, choreography, script tasks). Ninguno bloquea la operación de un ERP; se suman cuando
aparezca un caso real que los pida.

**Estado: motor completo para producción de ERP. Norte lejano: Camunda 8 como referencia de features, adoptadas
por ROI, no por completitud del estándar.**
