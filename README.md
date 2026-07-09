# imap-backend-bpm

Microservicio IMAP — **Orquestador de Procesos (BPM)**. Motor BPMN central que coordina los flujos de trabajo de todos los módulos de la plataforma.

- **Puerto**: 8093 · **Context path**: `/imap/bpm` · **Schema**: `bpm` · **Prefijo**: `bpm_`
- **Package**: `com.imap.bpm` · Stack estándar IMAP (Spring Boot 3.2.3, Java 17, hexagonal estricto, PostgreSQL, Flyway).

## Estado

**En producción — hexagonal estricto ✅ (grade A).** Motor BPMN completo con instancias, tokens, tareas, variables y auditoría.
122 clases Java, 17 migraciones Flyway (V001–V017), RLS activo en todas las tablas y smoke E2E funcional
(`smoke_service_task_registry.ps1`: login → crear processdef → start → completar → verificar audit + service_task).
Fase actual de consolidación (Fase 4-mgmt): migración de definiciones de procesos desde el EAV de `system` a tablas
relacionales propias en `bpm`.

## Para qué sirve

Es el cerebro que orquesta el trabajo de la empresa. Modela cada proceso de negocio (una compra, una venta, una
liquidación de impuestos, un onboarding) como un flujo BPMN y lo ejecuta paso a paso: crea tareas para las personas,
dispara acciones automáticas en otros módulos, evalúa reglas de decisión y deja trazabilidad completa de cada paso.
Un mismo motor gobierna todos los módulos, garantizando que los procesos se ejecuten siempre igual, auditados y
por tenant.

## Servicios

| Servicio | Qué hace | Quién lo usa |
|---|---|---|
| ProcessEngine | Motor central que ejecuta la máquina de estados BPMN (inicio, fin, tareas de usuario, tareas de servicio, gateways, eventos, subprocesos) avanzando los tokens y persistiendo el estado. | API de procesos + internamente el worker de timers y el bus de notificaciones SSE. |
| ProcessdefManagementService | Alta y edición de definiciones de proceso en tablas relacionales: crea la definición, su versión, elementos de flujo y secuencias en una transacción atómica con validación de topología. | Endpoint de administración de definiciones. |
| ServiceTaskRegistry | Despachador de acciones automáticas: descubre handlers locales y rutea los remotos vía HTTP seguro a otros microservicios, con reintentos y timeout configurable. | El motor, al ejecutar una tarea de servicio. |
| MigrationPlanManagementService | Gestión de planes de migración de una versión de proceso a otra: crea, edita y valida las reglas de transformación. | Endpoint de administración de migraciones. |
| MigrationApplyService | Aplica un plan de migración sobre instancias en curso: reubica los tokens a la nueva versión, aplica las reglas, audita los cambios y emite notificaciones. | Endpoint de aplicación de migraciones. |
| JobExecutorWorker | Worker programado que revisa periódicamente los trabajos pendientes (timers) y dispara los eventos temporizados cuando llega su hora. | Planificador de tareas en segundo plano (sin HTTP directo). |
| TaskAssignmentService | Resuelve a quién se asigna cada tarea de usuario al crearse (persona real o, si el iniciador es una cuenta de servicio, fallback a administrador). | El motor, al crear una tarea de usuario. |
| ScoreService + WorkHubConfigService | Cálculo de prioridad y semáforo de color de las tareas en la bandeja de trabajo (WorkHub) según reglas. | API de procesos + administración de WorkHub. |
| DmnEvaluator | Evaluador de reglas de decisión (DMN): aplica operadores declarativos y política de resolución sobre las tablas de decisión. | El motor, al ejecutar una tarea de regla de negocio. |
| SystemEntityResolver | Resuelve códigos de entidad de `system` vía HTTP seguro y cachea la relación código→UUID. | Validación de definiciones de proceso que referencian entidades. |

## Integraciones

Orquesta y habla **por HTTP seguro (S2S con JWT)** con el resto de la plataforma: rutea tareas de servicio a
cualquier microservicio (purchase, sale, inventory, tax-ar, accounting, treasury, etc.) vía
`{baseUrl}/v1/service-tasks/execute`, resuelve entidades y carga definiciones desde `system`, y asigna tareas a
usuarios de `iam`. Cero acceso SQL cross-schema — todo cross-service es HTTP. Es él mismo el **orquestador** que
gobierna los procesos de los demás módulos.

## Conformidad hexagonal

Hexagonal estricto **grade A**: domain puro (POJOs sin `@Entity` ni imports de infraestructura), `@Entity` solo en
`infrastructure/entity` (20), ports devuelven modelos de dominio, controllers siempre vía application services.
Audit-7 en las 20 entidades (con `updatable=false` en columnas núcleo) y RLS por tenant en todas las tablas.
**Exenciones documentadas**: el motor BPM (`ProcessEngine`) accede a repositorios de entidad directamente por ser la
máquina de estados central (§F.6); el cleanup bulk de tablas de infraestructura (jobs) usa DELETE físico; y
`bpm_inbox_event_tbl` va sin RLS por deduplicación fuera de la transacción. **Gaps**: solo menores (BAJA — falta
Bean Validation declarativa en DTOs, sin i18n en errores de negocio, sin suite JUnit) y uno MEDIA de
mantenibilidad (`ProcessEngine` monolítico, candidato a patrón Strategy por tipo de elemento). Sin gaps ALTA.
