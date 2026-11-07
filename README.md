# Arquitectura Orientada a Eventos (EDA) y Patrón Publicación/Suscripción (Pub/Sub) con RabbitMQ
**Universidad Tecnológica Nacional (UTN) — Sistemas Distribuidos**  
*Implementación práctica integrada de desacoplamiento espacial, temporal y de ejecución, enrutamiento por tópicos, eventos ECST vs. Thin, idempotencia y Cola de Mensajes Muertos (DLQ).*

---

## 1. Descripción del Proyecto y Conceptos Demostrados

Este repositorio implementa una **Arquitectura Orientada a Eventos (EDA)** apoyada en el patrón **Publicación/Suscripción (Pub/Sub)** mediante **RabbitMQ** y microservicios en **Node.js** contenerizados con **Docker**.

A diferencia de una arquitectura acoplada por llamadas HTTP síncronas en cascada, este escenario demuestra en vivo todos los fundamentos analizados en el informe teórico:

1. **Separación entre EDA (Estilo Arquitectónico) y Pub/Sub (Mecanismo de Transporte):**
   * **EDA** define *qué* se comunica: hechos históricos inmutables del dominio empaquetados en un **Sobre de Evento (`EventEnvelope`)** con metadatos de trazabilidad (`idEvento`, `tipoEvento`, `topico`, `fechaHora`, `idPedido`, `idCorrelacion`, `version` y `datos`).
   * **Pub/Sub** define *cómo* se reparte: a través de un intercambiador (**Exchange**) de tipo `topic` (`intercambiador_pedidos`), el productor publica el evento una única vez sin conocer cuántos suscriptores existen ni dónde se ubican (**desacople espacial**).
2. **Enrutamiento Selectivo por Tópicos y Comodines (`topic` exchange):**
   * `cola_envios` se suscribe únicamente a `pedido.creado`.
   * `cola_inventario` y `cola_facturacion` se suscriben a `pedido.creado` y `pedido.cancelado`.
   * `cola_notificaciones` se suscribe mediante el comodín **`pedido.#`**, recibiendo absolutamente todos los eventos del ciclo de vida del pedido para alertas y auditoría.
3. **Taxonomía de Eventos — *Event-Carried State Transfer (Fat / ECST)* vs. *Event Notification (Thin)*:**
   * **`PedidoCreado` (Evento Fat / ECST):** Transporta el estado completo del pedido (`cliente`, `articulos`, `total`). Permite que Inventario, Facturación y Envíos operen de forma 100% autónoma sin realizar llamadas de retorno al productor.
   * **`PedidoCancelado` (Evento Thin / Notificación Ligera):** Transporta únicamente el identificador `idPedido` y el `motivo`. Si un consumidor no posee el estado previo en caché local, realiza un *callback* síncrono `GET /pedidos/:idPedido` hacia la API de origen.
4. **Desacople Temporal y Confirmación Manual (`Manual ACK`):**
   * Las colas son durables y los mensajes persistentes. Si un consumidor (ej. `servicio-facturacion`) se detiene, el productor sigue operando con normalidad y RabbitMQ retiene los mensajes hasta que el servicio vuelve a estar en línea.
5. **Garantía *At-Least-Once*, Idempotencia y Control de Versiones:**
   * Cada consumidor registra los `idEvento` procesados y la última `version` aplicada por pedido (`version_entrante > version_almacenada`), evitando cobros duplicados o descuentos dobles de stock ante reintentos de red.
6. **Aislamiento de Fallos con Cola de Mensajes Muertos (*Dead Letter Queue - DLQ*):**
   * Todas las colas están vinculadas al intercambiador `intercambiador_dlq`. Si un mensaje falla en su validación o procesamiento, el consumidor emite `canal.nack(mensaje, false, false)` y RabbitMQ lo aparta automáticamente en `cola_mensajes_muertos` sin frenar el flujo principal.

---

## 2. Arquitectura de la Solución

```
                           +-----------------------------------+
                           |      Cliente HTTP / Script        |
                           +-----------------+-----------------+
                                             |
                        POST /pedidos        |   POST /pedidos/:id/cancelar
                        (Evento Fat / ECST)  |   (Evento Thin / Ligero)
                                             v
                           +-----------------------------------+
                           |            api-pedidos            |
                           |      (Productor del Dominio)      |
                           +-----------------+-----------------+
                                             | Publica Sobre de Evento (EventEnvelope)
                                             v
                           +-----------------------------------+
                           |       intercambiador_pedidos      |
                           |       (Exchange Tipo: TOPIC)      |
                           +---+---------+---------+---------+--+
                               |         |         |         |
       +-----------------------+         |         |         +-----------------------+
       | pedido.creado                   |         | pedido.creado                   | pedido.#
       | pedido.cancelado                |         | (Selectivo)                     | (Comodín Global)
       v                                 v         v                                 v
[cola_inventario]              [cola_facturacion] [cola_envios]            [cola_notificaciones]
       |                                 |         |                                 |
       v                                 v         v                                 v
+---------------------+       +--------------------+ +-------------------+ +-------------------------+
| servicio-inventario |       |servicio-facturacion| |  servicio-envios  | | servicio-notificaciones |
| (Reserva/Repone)    |       | (Factura/Anula IVA)| | (Guía de Despacho)| |  (Correos y Auditoría)  |
+----------+----------+       +---------+----------+ +---------+---------+ +------------+------------+
           |                            |                      |                        |
           +----------------------------+-----------+----------+------------------------+
                                                    | Si ocurre error -> NACK (sin reencolar)
                                                    v
                                    +-------------------------------+
                                    |     cola_mensajes_muertos     |
                                    |   (Dead Letter Queue - DLQ)   |
                                    +-------------------------------+
```

---

## 💻 3. Puesta en Marcha

### 3.1. Levantar todos los contenedores
Desde la carpeta `ecommerce-eda-demo`, ejecutar:

```bash
docker compose up --build -d
```

### 3.2. Verificar el estado de los contenedores
```bash
docker compose ps
```
Verás los **6 contenedores** activos:
* `broker-rabbitmq` (puertos `5672` y panel web en `15672`)
* `productor-api-pedidos` (puerto HTTP `3000`)
* `consumidor-inventario`
* `consumidor-facturacion`
* `consumidor-envios`
* `consumidor-notificaciones`

---

## 🎯 4. Ejecución de la Demo en Vivo (`demo.ps1`)

El repositorio incluye un script interactivo en PowerShell (`demo.ps1`) diseñado para demostrar en vivo cada uno de los escenarios durante el coloquio:

```powershell
.\demo.ps1
```

También puedes ejecutar cada escenario directamente por número o correr todos en secuencia:
* `.\demo.ps1 1` — **Escenario 1 (Pub/Sub Concurrente, Coreografía y Evento Fat / ECST):** Crea un pedido (`POST /pedidos`) y muestra cómo los 4 consumidores reaccionan en paralelo sin bloquear al cliente.
* `.\demo.ps1 2` — **Escenario 2 (Enrutamiento Selectivo por Tópicos y Evento Thin):** Cancela el pedido (`POST /pedidos/PED-UTN-101/cancelar`). Demuestra que Inventario, Facturación y Notificaciones reciben el evento ligero `pedido.cancelado`, mientras que Envíos (suscrito sólo a `pedido.creado`) no es invocado.
* `.\demo.ps1 3` — **Escenario 3 (Desacople Temporal y Recuperación ante Caída):** Detiene `servicio-facturacion`, emite un nuevo pedido, verifica mediante la API de RabbitMQ que el mensaje quedó retenido en `cola_facturacion`, vuelve a encender el contenedor y comprueba el procesamiento diferido automático.
* `.\demo.ps1 4` — **Escenario 4 (Idempotencia y Control de Versiones):** Envía dos veces seguidas un evento con el mismo `idEvento` (`EVT-DUPLICADO-999`) y demuestra cómo todos los consumidores detectan el duplicado y evitan cobrar o descontar stock por duplicado.
* `.\demo.ps1 5` — **Escenario 5 (Cola de Mensajes Muertos / DLQ):** Envía un pedido con datos inválidos (`total: -50`, `simularFallo: true`) y comprueba cómo los consumidores rechazan el mensaje (`NACK`) y RabbitMQ lo aísla en `cola_mensajes_muertos`.
* `.\demo.ps1 todos` — Ejecuta los 5 escenarios de principio a fin.

---

## 📊 5. Panel de Administración de RabbitMQ

* **URL:** [http://localhost:15672](http://localhost:15672)
* **Usuario:** `guest` | **Contraseña:** `guest`

En el panel web se puede inspeccionar en vivo:
1. **Pestaña `Exchanges`:** El intercambiador `intercambiador_pedidos` (tipo `topic`) con sus reglas de enrutamiento (*Bindings*) y `intercambiador_dlq` (tipo `fanout`).
2. **Pestaña `Queues`:** Las 4 colas operativas (`cola_inventario`, `cola_facturacion`, `cola_envios`, `cola_notificaciones`) y la cola de aislamiento de fallos `cola_mensajes_muertos` (DLQ).

---

## 6. Detener el Entorno

```bash
docker compose down -v
```
