# Arquitectura Orientada a Eventos (EDA) con Microservicios y RabbitMQ (Pub/Sub)
**Universidad Tecnológica Nacional (UTN)**  
*Demostración práctica de desacoplamiento, concurrencia y mensajería asíncrona con Node.js y Docker.*

---

## 📌 1. Descripción del Proyecto

Este repositorio contiene una solución completa basada en una **Arquitectura Orientada a Eventos (Event-Driven Architecture - EDA)** aplicando el patrón **Publicador/Suscriptor (Pub/Sub)** sobre un broker de mensajería **RabbitMQ**.

### ¿Qué problema resolvemos con EDA?
En un diseño monolítico o síncrono tradicional (HTTP REST directo entre servicios), cuando un cliente realiza una compra, el servicio de órdenes debe invocar secuencialmente a Inventario, Facturación y Envíos. Si cualquiera de ellos falla, responde lento o está caído, la operación completa se bloquea o falla.

Con **EDA y RabbitMQ**:
1. **Desacoplamiento total**: El servicio productor (`orders-api`) no conoce qué servicios existen detrás ni cuántos son; únicamente publica el evento de negocio `PedidoCreado` en un **Exchange** de tipo `fanout`.
2. **Difusión en Abanico (Fanout)**: RabbitMQ clona y entrega el mensaje en tiempo real a todas las colas suscritas (`inventario_queue`, `facturacion_queue`, `envios_queue`).
3. **Procesamiento Concurrente y Paralelo**: Los microservicios consumidores procesan el pedido de forma simultánea e independiente sin bloquear la respuesta al usuario final.
4. **Tolerancia a Fallos y Fiabilidad (Manual ACK)**: Los workers confirman la recepción (`channel.ack(msg)`) sólo cuando completaron el procesamiento. Si un worker se detiene, el mensaje permanece seguro en RabbitMQ.

```
                      +-----------------------------+
                      |   Cliente HTTP / Frontend   |
                      +--------------+--------------+
                                     |
                             POST /orders
                                     v
                      +-----------------------------+
                      |         orders-api          |
                      |   (Productor del Evento)    |
                      +--------------+--------------+
                                     | Publica evento: "PedidoCreado"
                                     v
                      +-----------------------------+
                      |  pedidos_exchange (Fanout)  |
                      +-------+------+-------+------+
                              |      |       |
            +-----------------+      |       +-----------------+
            | Difusión (Fanout)      | Difusión (Fanout)       | Difusión (Fanout)
            v                        v                         v
     [inventario_queue]      [facturacion_queue]        [envios_queue]
            |                        |                         |
            v                        v                         v
+-----------------------+ +-----------------------+ +-----------------------+
|   inventory-service   | |    billing-service    | |   shipping-service    |
| (Descuenta de stock)  | |  (Emite la factura)   | | (Genera guía y flete) |
+-----------------------+ +-----------------------+ +-----------------------+
```

---

## 💻 2. Prerrequisitos

Para ejecutar este entorno reproducible, únicamente necesitas tener instalado en tu máquina:

* [Docker Desktop](https://www.docker.com/products/docker-desktop/) (en Windows o macOS) con el motor de Docker iniciado.
* O bien **Docker Engine** y el plugin **Docker Compose** (en distribuciones Linux).
* Git (para clonar y versionar el repositorio).

*(No es obligatorio tener Node.js instalado localmente, ya que cada microservicio corre dentro de su propio contenedor aislado).*

---

## 🚀 3. Paso a Paso para Levantar el Entorno

Desde la terminal, situarse en la raíz del proyecto y ejecutar:

### 3.1. Iniciar los contenedores en segundo plano
```bash
docker compose up --build -d
```
*(Si usas versiones anteriores de Docker Compose, puedes usar `docker-compose up --build -d`)*.

Este comando realizará automáticamente:
1. La descarga de la imagen oficial de RabbitMQ (`rabbitmq:3-management-alpine`).
2. El build de las imágenes Docker de cada microservicio en Node.js.
3. La creación de la red puente `eda-network`.
4. El arranque ordenado (los microservicios esperan a que RabbitMQ esté con estado `healthy` antes de iniciar).

### 3.2. Verificar el estado de los contenedores
```bash
docker compose ps
```
Deberás ver los 5 contenedores con estado **Up**:
* `rabbitmq-broker` (puertos `5672` y `15672`)
* `orders-api-producer` (puerto `3000`)
* `inventory-service-worker`
* `billing-service-worker`
* `shipping-service-worker`

---

## 🎬 4. Guía para la Demo en Vivo

### Paso 1: Abrir la terminal de logs en tiempo real
Para apreciar visualmente la concurrencia y el patrón Pub/Sub, abre una **segunda ventana de terminal** y ejecuta el siguiente comando para seguir los logs de los 3 microservicios consumidores a la vez:

```bash
docker compose logs -f inventory-service billing-service shipping-service
```

### Paso 2: Disparar la orden de compra de prueba (`POST /orders`)
Desde tu primera terminal (o herramientas como Postman / Thunder Client), dispara la petición HTTP:

#### En Windows (PowerShell):
```powershell
Invoke-RestMethod -Uri http://localhost:3000/orders -Method Post -ContentType "application/json" -Body (@{
    orderId = "ORD-UTN-2026"
    customer = @{
        name = "Estudiante UTN"
        email = "alumno@utn.edu.ar"
        address = "French 414, Resistencia, Chaco"
    }
    items = @(
        @{ sku = "NOTEBOOK-01"; name = "Laptop Dev 16GB"; quantity = 1; price = 1200.00 },
        @{ sku = "MOUSE-02"; name = "Mouse Inalámbrico"; quantity = 1; price = 25.50 }
    )
    total = 1225.50
} | ConvertTo-Json -Depth 5)
```

#### En Linux / macOS / Git Bash (cURL):
```bash
curl -X POST http://localhost:3000/orders \
  -H "Content-Type: application/json" \
  -d '{
    "orderId": "ORD-UTN-2026",
    "customer": {
      "name": "Estudiante UTN",
      "email": "alumno@utn.edu.ar",
      "address": "French 414, Resistencia, Chaco"
    },
    "items": [
      { "sku": "NOTEBOOK-01", "name": "Laptop Dev 16GB", "quantity": 1, "price": 1200.00 },
      { "sku": "MOUSE-02", "name": "Mouse Inalámbrico", "quantity": 1, "price": 25.50 }
    ],
    "total": 1225.50
  }'
```

### Paso 3: ¿Qué debe observar la audiencia?

1. **En la respuesta HTTP (Inmediata / Milisegundos):**
   `orders-api` responde inmediatamente `HTTP 201 Created` informando que el evento `PedidoCreado` fue emitido exitosamente. El cliente no tiene que esperar a que el inventario se actualice ni a que la factura se imprima.

2. **En la segunda terminal (Procesamiento Concurrente y Paralelo):**
   En el mismo instante verás cómo los tres servicios reciben el mensaje a la vez y trabajan de forma simultánea:
   ```text
   inventory-service-worker  | [Inventory Service] 📦 EVENTO RECIBIDO: PedidoCreado
   billing-service-worker    | [Billing Service] 📦 EVENTO RECIBIDO: PedidoCreado
   shipping-service-worker   | [Shipping Service] 📦 EVENTO RECIBIDO: PedidoCreado
   inventory-service-worker  | [Inventory Service] 🔄 Procesando reserva de inventario para Pedido ID: ORD-UTN-2026...
   billing-service-worker    | [Billing Service] 💳 Iniciando proceso de facturación para Pedido ID: ORD-UTN-2026...
   shipping-service-worker   | [Shipping Service] 🚚 Procesando orden de despacho para Pedido ID: ORD-UTN-2026...
   inventory-service-worker  | [Inventory Service] ✅ Stock descontado y reservado exitosamente para el Pedido: ORD-UTN-2026
   billing-service-worker    | [Billing Service] ✅ Cobro y Factura emitidos exitosamente para Pedido: ORD-UTN-2026
   shipping-service-worker   | [Shipping Service] ✅ Envío programado y guía emitida para Pedido: ORD-UTN-2026
   ```

---

## 📊 5. Herramienta Visual (RabbitMQ Management Dashboard)

RabbitMQ incluye una consola gráfica web que permite ilustrar con claridad a la cátedra y compañeros cómo viajan los mensajes:

* **URL:** [http://localhost:15672](http://localhost:15672)
* **Usuario:** `guest`
* **Contraseña:** `guest`

### ¿Qué mostrar durante la presentación?
1. **Pestaña `Exchanges`:** Localiza `pedidos_exchange`. Podrás mostrar que es de tipo **fanout** y revisar la sección *Bindings*, donde figuran conectadas las tres colas.
2. **Pestaña `Queues`:** Verás las tres colas activas (`inventario_queue`, `facturacion_queue`, `envios_queue`).
3. **Métricas en tiempo real:** Al enviar pedidos concurrentes, se aprecian las curvas de entrada de mensajes (`Publish`) y entrega a consumidores (`Deliver/Ack`).

---

## 🛑 6. Detener el Entorno

Para apagar los contenedores y liberar los recursos del sistema:

```bash
docker compose down
```

Si además deseas eliminar volúmenes asociados:
```bash
docker compose down -v
```
