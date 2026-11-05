const amqp = require('amqplib');

const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://guest:guest@localhost:5672';
const EXCHANGE_NAME = process.env.EXCHANGE_NAME || 'pedidos_exchange';
const QUEUE_NAME = process.env.QUEUE_NAME || 'inventario_queue';

/**
 * Simulación de lógica de negocio: Reserva y descuento de inventario
 */
async function processInventory(orderData) {
  const { orderId, items } = orderData.payload;
  console.log(`[Inventory Service] 📦 Procesando reserva de inventario para Pedido ID: ${orderId}...`);

  for (const item of items) {
    console.log(`   -> Descontando ${item.quantity || 1} unidad(es) de SKU [${item.sku || item.name}]...`);
    // Simulación de latencia de BD
    await new Promise((resolve) => setTimeout(resolve, 300));
  }

  console.log(`[Inventory Service] ✅ Stock descontado y reservado exitosamente para el Pedido: ${orderId}\n`);
}

/**
 * Conexión a RabbitMQ y suscripción a la cola
 */
async function startWorker() {
  const maxRetries = 10;
  let attempt = 0;

  while (attempt < maxRetries) {
    try {
      attempt++;
      console.log(`[Inventory Service] Conectando a RabbitMQ (${attempt}/${maxRetries})...`);

      const connection = await amqp.connect(RABBITMQ_URL);
      const channel = await connection.createChannel();

      // 1. Asegurar que el exchange exista (fanout y durable)
      await channel.assertExchange(EXCHANGE_NAME, 'fanout', { durable: true });

      // 2. Declarar la cola como durable para persistencia
      await channel.assertQueue(QUEUE_NAME, { durable: true });

      // 3. Vincular (bind) la cola al exchange
      await channel.bindQueue(QUEUE_NAME, EXCHANGE_NAME, '');

      // Limitar a 1 mensaje a la vez para balanceo justo (prefetch)
      await channel.prefetch(1);

      console.log(`[Inventory Service] 🚀 Esperando eventos en cola '${QUEUE_NAME}'. Exchange: '${EXCHANGE_NAME}'`);

      // 4. Consumir mensajes
      channel.consume(
        QUEUE_NAME,
        async (msg) => {
          if (!msg) return;

          try {
            const rawContent = msg.content.toString();
            const event = JSON.parse(rawContent);

            console.log(`\n------------------------------------------------------`);
            console.log(`[Inventory Service] 📩 EVENTO RECIBIDO: ${event.eventType}`);
            console.log(`   Timestamp: ${event.timestamp}`);
            console.log(`   Evento ID: ${event.eventId}`);

            if (event.eventType === 'PedidoCreado') {
              await processInventory(event);
            } else {
              console.warn(`[Inventory Service] Evento desconocido ignorado: ${event.eventType}`);
            }

            // Confirmación manual del mensaje (ACK)
            channel.ack(msg);
          } catch (err) {
            console.error('[Inventory Service] Error procesando mensaje:', err);
            // En caso de error crítico sin recuperación se rechaza sin reencolar,
            // o se reencola según la política deseada (en este caso nack sin reencolar)
            channel.nack(msg, false, false);
          }
        },
        { noAck: false } // Modo manual ACK garantizado
      );

      connection.on('error', (err) => {
        console.error('[Inventory Service] Error en conexión:', err.message);
      });

      connection.on('close', () => {
        console.warn('[Inventory Service] Conexión cerrada. Reintentando en 5s...');
        setTimeout(startWorker, 5000);
      });

      return;
    } catch (error) {
      console.error(`[Inventory Service] Error en intento (${attempt}/${maxRetries}):`, error.message);
      if (attempt < maxRetries) {
        await new Promise((resolve) => setTimeout(resolve, 3000));
      }
    }
  }
}

startWorker();
