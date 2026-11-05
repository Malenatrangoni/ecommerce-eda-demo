const amqp = require('amqplib');

const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://guest:guest@localhost:5672';
const EXCHANGE_NAME = process.env.EXCHANGE_NAME || 'pedidos_exchange';
const QUEUE_NAME = process.env.QUEUE_NAME || 'facturacion_queue';

/**
 * Simulación de lógica de negocio: Emisión de factura electrónica y cobro
 */
async function processBilling(orderData) {
  const { orderId, customer, total } = orderData.payload;
  console.log(`[Billing Service] 💳 Iniciando proceso de facturación para Pedido ID: ${orderId}...`);

  const invoiceNumber = `FAC-2026-${Math.floor(100000 + Math.random() * 900000)}`;
  const tax = Number((total * 0.19).toFixed(2));
  const subtotal = Number((total - tax).toFixed(2));

  // Simulación de procesamiento de pasarela de pago y generación de PDF fiscal
  await new Promise((resolve) => setTimeout(resolve, 500));

  console.log(`   -> Factura generada : ${invoiceNumber}`);
  console.log(`   -> Cliente emisor   : ${customer.name || customer}`);
  console.log(`   -> Subtotal: $${subtotal} | IVA: $${tax} | Total: $${total}`);
  console.log(`[Billing Service] ✅ Cobro y Factura emitidos exitosamente para Pedido: ${orderId}\n`);
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
      console.log(`[Billing Service] Conectando a RabbitMQ (${attempt}/${maxRetries})...`);

      const connection = await amqp.connect(RABBITMQ_URL);
      const channel = await connection.createChannel();

      // 1. Declarar exchange fanout y durable
      await channel.assertExchange(EXCHANGE_NAME, 'fanout', { durable: true });

      // 2. Declarar cola durable
      await channel.assertQueue(QUEUE_NAME, { durable: true });

      // 3. Vincular cola al exchange
      await channel.bindQueue(QUEUE_NAME, EXCHANGE_NAME, '');

      // Limitar a 1 mensaje a la vez (prefetch)
      await channel.prefetch(1);

      console.log(`[Billing Service] 🚀 Esperando eventos en cola '${QUEUE_NAME}'. Exchange: '${EXCHANGE_NAME}'`);

      // 4. Consumir mensajes
      channel.consume(
        QUEUE_NAME,
        async (msg) => {
          if (!msg) return;

          try {
            const rawContent = msg.content.toString();
            const event = JSON.parse(rawContent);

            console.log(`\n------------------------------------------------------`);
            console.log(`[Billing Service] 📩 EVENTO RECIBIDO: ${event.eventType}`);
            console.log(`   Timestamp: ${event.timestamp}`);
            console.log(`   Evento ID: ${event.eventId}`);

            if (event.eventType === 'PedidoCreado') {
              await processBilling(event);
            } else {
              console.warn(`[Billing Service] Evento desconocido ignorado: ${event.eventType}`);
            }

            // Confirmación manual del mensaje (ACK)
            channel.ack(msg);
          } catch (err) {
            console.error('[Billing Service] Error procesando factura:', err);
            channel.nack(msg, false, false);
          }
        },
        { noAck: false }
      );

      connection.on('error', (err) => {
        console.error('[Billing Service] Error en conexión:', err.message);
      });

      connection.on('close', () => {
        console.warn('[Billing Service] Conexión cerrada. Reintentando en 5s...');
        setTimeout(startWorker, 5000);
      });

      return;
    } catch (error) {
      console.error(`[Billing Service] Error en intento (${attempt}/${maxRetries}):`, error.message);
      if (attempt < maxRetries) {
        await new Promise((resolve) => setTimeout(resolve, 3000));
      }
    }
  }
}

startWorker();
