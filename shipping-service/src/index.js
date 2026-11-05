const amqp = require('amqplib');

const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://guest:guest@localhost:5672';
const EXCHANGE_NAME = process.env.EXCHANGE_NAME || 'pedidos_exchange';
const QUEUE_NAME = process.env.QUEUE_NAME || 'envios_queue';

/**
 * Simulación de lógica de negocio: Creación de guía de despacho y asignación de courier
 */
async function processShipping(orderData) {
  const { orderId, customer, items } = orderData.payload;
  console.log(`[Shipping Service] 🚚 Procesando orden de despacho para Pedido ID: ${orderId}...`);

  const trackingNumber = `TRK-EXP-${Math.floor(10000000 + Math.random() * 90000000)}`;
  const destination = customer.address || 'Av. Providencia 1234, Santiago, Chile';
  const totalPackages = items.reduce((acc, curr) => acc + (curr.quantity || 1), 0);

  // Simulación de interacción con API de Courier externo (FedEx / DHL / BlueExpress)
  await new Promise((resolve) => setTimeout(resolve, 400));

  console.log(`   -> Guía de despacho generada : ${trackingNumber}`);
  console.log(`   -> Destinatario             : ${customer.name || customer}`);
  console.log(`   -> Destino                  : ${destination}`);
  console.log(`   -> Total bultos / paquetes  : ${totalPackages}`);
  console.log(`[Shipping Service] ✅ Envío programado y guía emitida para Pedido: ${orderId}\n`);
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
      console.log(`[Shipping Service] Conectando a RabbitMQ (${attempt}/${maxRetries})...`);

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

      console.log(`[Shipping Service] 🚀 Esperando eventos en cola '${QUEUE_NAME}'. Exchange: '${EXCHANGE_NAME}'`);

      // 4. Consumir mensajes
      channel.consume(
        QUEUE_NAME,
        async (msg) => {
          if (!msg) return;

          try {
            const rawContent = msg.content.toString();
            const event = JSON.parse(rawContent);

            console.log(`\n------------------------------------------------------`);
            console.log(`[Shipping Service] 📩 EVENTO RECIBIDO: ${event.eventType}`);
            console.log(`   Timestamp: ${event.timestamp}`);
            console.log(`   Evento ID: ${event.eventId}`);

            if (event.eventType === 'PedidoCreado') {
              await processShipping(event);
            } else {
              console.warn(`[Shipping Service] Evento desconocido ignorado: ${event.eventType}`);
            }

            // Confirmación manual del mensaje (ACK)
            channel.ack(msg);
          } catch (err) {
            console.error('[Shipping Service] Error procesando despacho:', err);
            channel.nack(msg, false, false);
          }
        },
        { noAck: false }
      );

      connection.on('error', (err) => {
        console.error('[Shipping Service] Error en conexión:', err.message);
      });

      connection.on('close', () => {
        console.warn('[Shipping Service] Conexión cerrada. Reintentando en 5s...');
        setTimeout(startWorker, 5000);
      });

      return;
    } catch (error) {
      console.error(`[Shipping Service] Error en intento (${attempt}/${maxRetries}):`, error.message);
      if (attempt < maxRetries) {
        await new Promise((resolve) => setTimeout(resolve, 3000));
      }
    }
  }
}

startWorker();
