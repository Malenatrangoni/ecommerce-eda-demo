const express = require('express');
const amqp = require('amqplib');

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 3000;
const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://guest:guest@localhost:5672';
const EXCHANGE_NAME = process.env.EXCHANGE_NAME || 'pedidos_exchange';

let channel = null;

/**
 * Conexión a RabbitMQ con reintentos para resiliencia en el arranque
 */
async function connectRabbitMQ() {
  const maxRetries = 10;
  let attempt = 0;

  while (attempt < maxRetries) {
    try {
      attempt++;
      console.log(`[Orders API] Intentando conectar a RabbitMQ (${attempt}/${maxRetries})...`);
      
      const connection = await amqp.connect(RABBITMQ_URL);
      channel = await connection.createChannel();

      // Declaramos el exchange de tipo 'fanout' y durable para Pub/Sub
      await channel.assertExchange(EXCHANGE_NAME, 'fanout', { durable: true });
      
      console.log(`[Orders API] Conectado exitosamente a RabbitMQ. Exchange '${EXCHANGE_NAME}' listo.`);
      
      connection.on('error', (err) => {
        console.error('[Orders API] Error en la conexión RabbitMQ:', err.message);
      });

      connection.on('close', () => {
        console.warn('[Orders API] Conexión RabbitMQ cerrada. Reintentando reconectar...');
        setTimeout(connectRabbitMQ, 5000);
      });

      return;
    } catch (error) {
      console.error(`[Orders API] Falló intento de conexión (${attempt}/${maxRetries}):`, error.message);
      if (attempt < maxRetries) {
        await new Promise((resolve) => setTimeout(resolve, 3000));
      } else {
        console.error('[Orders API] No fue posible conectar a RabbitMQ tras múltiples intentos.');
      }
    }
  }
}

/**
 * Health check
 */
app.get('/health', (req, res) => {
  res.json({
    status: 'OK',
    service: 'orders-api',
    rabbitConnected: channel !== null
  });
});

/**
 * Endpoint POST /orders
 * Recibe los datos de la orden y publica el evento "PedidoCreado" en el Fanout Exchange
 */
app.post('/orders', async (req, res) => {
  try {
    const { orderId, customer, items, total } = req.body;

    if (!orderId || !customer || !items || !total) {
      return res.status(400).json({
        error: 'Datos incompletos. Se requieren: orderId, customer, items y total.'
      });
    }

    if (!channel) {
      return res.status(503).json({
        error: 'El servicio de mensajería (RabbitMQ) no está disponible en este momento.'
      });
    }

    // Estructura del evento de negocio (EDA)
    const event = {
      eventType: 'PedidoCreado',
      eventId: `EVT-${Date.now()}-${Math.floor(Math.random() * 1000)}`,
      timestamp: new Date().toISOString(),
      payload: {
        orderId,
        customer,
        items,
        total
      }
    };

    // Publicamos en el exchange tipo fanout (routingKey queda vacío '' ya que fanout difunde a todas las colas unidas)
    const bufferData = Buffer.from(JSON.stringify(event));
    const published = channel.publish(EXCHANGE_NAME, '', bufferData, {
      persistent: true, // Persistencia del mensaje en disco
      contentType: 'application/json'
    });

    if (published) {
      console.log(`\n======================================================`);
      console.log(`[Orders API]  EVENTO PUBLICADO -> PedidoCreado`);
      console.log(`   ID Evento : ${event.eventId}`);
      console.log(`   ID Pedido : ${orderId}`);
      console.log(`   Cliente   : ${customer.name || customer}`);
      console.log(`   Total     : $${total}`);
      console.log(`======================================================\n`);

      return res.status(201).json({
        message: 'Pedido recibido y evento PedidoCreado publicado exitosamente.',
        event
      });
    } else {
      return res.status(500).json({
        error: 'No se pudo publicar el evento en el búfer de RabbitMQ.'
      });
    }
  } catch (error) {
    console.error('[Orders API] Error al procesar pedido:', error);
    return res.status(500).json({ error: 'Error interno al procesar el pedido.' });
  }
});

// Inicialización del servidor
app.listen(PORT, async () => {
  console.log(`[Orders API] Servidor Express corriendo en el puerto ${PORT}`);
  await connectRabbitMQ();
});
