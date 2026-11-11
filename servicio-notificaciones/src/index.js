const amqp = require('amqplib');

const URL_RABBITMQ = process.env.URL_RABBITMQ || 'amqp://guest:guest@localhost:5672';
const URL_API_PEDIDOS = process.env.URL_API_PEDIDOS || 'http://api-pedidos:3000';
const NOMBRE_INTERCAMBIADOR = process.env.NOMBRE_INTERCAMBIADOR || 'intercambiador_pedidos';
const NOMBRE_COLA = process.env.NOMBRE_COLA || 'cola_notificaciones';
const INTERCAMBIADOR_DLQ = process.env.INTERCAMBIADOR_DLQ || 'intercambiador_dlq';
const COLA_DLQ = process.env.COLA_DLQ || 'cola_mensajes_muertos';

// Suscripción por Comodín Global ('#'): recibe los eventos de TODOS los dominios (pedido.*, inventario.*, facturacion.*, envio.*)
const TOPICOS_SUSCRITOS = ['#'];

const eventosProcesados = new Set();
const cadenaPorCorrelacion = new Map();

async function reportarActividad(estado, titulo, detalle, idPedido, idEvento, topico, cadenaCoreografia = []) {
  try {
    await fetch(`${URL_API_PEDIDOS}/actividad`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        servicio: 'servicio-notificaciones',
        cola: NOMBRE_COLA,
        estado,
        titulo,
        detalle,
        idPedido,
        idEvento,
        topico,
        cadenaCoreografia,
        fechaHora: new Date().toISOString()
      })
    });
  } catch (_) {}
}

/**
 * Audita y correlaciona toda la cadena coreografiada de eventos que ocurren en los distintos microservicios
 */
async function notificarYAuditar(evento, topico) {
  const datos = evento.datos || evento.payload || {};
  const idPedido = evento.idPedido || datos.idPedido || datos.orderId;
  const tipoEvento = evento.tipoEvento || evento.eventType;
  const idCorrelacion = evento.idCorrelacion || idPedido;

  const historial = cadenaPorCorrelacion.get(idCorrelacion) || [];
  if (!historial.includes(tipoEvento)) {
    historial.push(tipoEvento);
    cadenaPorCorrelacion.set(idCorrelacion, historial);
  }

  await new Promise((resolver) => setTimeout(resolver, 150));

  const textoCadena = historial.join(' → ');
  await reportarActividad(
    'EXITO',
    `Trazabilidad EDA (${historial.length} eventos correlacionados)`,
    `Cadena: ${textoCadena}`,
    idPedido,
    evento.idEvento,
    topico,
    historial
  );
}

async function iniciarConsumidor() {
  const maximosReintentos = 10;
  let intento = 0;

  while (intento < maximosReintentos) {
    try {
      intento++;
      const conexion = await amqp.connect(URL_RABBITMQ);
      const canal = await conexion.createChannel();

      await canal.assertExchange(INTERCAMBIADOR_DLQ, 'fanout', { durable: true });
      await canal.assertQueue(COLA_DLQ, { durable: true });
      await canal.bindQueue(COLA_DLQ, INTERCAMBIADOR_DLQ, '');

      await canal.assertExchange(NOMBRE_INTERCAMBIADOR, 'topic', { durable: true });

      await canal.assertQueue(NOMBRE_COLA, {
        durable: true,
        arguments: { 'x-dead-letter-exchange': INTERCAMBIADOR_DLQ }
      });

      for (const topico of TOPICOS_SUSCRITOS) {
        await canal.bindQueue(NOMBRE_COLA, NOMBRE_INTERCAMBIADOR, topico);
      }

      await canal.prefetch(1);

      canal.consume(
        NOMBRE_COLA,
        async (mensaje) => {
          if (!mensaje) return;

          try {
            const evento = JSON.parse(mensaje.content.toString());
            const idEvento = evento.idEvento || evento.eventId;
            const idPedido = evento.idPedido || (evento.datos && evento.datos.idPedido);
            const topico = mensaje.fields.routingKey;

            if (eventosProcesados.has(idEvento)) {
              await reportarActividad('IDEMPOTENCIA', 'Correo duplicado evitado (Idempotencia)', `El evento ${idEvento} ya fue auditado y notificado.`, idPedido, idEvento, topico);
              canal.ack(mensaje);
              return;
            }

            await notificarYAuditar(evento, topico);
            eventosProcesados.add(idEvento);

            canal.ack(mensaje);
          } catch (error) {
            canal.nack(mensaje, false, false);
          }
        },
        { noAck: false }
      );

      conexion.on('close', () => setTimeout(iniciarConsumidor, 5000));
      return;
    } catch (error) {
      if (intento < maximosReintentos) await new Promise((r) => setTimeout(r, 3000));
    }
  }
}

iniciarConsumidor();
