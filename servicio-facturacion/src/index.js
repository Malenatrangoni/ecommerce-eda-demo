const amqp = require('amqplib');

const URL_RABBITMQ = process.env.URL_RABBITMQ || 'amqp://guest:guest@localhost:5672';
const URL_API_PEDIDOS = process.env.URL_API_PEDIDOS || 'http://api-pedidos:3000';
const NOMBRE_INTERCAMBIADOR = process.env.NOMBRE_INTERCAMBIADOR || 'intercambiador_pedidos';
const NOMBRE_COLA = process.env.NOMBRE_COLA || 'cola_facturacion';
const INTERCAMBIADOR_DLQ = process.env.INTERCAMBIADOR_DLQ || 'intercambiador_dlq';
const COLA_DLQ = process.env.COLA_DLQ || 'cola_mensajes_muertos';

const TOPICOS_SUSCRITOS = ['pedido.creado', 'pedido.cancelado'];

const eventosProcesados = new Set();
const versionesPorPedido = new Map();
const facturasPorPedido = new Map();

let canalGlobal = null;

function emitirEventoDominio(topico, tipoEvento, eventoOrigen, datosExtra) {
  if (!canalGlobal) return;
  const nuevoEvento = {
    idEvento: `EVT-FAC-${Date.now()}-${Math.floor(100 + Math.random() * 900)}`,
    tipoEvento,
    productor: 'servicio-facturacion',
    eventoCausanteId: eventoOrigen.idEvento,
    topico,
    patronDatos: 'ECST (Evento de Dominio Coreografiado)',
    fechaHora: new Date().toISOString(),
    idPedido: eventoOrigen.idPedido,
    idCorrelacion: eventoOrigen.idCorrelacion,
    version: eventoOrigen.version || 1,
    datos: datosExtra
  };

  canalGlobal.publish(NOMBRE_INTERCAMBIADOR, topico, Buffer.from(JSON.stringify(nuevoEvento)), {
    persistent: true,
    contentType: 'application/json',
    messageId: nuevoEvento.idEvento,
    correlationId: nuevoEvento.idCorrelacion
  });

  console.log(`[Servicio de Facturación] 📢 [COREOGRAFÍA EDA] Emitido nuevo evento '${tipoEvento}' en tópico '${topico}'`);
  return nuevoEvento;
}

async function reportarActividad(estado, titulo, detalle, idPedido, idEvento, topico, eventoEmitido = null) {
  try {
    await fetch(`${URL_API_PEDIDOS}/actividad`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        servicio: 'servicio-facturacion',
        cola: NOMBRE_COLA,
        estado,
        titulo,
        detalle,
        idPedido,
        idEvento,
        topico,
        eventoEmitido,
        fechaHora: new Date().toISOString()
      })
    });
  } catch (_) {}
}

async function emitirFactura(evento) {
  const datos = evento.datos || evento.payload;
  const idPedido = datos.idPedido || datos.orderId;
  const cliente = datos.cliente || datos.customer;
  const total = Number(datos.total);

  if (datos.simularFallo || isNaN(total) || total <= 0) {
    throw new Error(`Monto total inválido ($${datos.total}) o fallo simulado al facturar el pedido '${idPedido}'.`);
  }

  const numeroFactura = `FAC-2026-${Math.floor(100000 + Math.random() * 900000)}`;
  const subtotal = Number((total / 1.21).toFixed(2));
  const iva = Number((total - subtotal).toFixed(2));
  const nombreCliente = cliente.nombre || cliente.name || cliente;

  await new Promise((resolver) => setTimeout(resolver, 350));

  facturasPorPedido.set(idPedido, { numeroFactura, subtotal, iva, total, cliente: nombreCliente, estado: 'EMITIDA' });

  // COREOGRAFÍA EDA: Facturación publica su propio evento de dominio "FacturaEmitida"
  const evEmitido = emitirEventoDominio('facturacion.emitida', 'FacturaEmitida', evento, {
    idPedido,
    numeroFactura,
    cliente: nombreCliente,
    subtotal,
    iva,
    total
  });

  await reportarActividad(
    'EXITO',
    `Factura ${numeroFactura} creada → Emitió FacturaEmitida`,
    `Subtotal: $${subtotal} + IVA 21%: $${iva} = Total: $${total}`,
    idPedido,
    evento.idEvento,
    'pedido.creado',
    evEmitido ? { tipoEvento: evEmitido.tipoEvento, topico: evEmitido.topico } : null
  );
}

async function emitirNotaDeCredito(evento) {
  const datos = evento.datos || evento.payload;
  const idPedido = datos.idPedido || datos.orderId;
  const motivo = datos.motivo || 'Cancelación de pedido';

  const facturaPrevia = facturasPorPedido.get(idPedido);
  const notaCredito = `NC-2026-${Math.floor(100000 + Math.random() * 900000)}`;

  await new Promise((resolver) => setTimeout(resolver, 280));

  if (facturaPrevia) facturaPrevia.estado = 'ANULADA';

  const evEmitido = emitirEventoDominio('facturacion.anulada', 'NotaCreditoEmitida', evento, {
    idPedido,
    notaCredito,
    facturaAnulada: facturaPrevia ? facturaPrevia.numeroFactura : 'S/D',
    motivo
  });

  await reportarActividad(
    'EXITO',
    `Nota de Crédito ${notaCredito} → Emitió NotaCreditoEmitida`,
    `Anula factura ${facturaPrevia ? facturaPrevia.numeroFactura : 'previa'} | Motivo: ${motivo}`,
    idPedido,
    evento.idEvento,
    'pedido.cancelado',
    evEmitido ? { tipoEvento: evEmitido.tipoEvento, topico: evEmitido.topico } : null
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
      canalGlobal = canal;

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

          let evento = {};
          try {
            evento = JSON.parse(mensaje.content.toString());
            const tipoEvento = evento.tipoEvento || evento.eventType;
            const idEvento = evento.idEvento || evento.eventId;
            const idPedido = evento.idPedido || (evento.datos && evento.datos.idPedido);
            const versionEntrante = evento.version || 1;
            const topico = mensaje.fields.routingKey;

            if (eventosProcesados.has(idEvento)) {
              await reportarActividad('IDEMPOTENCIA', 'Doble cobro evitado (Idempotencia)', `El evento ${idEvento} ya fue facturado previamente.`, idPedido, idEvento, topico);
              canal.ack(mensaje);
              return;
            }

            const versionActual = versionesPorPedido.get(idPedido) || 0;
            if (idPedido && versionEntrante <= versionActual) {
              await reportarActividad('IDEMPOTENCIA', 'Versión obsoleta descartada', `Recibida v${versionEntrante} <= v${versionActual} actual.`, idPedido, idEvento, topico);
              canal.ack(mensaje);
              return;
            }

            if (tipoEvento === 'PedidoCreado') {
              await emitirFactura(evento);
            } else if (tipoEvento === 'PedidoCancelado') {
              await emitirNotaDeCredito(evento);
            }

            eventosProcesados.add(idEvento);
            if (idPedido) versionesPorPedido.set(idPedido, versionEntrante);

            canal.ack(mensaje);
          } catch (error) {
            await reportarActividad('ERROR_DLQ', 'Rechazado (NACK) → Enviado a DLQ', error.message, evento.idPedido || 'S/D', evento.idEvento || 'S/D', mensaje.fields.routingKey);
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
