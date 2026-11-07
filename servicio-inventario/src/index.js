const amqp = require('amqplib');

const URL_RABBITMQ = process.env.URL_RABBITMQ || 'amqp://guest:guest@localhost:5672';
const URL_API_PEDIDOS = process.env.URL_API_PEDIDOS || 'http://api-pedidos:3000';
const NOMBRE_INTERCAMBIADOR = process.env.NOMBRE_INTERCAMBIADOR || 'intercambiador_pedidos';
const NOMBRE_COLA = process.env.NOMBRE_COLA || 'cola_inventario';
const INTERCAMBIADOR_DLQ = process.env.INTERCAMBIADOR_DLQ || 'intercambiador_dlq';
const COLA_DLQ = process.env.COLA_DLQ || 'cola_mensajes_muertos';

// Patrones de suscripción Pub/Sub: Inventario escucha creación y cancelación de pedidos
const TOPICOS_SUSCRITOS = ['pedido.creado', 'pedido.cancelado'];

// Estado Local del Dominio de Inventario (Proyección local en EDA)
const stockDisponible = new Map([
  ['NOTEBOOK-PRO', { codigo: 'NOTEBOOK-PRO', nombre: 'Notebook Desarrollo 16GB', stock: 15 }],
  ['MOUSE-INAL', { codigo: 'MOUSE-INAL', nombre: 'Mouse Inalámbrico', stock: 30 }],
  ['TECLADO-MEC', { codigo: 'TECLADO-MEC', nombre: 'Teclado Mecánico', stock: 20 }],
  ['MONITOR-24', { codigo: 'MONITOR-24', nombre: 'Monitor IPS 24 Pulgadas', stock: 10 }]
]);

// Control de Idempotencia y Versionado en memoria
const eventosProcesados = new Set();
const versionesPorPedido = new Map();
const reservasPorPedido = new Map();

let canalGlobal = null;

/**
 * En EDA (Coreografía), un consumidor también actúa como Productor emitiendo nuevos eventos de su propio dominio
 */
function emitirEventoDominio(topico, tipoEvento, eventoOrigen, datosExtra) {
  if (!canalGlobal) return;
  const nuevoEvento = {
    idEvento: `EVT-INV-${Date.now()}-${Math.floor(100 + Math.random() * 900)}`,
    tipoEvento,
    productor: 'servicio-inventario',
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

  console.log(`[Servicio de Inventario] 📢 [COREOGRAFÍA EDA] Emitido nuevo evento '${tipoEvento}' en tópico '${topico}'`);
  return nuevoEvento;
}

async function reportarActividad(estado, titulo, detalle, idPedido, idEvento, topico, eventoEmitido = null) {
  try {
    await fetch(`${URL_API_PEDIDOS}/actividad`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        servicio: 'servicio-inventario',
        cola: NOMBRE_COLA,
        estado,
        titulo,
        detalle,
        idPedido,
        idEvento,
        topico,
        eventoEmitido,
        proyeccionStock: Array.from(stockDisponible.values()),
        fechaHora: new Date().toISOString()
      })
    });
  } catch (_) {}
}

/**
 * Procesa PedidoCreado (ECST), actualiza su proyección de stock y emite el evento de dominio "StockReservado"
 */
async function reservarStock(evento) {
  const datos = evento.datos || evento.payload;
  const idPedido = datos.idPedido || datos.orderId;
  const articulos = datos.articulos || datos.items || [];

  if (datos.simularFallo || !Array.isArray(articulos) || articulos.length === 0) {
    throw new Error(`Fallo de validación en inventario para el pedido '${idPedido}' (artículos inválidos o fallo simulado).`);
  }

  console.log(`[Servicio de Inventario] 📦 [ECST - Autónomo] Procesando reserva de stock para Pedido ID: ${idPedido}...`);

  const resumenArticulos = [];
  for (const articulo of articulos) {
    const cantidad = articulo.cantidad || articulo.quantity || 1;
    const codigo = articulo.codigo || articulo.sku || articulo.nombre || articulo.name;
    if (cantidad <= 0) {
      throw new Error(`Cantidad inválida (${cantidad}) para el artículo [${codigo}].`);
    }

    const itemStock = stockDisponible.get(codigo) || { codigo, nombre: articulo.nombre || codigo, stock: 50 };
    const stockAnterior = itemStock.stock;
    itemStock.stock = Math.max(0, itemStock.stock - cantidad);
    stockDisponible.set(codigo, itemStock);

    resumenArticulos.push(`${codigo}: ${stockAnterior}→${itemStock.stock} u.`);
    console.log(`   -> Descontando ${cantidad} u. de [${codigo}] (Stock: ${stockAnterior} -> ${itemStock.stock})...`);
    await new Promise((resolver) => setTimeout(resolver, 250));
  }

  reservasPorPedido.set(idPedido, articulos);

  // COREOGRAFÍA EDA: Inventario emite su propio hecho de dominio "StockReservado"
  const evEmitido = emitirEventoDominio('inventario.reservado', 'StockReservado', evento, {
    idPedido,
    articulosReservados: articulos,
    resumenStock: resumenArticulos
  });

  console.log(`[Servicio de Inventario] ✅ Stock reservado y evento 'StockReservado' publicado para Pedido: ${idPedido}\n`);
  await reportarActividad(
    'EXITO',
    'Stock descontado → Emitió evento StockReservado',
    `Proyección local actualizada (${resumenArticulos.join(', ')})`,
    idPedido,
    evento.idEvento,
    'pedido.creado',
    evEmitido ? { tipoEvento: evEmitido.tipoEvento, topico: evEmitido.topico } : null
  );
}

/**
 * Procesa PedidoCancelado (Thin Event), repone stock y emite "StockRestituido"
 */
async function reponerStockPorCancelacion(evento) {
  const datos = evento.datos || evento.payload;
  const idPedido = datos.idPedido || datos.orderId;
  const motivo = datos.motivo || 'Sin motivo especificado';

  console.log(`[Servicio de Inventario] 🔄 [Evento Ligero / Thin] Procesando cancelación de Pedido ID: ${idPedido}...`);

  let articulos = reservasPorPedido.get(idPedido);
  let usoCallback = false;

  if (!articulos) {
    usoCallback = true;
    try {
      const respuesta = await fetch(`${URL_API_PEDIDOS}/pedidos/${idPedido}`);
      if (respuesta.ok) {
        const pedidoOrigen = await respuesta.json();
        articulos = pedidoOrigen.articulos || [];
      }
    } catch (_) {}
  }

  const resumen = [];
  for (const articulo of (articulos || [])) {
    const cantidad = articulo.cantidad || articulo.quantity || 1;
    const codigo = articulo.codigo || articulo.sku || articulo.nombre;
    const itemStock = stockDisponible.get(codigo) || { codigo, nombre: codigo, stock: 10 };
    const stockAnterior = itemStock.stock;
    itemStock.stock += cantidad;
    stockDisponible.set(codigo, itemStock);

    resumen.push(`${codigo}: ${stockAnterior}→${itemStock.stock} u.`);
    await new Promise((resolver) => setTimeout(resolver, 200));
  }

  reservasPorPedido.delete(idPedido);

  const evEmitido = emitirEventoDominio('inventario.restituido', 'StockRestituido', evento, {
    idPedido,
    motivo,
    resumenStock: resumen
  });

  await reportarActividad(
    'EXITO',
    `Stock repuesto (${usoCallback ? 'Callback GET' : 'Caché'}) → Emitió StockRestituido`,
    `Restituido (${resumen.join(', ') || 'sin cambios'})`,
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
      console.log(`[Servicio de Inventario] Conectando a RabbitMQ (${intento}/${maximosReintentos})...`);

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

      console.log(`[Servicio de Inventario] 🚀 Suscrito a [${TOPICOS_SUSCRITOS.join(', ')}] en cola '${NOMBRE_COLA}'`);

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
              console.warn(`[Servicio de Inventario] ⚠️ [IDEMPOTENCIA] Evento '${idEvento}' repetido ignorado.\n`);
              await reportarActividad('IDEMPOTENCIA', 'Evento duplicado ignorado (Idempotencia)', `El evento ${idEvento} ya fue aplicado; no se descuenta doble stock.`, idPedido, idEvento, topico);
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
              await reservarStock(evento);
            } else if (tipoEvento === 'PedidoCancelado') {
              await reponerStockPorCancelacion(evento);
            }

            eventosProcesados.add(idEvento);
            if (idPedido) versionesPorPedido.set(idPedido, versionEntrante);

            canal.ack(mensaje);
          } catch (error) {
            console.error(`[Servicio de Inventario] ❌ ERROR: ${error.message} -> NACK a DLQ`);
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
