const path = require('path');
const express = require('express');
const amqp = require('amqplib');

const aplicacion = express();
aplicacion.use(express.json());
aplicacion.use(express.static(path.join(__dirname, 'public')));

const PUERTO = process.env.PUERTO || 3000;
const URL_RABBITMQ = process.env.URL_RABBITMQ || 'amqp://guest:guest@localhost:5672';
const URL_ADMIN_RABBITMQ = process.env.URL_ADMIN_RABBITMQ || 'http://rabbitmq:15672';
const NOMBRE_INTERCAMBIADOR = process.env.NOMBRE_INTERCAMBIADOR || 'intercambiador_pedidos';
const TIPO_INTERCAMBIADOR = 'topic';

let canal = null;

// Almacenamiento en memoria del productor y bitácora visual de eventos y reacciones
const pedidos = new Map();
const eventosPublicados = [];
const bitacoraActividad = [];

/**
 * Conexión a RabbitMQ con reintentos para resiliencia en el arranque
 */
async function conectarRabbitMQ() {
  const maximosReintentos = 10;
  let intento = 0;

  while (intento < maximosReintentos) {
    try {
      intento++;
      console.log(`[API de Pedidos] Intentando conectar a RabbitMQ (${intento}/${maximosReintentos})...`);

      const conexion = await amqp.connect(URL_RABBITMQ);
      canal = await conexion.createChannel();

      await canal.assertExchange(NOMBRE_INTERCAMBIADOR, TIPO_INTERCAMBIADOR, { durable: true });

      // Suscripción de retorno para capturar todos los eventos coreografiados emitidos por los otros microservicios
      const colaMonitor = await canal.assertQueue('', { exclusive: true });
      await canal.bindQueue(colaMonitor.queue, NOMBRE_INTERCAMBIADOR, '#');
      canal.consume(
        colaMonitor.queue,
        (msg) => {
          if (!msg) return;
          try {
            const ev = JSON.parse(msg.content.toString());
            if (!eventosPublicados.some((e) => e.idEvento === ev.idEvento)) {
              eventosPublicados.unshift(ev);
              if (eventosPublicados.length > 50) eventosPublicados.pop();
            }
            // Actualizar proyección del estado del pedido por Consistencia Eventual
            const ped = pedidos.get(ev.idPedido);
            if (ped && ev.productor && ev.productor !== 'api-pedidos') {
              ped.eventosDominio = ped.eventosDominio || [];
              if (!ped.eventosDominio.includes(ev.tipoEvento)) {
                ped.eventosDominio.push(ev.tipoEvento);
              }
              if (ev.tipoEvento === 'EnvioProgramado') ped.estado = 'COMPLETADO_Y_DESPACHADO';
              if (ev.tipoEvento === 'StockRestituido') ped.estado = 'CANCELADO_Y_REPUESTO';
            }
          } catch (_) {}
        },
        { noAck: true }
      );

      console.log(`[API de Pedidos] ✅ Conectado a RabbitMQ. Intercambiador '${NOMBRE_INTERCAMBIADOR}' (${TIPO_INTERCAMBIADOR}) listo.`);

      conexion.on('error', (err) => {
        console.error('[API de Pedidos] Error en la conexión RabbitMQ:', err.message);
      });

      conexion.on('close', () => {
        console.warn('[API de Pedidos] Conexión RabbitMQ cerrada. Reintentando reconectar...');
        canal = null;
        setTimeout(conectarRabbitMQ, 5000);
      });

      return;
    } catch (error) {
      console.error(`[API de Pedidos] Falló intento de conexión (${intento}/${maximosReintentos}):`, error.message);
      if (intento < maximosReintentos) {
        await new Promise((resolver) => setTimeout(resolver, 3000));
      } else {
        console.error('[API de Pedidos] No fue posible conectar a RabbitMQ tras múltiples intentos.');
      }
    }
  }
}

/**
 * Función auxiliar para construir el Sobre de Evento (EventEnvelope) y publicarlo en un tópico
 */
function publicarEvento({ topico, tipoEvento, patronDatos, idPedido, version, idEvento, idCorrelacion, datos }) {
  const sobreEvento = {
    idEvento: idEvento || `EVT-${Date.now()}-${Math.floor(100 + Math.random() * 900)}`,
    tipoEvento,
    productor: 'api-pedidos',
    topico,
    patronDatos,
    fechaHora: new Date().toISOString(),
    idPedido,
    idCorrelacion: idCorrelacion || `CORR-${idPedido}-${Date.now()}`,
    version,
    datos
  };

  const datosBuffer = Buffer.from(JSON.stringify(sobreEvento));
  const publicado = canal.publish(NOMBRE_INTERCAMBIADOR, topico, datosBuffer, {
    persistent: true,
    contentType: 'application/json',
    messageId: sobreEvento.idEvento,
    correlationId: sobreEvento.idCorrelacion
  });

  if (publicado) {
    eventosPublicados.unshift(sobreEvento);
    if (eventosPublicados.length > 40) eventosPublicados.pop();

    console.log(`\n======================================================`);
    console.log(`[API de Pedidos] 📢 EVENTO PUBLICADO EN BROKER`);
    console.log(`   Tipo Evento : ${sobreEvento.tipoEvento} (${sobreEvento.patronDatos})`);
    console.log(`   Tópico      : ${sobreEvento.topico}`);
    console.log(`   ID Evento   : ${sobreEvento.idEvento}`);
    console.log(`   ID Pedido   : ${sobreEvento.idPedido} (Versión: v${sobreEvento.version})`);
    console.log(`   Correlación : ${sobreEvento.idCorrelacion}`);
    console.log(`======================================================\n`);
  }

  return { publicado, sobreEvento };
}

/**
 * Endpoint interno donde los consumidores registran sus reacciones para mostrarlas en el Panel Web en español
 */
aplicacion.post('/actividad', (req, res) => {
  const registro = {
    id: `ACT-${Date.now()}-${Math.floor(Math.random() * 1000)}`,
    ...req.body
  };
  bitacoraActividad.unshift(registro);
  if (bitacoraActividad.length > 60) bitacoraActividad.pop();
  res.status(201).json({ ok: true });
});

/**
 * Endpoint para limpiar la bitácora visual desde el panel web
 */
aplicacion.post('/actividad/limpiar', (req, res) => {
  bitacoraActividad.length = 0;
  eventosPublicados.length = 0;
  res.json({ mensaje: 'Bitácora reiniciada' });
});

/**
 * Endpoint que consulta en vivo la API de RabbitMQ y devuelve todo traducido al español para el Panel Web
 */
aplicacion.get('/estado-sistema', async (req, res) => {
  let colasRabbit = [];
  let intercambiadoresRabbit = [];

  try {
    const cabeceras = {
      Authorization: 'Basic ' + Buffer.from('guest:guest').toString('base64')
    };
    const [respColas, respExchanges] = await Promise.all([
      fetch(`${URL_ADMIN_RABBITMQ}/api/queues/%2F`, { headers: cabeceras }),
      fetch(`${URL_ADMIN_RABBITMQ}/api/exchanges/%2F`, { headers: cabeceras })
    ]);

    if (respColas.ok) {
      const datosColas = await respColas.json();
      const metadatosColas = {
        cola_inventario: {
          nombreMostrar: 'Cola de Inventario',
          servicio: 'servicio-inventario',
          topicos: ['pedido.creado', 'pedido.cancelado'],
          rol: 'Reserva stock (ECST) y repone por cancelación (Thin)'
        },
        cola_facturacion: {
          nombreMostrar: 'Cola de Facturación',
          servicio: 'servicio-facturacion',
          topicos: ['pedido.creado', 'pedido.cancelado'],
          rol: 'Emite Factura IVA 21% y Notas de Crédito'
        },
        cola_envios: {
          nombreMostrar: 'Cola de Envíos (Selectiva)',
          servicio: 'servicio-envios',
          topicos: ['pedido.creado'],
          rol: 'Genera guía de despacho (ignora cancelaciones)'
        },
        cola_notificaciones: {
          nombreMostrar: 'Cola de Notificaciones (Comodín Global)',
          servicio: 'servicio-notificaciones',
          topicos: ['# (todos los dominios)'],
          rol: 'Correlaciona toda la cadena coreografiada de eventos EDA'
        },
        cola_mensajes_muertos: {
          nombreMostrar: 'Cola de Mensajes Muertos (DLQ)',
          servicio: 'Aislamiento de Fallos (DLQ)',
          topicos: ['Mensajes rechazados (NACK)'],
          rol: 'Almacena mensajes fallidos sin frenar el sistema',
          esDLQ: true
        }
      };

      colasRabbit = datosColas.map((c) => {
        const meta = metadatosColas[c.name] || {
          nombreMostrar: c.name,
          servicio: 'Consumidor',
          topicos: [],
          rol: 'Cola de mensajería'
        };
        return {
          nombreTecnico: c.name,
          nombreMostrar: meta.nombreMostrar,
          servicio: meta.servicio,
          topicos: meta.topicos,
          rol: meta.rol,
          esDLQ: Boolean(meta.esDLQ),
          durable: c.durable,
          consumidoresActivos: c.consumers || 0,
          mensajesListos: c.messages_ready || 0,
          mensajesSinConfirmar: c.messages_unacknowledged || 0,
          mensajesTotales: c.messages || 0,
          totalEntregados: (c.message_stats && c.message_stats.deliver_get) || 0,
          totalConfirmadosAck: (c.message_stats && c.message_stats.ack) || 0
        };
      });
    }

    if (respExchanges.ok) {
      const datosEx = await respExchanges.json();
      intercambiadoresRabbit = datosEx
        .filter((e) => e.name === NOMBRE_INTERCAMBIADOR || e.name === 'intercambiador_dlq')
        .map((e) => ({
          nombre: e.name,
          tipo: e.type,
          durable: e.durable
        }));
    }
  } catch (err) {
    console.warn('[API de Pedidos] No se pudo consultar métricas de RabbitMQ:', err.message);
  }

  res.json({
    conectadoARabbitMQ: canal !== null,
    intercambiadorPrincipal: NOMBRE_INTERCAMBIADOR,
    tipoIntercambiador: TIPO_INTERCAMBIADOR,
    intercambiadores: intercambiadoresRabbit,
    colas: colasRabbit,
    pedidos: Array.from(pedidos.values()).reverse(),
    eventosPublicados,
    bitacoraActividad
  });
});

aplicacion.get('/salud', (req, res) => {
  res.json({
    estado: 'OK',
    servicio: 'api-pedidos',
    conectadoARabbitMQ: canal !== null,
    totalPedidosRegistrados: pedidos.size
  });
});

aplicacion.get('/pedidos', (req, res) => {
  res.json({
    total: pedidos.size,
    pedidos: Array.from(pedidos.values())
  });
});

aplicacion.get('/pedidos/:idPedido', (req, res) => {
  const pedido = pedidos.get(req.params.idPedido);
  if (!pedido) {
    return res.status(404).json({ error: `Pedido '${req.params.idPedido}' no encontrado.` });
  }
  console.log(`[API de Pedidos] 🔍 Consulta HTTP GET síncrona recibida para Pedido: ${req.params.idPedido}`);
  res.json(pedido);
});

async function crearPedido(req, res) {
  try {
    const idPedido = req.body.idPedido || req.body.orderId;
    const cliente = req.body.cliente || req.body.customer;
    const articulos = req.body.articulos || req.body.items;
    const total = req.body.total;
    const simularFallo = Boolean(req.body.simularFallo);

    if (!idPedido || !cliente || !articulos || total === undefined) {
      return res.status(400).json({
        error: 'Datos incompletos. Se requieren: idPedido, cliente, articulos y total.'
      });
    }

    if (!canal) {
      return res.status(503).json({
        error: 'El servicio de mensajería (RabbitMQ) no está disponible en este momento.'
      });
    }

    const pedidoExistente = pedidos.get(idPedido);
    const version = req.body.version || (pedidoExistente ? pedidoExistente.version + 1 : 1);

    if (!simularFallo) {
      pedidos.set(idPedido, {
        idPedido,
        estado: 'CREADO',
        version,
        cliente,
        articulos,
        total,
        fechaCreacion: new Date().toISOString()
      });
    }

    const { publicado, sobreEvento } = publicarEvento({
      topico: 'pedido.creado',
      tipoEvento: 'PedidoCreado',
      patronDatos: 'ECST (Evento Completo / Fat)',
      idPedido,
      version,
      idEvento: req.body.idEvento,
      idCorrelacion: req.body.idCorrelacion,
      datos: {
        idPedido,
        estado: simularFallo ? 'CON_ERROR' : 'CREADO',
        cliente,
        articulos,
        total,
        simularFallo
      }
    });

    if (publicado) {
      return res.status(201).json({
        mensaje: 'Pedido recibido y evento PedidoCreado publicado exitosamente.',
        evento: sobreEvento
      });
    } else {
      return res.status(500).json({
        error: 'No se pudo publicar el evento en el búfer de RabbitMQ.'
      });
    }
  } catch (error) {
    console.error('[API de Pedidos] Error al procesar pedido:', error);
    return res.status(500).json({ error: 'Error interno al procesar el pedido.' });
  }
}

aplicacion.post('/pedidos/:idPedido/cancelar', async (req, res) => {
  try {
    const { idPedido } = req.params;
    const motivo = req.body.motivo || 'Cancelado a solicitud del cliente';

    if (!canal) {
      return res.status(503).json({
        error: 'El servicio de mensajería (RabbitMQ) no está disponible en este momento.'
      });
    }

    const pedidoExistente = pedidos.get(idPedido);
    const version = req.body.version || (pedidoExistente ? pedidoExistente.version + 1 : 2);

    if (pedidoExistente) {
      pedidoExistente.estado = 'CANCELADO';
      pedidoExistente.version = version;
      pedidoExistente.motivoCancelacion = motivo;
      pedidoExistente.fechaCancelacion = new Date().toISOString();
    } else {
      pedidos.set(idPedido, {
        idPedido,
        estado: 'CANCELADO',
        version,
        motivoCancelacion: motivo,
        fechaCancelacion: new Date().toISOString()
      });
    }

    const { publicado, sobreEvento } = publicarEvento({
      topico: 'pedido.cancelado',
      tipoEvento: 'PedidoCancelado',
      patronDatos: 'Event Notification (Evento Ligero / Thin)',
      idPedido,
      version,
      idEvento: req.body.idEvento,
      idCorrelacion: req.body.idCorrelacion,
      datos: {
        idPedido,
        estado: 'CANCELADO',
        motivo
      }
    });

    if (publicado) {
      return res.status(200).json({
        mensaje: 'Pedido cancelado y evento ligero PedidoCancelado publicado exitosamente.',
        evento: sobreEvento
      });
    } else {
      return res.status(500).json({ error: 'No se pudo publicar el evento de cancelación.' });
    }
  } catch (error) {
    console.error('[API de Pedidos] Error al cancelar pedido:', error);
    return res.status(500).json({ error: 'Error interno al cancelar el pedido.' });
  }
});

aplicacion.post('/pedidos', crearPedido);
aplicacion.post('/orders', crearPedido);

aplicacion.listen(PUERTO, async () => {
  console.log(`[API de Pedidos] Servidor Express y Panel Visual corriendo en http://localhost:${PUERTO}`);
  await conectarRabbitMQ();
});
