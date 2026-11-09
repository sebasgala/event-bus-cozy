import './registro-rutas';
import 'reflect-metadata';
import * as assert from 'assert';
import { CanalConfirmacionAmqp, ConexionAmqp, MensajeAmqp } from './cliente-amqp';
import { ConexionPorRolIncompleta, opcionesBaseDatosEventBus } from './conexiones';
import { ConsumidorEjemploService } from './consumidor-ejemplo.service';
import { HealthController } from './health.controller';
import { EventoConPiiException, exigirEventoSinPii, limpiarPii } from './lopdp';
import {
  FilaOutboxPendiente,
  PublicadorOutboxService,
  resolverPublicadorActivo,
  SQL_ACTUALIZAR_CONTADOR,
  SQL_CANDADO,
  SQL_INSERTAR_FEED,
  SQL_LEER_PENDIENTES,
  SQL_MARCAR_OUTBOX,
} from './publicador-outbox.service';
import { RabbitMqService } from './rabbitmq.service';
import {
  COLAS_EVENT_BUS,
  declararTopologiaRabbitMq,
  DOMINIOS_EVENTOS,
  EXCHANGE_DLX,
  EXCHANGE_POR_DOMINIO,
  MAX_INTENTOS_CONSUMIDOR,
} from './topologia-rabbitmq';

function crearCanalSimulado(opciones?: { fallarConfirmEn?: number }) {
  const exchanges: { exchange: string; tipo: string; durable?: boolean }[] = [];
  const colas: { cola: string; durable?: boolean; arguments?: Record<string, unknown> }[] = [];
  const enlaces: { cola: string; exchange: string; patron: string }[] = [];
  const publicados: { exchange: string; routingKey: string; body: Record<string, unknown>; options?: unknown }[] = [];
  const acks: string[] = [];
  const nacks: { id: string; requeue: boolean }[] = [];
  let contadorPublicaciones = 0;

  const canal: CanalConfirmacionAmqp = {
    async assertExchange(exchange, tipo, opts) {
      exchanges.push({ exchange, tipo, durable: opts?.durable });
      return {};
    },
    async assertQueue(cola, opts) {
      colas.push({ cola, durable: opts?.durable, arguments: opts?.arguments });
      return {};
    },
    async bindQueue(cola, exchange, patron) {
      enlaces.push({ cola, exchange, patron });
      return {};
    },
    async prefetch() {},
    publish(exchange, routingKey, content, options, cb) {
      contadorPublicaciones++;
      if (opciones?.fallarConfirmEn && contadorPublicaciones === opciones.fallarConfirmEn) {
        cb?.(new Error('NACK de RabbitMQ (publisher confirm rechazado)'));
        return false;
      }
      publicados.push({
        exchange,
        routingKey,
        body: JSON.parse(content.toString('utf8')),
        options,
      });
      cb?.(null);
      return true;
    },
    async waitForConfirms() {},
    async consume() {
      return { consumerTag: 'ctag-1' };
    },
    ack(msg) {
      acks.push(JSON.parse(msg.content.toString('utf8')).id);
    },
    nack(msg, _allUpTo, requeue = true) {
      let id = 'desconocido';
      try {
        id = JSON.parse(msg.content.toString('utf8')).id ?? 'sin-id';
      } catch {
        id = 'malformado';
      }
      nacks.push({ id, requeue });
    },
    async close() {},
  };

  const conexion: ConexionAmqp = {
    async createConfirmChannel() {
      return canal;
    },
    on() {
      return this;
    },
    async close() {},
  };

  return { canal, conexion, exchanges, colas, enlaces, publicados, acks, nacks };
}

async function ejecutarPruebasUnitarias(): Promise<void> {
  // 1. Topología de RabbitMQ: 4 exchanges por dominio + DLX + colas con DLQ
  {
    const sim = crearCanalSimulado();
    await declararTopologiaRabbitMq(sim.canal);

    for (const dominio of DOMINIOS_EVENTOS) {
      assert.ok(
        sim.exchanges.some((e) => e.exchange === EXCHANGE_POR_DOMINIO[dominio] && e.tipo === 'topic' && e.durable === true),
        `Falta exchange topic durable para ${dominio}`,
      );
    }
    assert.ok(
      sim.exchanges.some((e) => e.exchange === EXCHANGE_DLX && e.tipo === 'topic' && e.durable === true),
      'Falta exchange DLX (cozy.dlx)',
    );
    for (const def of COLAS_EVENT_BUS) {
      const principal = sim.colas.find((c) => c.cola === def.nombre);
      const dlq = sim.colas.find((c) => c.cola === def.dlq);
      assert.ok(principal && principal.durable, `Falta cola durable ${def.nombre}`);
      assert.ok(dlq && dlq.durable, `Falta DLQ durable ${def.dlq}`);
      assert.strictEqual(principal.arguments?.['x-dead-letter-exchange'], EXCHANGE_DLX);
      assert.strictEqual(principal.arguments?.['x-dead-letter-routing-key'], def.dlq);
    }
  }

  // 2. Publicador outbox (Opción A): publica en orden, espera confirm, asigna sequence e inserta en comun.feed_eventos
  {
    const sim = crearCanalSimulado();
    const rabbit = new RabbitMqService(async () => sim.conexion);

    const estadoOutbox: FilaOutboxPendiente[] = [
      {
        origen: 'disponibilidad',
        id: 10,
        eventoId: 'evt-1',
        tipo: 'alojamiento.stock.actualizado',
        sujeto: 'alojamientos/HTL-COZY-UIO-001/habitaciones/SUITE',
        alojamientoId: 1,
        correlacionId: 'corr-1',
        datos: { alojamientoId: 'HTL-COZY-UIO-001', tipoHabitacionCodigo: 'SUITE', fecha: '2026-12-01', unidadesDisponibles: 4 },
        creadoEn: new Date('2026-11-08T12:00:00.000Z'),
        registradoEn: new Date('2026-11-08T12:00:00.100Z'),
        nuevo: 1,
      },
      {
        origen: 'reservas',
        id: 25,
        eventoId: 'evt-2',
        tipo: 'alojamiento.reserva.confirmada',
        sujeto: 'orders/ord-100',
        alojamientoId: 1,
        correlacionId: 'corr-2',
        datos: { orderId: 'ord-100', external_order_id: 'ORD-HTL-ABC', estado: 'CONFIRMED', alojamientoId: 'HTL-COZY-UIO-001' },
        creadoEn: new Date('2026-11-08T12:00:01.000Z'),
        registradoEn: new Date('2026-11-08T12:00:01.100Z'),
        nuevo: 2,
      },
    ];

    const sentenciasEjecutadas: { sql: string; params?: unknown[] }[] = [];
    let ultimoSecuencia = 0;
    const feedInsertado: unknown[][] = [];
    const marcadosPorDominio: Record<string, { id: number | string; sequence: number }[]> = {
      usuarios: [],
      catalogo: [],
      disponibilidad: [],
      reservas: [],
    };

    const dataSourceMock = {
      async transaction<T>(cb: (manager: { query: (sql: string, params?: unknown[]) => Promise<unknown> }) => Promise<T>): Promise<T> {
        return cb({
          async query(sql: string, params?: unknown[]) {
            sentenciasEjecutadas.push({ sql, params });
            if (sql === SQL_CANDADO) return [{ ultimo: String(ultimoSecuencia) }];
            if (sql === SQL_LEER_PENDIENTES) return estadoOutbox;
            if (sql === SQL_INSERTAR_FEED) {
              feedInsertado.push(params ?? []);
              return [];
            }
            for (const dom of DOMINIOS_EVENTOS) {
              if (sql === SQL_MARCAR_OUTBOX[dom]) {
                marcadosPorDominio[dom].push({ sequence: Number(params?.[0]), id: params?.[1] as number });
                return [];
              }
            }
            if (sql === SQL_ACTUALIZAR_CONTADOR) {
              ultimoSecuencia = Number(params?.[0]);
              return [];
            }
            return [];
          },
        });
      },
    };

    const publicador = new PublicadorOutboxService(dataSourceMock as never, rabbit);
    const publicados = await publicador.publicarLote(500);

    assert.strictEqual(publicados, 2);
    assert.strictEqual(sim.publicados.length, 2);
    assert.strictEqual(sim.publicados[0].exchange, 'cozy.disponibilidad');
    assert.strictEqual(sim.publicados[0].routingKey, 'alojamiento.stock.actualizado');
    assert.strictEqual(sim.publicados[0].body.sequence, 1);
    assert.strictEqual(sim.publicados[1].exchange, 'cozy.reservas');
    assert.strictEqual(sim.publicados[1].routingKey, 'alojamiento.reserva.confirmada');
    assert.strictEqual(sim.publicados[1].body.sequence, 2);

    assert.strictEqual(feedInsertado.length, 2);
    assert.deepStrictEqual(marcadosPorDominio.disponibilidad, [{ sequence: 1, id: 10 }]);
    assert.deepStrictEqual(marcadosPorDominio.reservas, [{ sequence: 2, id: 25 }]);
    assert.strictEqual(ultimoSecuencia, 2);
    // Verifica que nunca se ejecuta DELETE sobre ninguna outbox
    assert.ok(sentenciasEjecutadas.every((s) => !/\bDELETE\b/i.test(s.sql)));
  }

  // 3. Si RabbitMQ rechaza el publisher confirm, NO se marca sequence ni se inserta en feed_eventos (se reintentará)
  {
    const simFallo = crearCanalSimulado({ fallarConfirmEn: 1 });
    const rabbitFallo = new RabbitMqService(async () => simFallo.conexion);
    const escrituras: string[] = [];

    const dataSourceMock = {
      async transaction<T>(cb: (manager: { query: (sql: string, params?: unknown[]) => Promise<unknown> }) => Promise<T>): Promise<T> {
        return cb({
          async query(sql: string) {
            if (sql === SQL_CANDADO) return [{ ultimo: '0' }];
            if (sql === SQL_LEER_PENDIENTES) {
              return [
                {
                  origen: 'catalogo',
                  id: 1,
                  eventoId: 'evt-fallo',
                  tipo: 'catalogo.alojamiento.actualizado',
                  sujeto: 'alojamientos/1',
                  alojamientoId: 1,
                  correlacionId: null,
                  datos: { alojamientoId: 'HTL-COZY-UIO-001', activo: true },
                  creadoEn: new Date(),
                  registradoEn: new Date(),
                  nuevo: 1,
                },
              ];
            }
            escrituras.push(sql);
            return [];
          },
        });
      },
    };

    const publicador = new PublicadorOutboxService(dataSourceMock as never, rabbitFallo);
    await assert.rejects(() => publicador.publicarLote(500), /NACK de RabbitMQ/);
    assert.strictEqual(escrituras.length, 0, 'No debe marcar sequence ni escribir en feed_eventos si falla publisher confirm');
  }

  // 4. Cumplimiento LOPDP (§7.1): los eventos no llevan PII
  {
    exigirEventoSinPii({ orderId: '123', alojamientoId: 'HTL-COZY-UIO-001', total: { amount: '100.00', currency: 'USD' } });
    assert.throws(
      () => exigirEventoSinPii({ orderId: '123', email: 'cliente@example.com' }),
      (err: unknown) => err instanceof EventoConPiiException && err.campo === 'data.email',
    );
    const saneado = limpiarPii({ orderId: '123', nombre: 'Juan', nested: { telefono: '0999999999', monto: 50 } });
    assert.deepStrictEqual(saneado, { orderId: '123', nested: { monto: 50 } });
  }

  // 5. Consumidor de ejemplo: deduplica por id del evento y envía a DLQ tras 5 fallos
  {
    const sim = crearCanalSimulado();
    const rabbit = new RabbitMqService(async () => sim.conexion);
    const consumidor = new ConsumidorEjemploService(rabbit);

    const crearMensaje = (id: string, sequence = 1): MensajeAmqp => ({
      content: Buffer.from(
        JSON.stringify({
          id,
          source: 'urn:cozy:alojamientos',
          specversion: '1.0',
          type: 'alojamiento.stock.actualizado',
          time: '2026-11-08T12:00:00.000Z',
          sequence,
          subject: 'alojamientos/1',
          datacontenttype: 'application/json',
          data: { alojamientoId: 'HTL-COZY-UIO-001', unidadesDisponibles: 3 },
        }),
        'utf8',
      ),
      fields: { deliveryTag: 1, redelivered: false, exchange: 'cozy.disponibilidad', routingKey: 'alojamiento.stock.actualizado' },
      properties: { headers: {} },
    });

    let ejecucionesNegocio = 0;
    const msg1 = crearMensaje('evt-unico-1', 1);
    const r1 = await consumidor.procesarMensaje(msg1, sim.canal, () => {
      ejecucionesNegocio++;
    });
    const r2 = await consumidor.procesarMensaje(msg1, sim.canal, () => {
      ejecucionesNegocio++;
    });

    assert.strictEqual(r1, 'procesado');
    assert.strictEqual(r2, 'duplicado');
    assert.strictEqual(ejecucionesNegocio, 1, 'El manejador de negocio solo debe ejecutarse una vez por id de evento');
    assert.strictEqual(consumidor.fueProcesado('evt-unico-1'), true);
    assert.strictEqual(consumidor.obtenerEstadisticas().duplicadosIgnorados, 1);

    // Simula un evento que falla MAX_INTENTOS_CONSUMIDOR (5) veces y en el 5to intento va a DLQ (requeue=false)
    const msgFallo = crearMensaje('evt-venenoso', 2);
    for (let i = 1; i < MAX_INTENTOS_CONSUMIDOR; i++) {
      const res = await consumidor.procesarMensaje(msgFallo, sim.canal, () => {
        throw new Error('Error transitorio');
      });
      assert.strictEqual(res, 'reintentar');
    }
    const resFinal = await consumidor.procesarMensaje(msgFallo, sim.canal, () => {
      throw new Error('Error definitivo');
    });
    assert.strictEqual(resFinal, 'dlq');
    assert.strictEqual(sim.nacks[sim.nacks.length - 1].requeue, false, 'En el 5to fallo debe hacer nack con requeue=false hacia DLQ');
  }

  // 6. HealthController + rol comun en conexiones (el feed de Booking vive solo en el backend)
  {
    const sim = crearCanalSimulado();
    const rabbit = new RabbitMqService(async () => sim.conexion);
    const health = new HealthController(rabbit, { query: async () => [{ '?column?': 1 }] } as never);
    const estado = await health.check();
    assert.strictEqual(estado.status, 'ok');
    assert.strictEqual(estado.modulo, 'event-bus');
    assert.strictEqual(estado.rabbitmq, 'up');
    assert.strictEqual(estado.database, 'up');

    // Conexión por rol comun
    const opts = opcionesBaseDatosEventBus({ DB_HOST: 'localhost', DB_NAME: 'cozy_v2', DB_CLAVE_COMUN: 'secreto-comun' });
    assert.strictEqual(opts.username, 'comun');
    assert.throws(
      () => opcionesBaseDatosEventBus({ DB_CLAVE_USUARIOS: 'x' }),
      (err: unknown) => err instanceof ConexionPorRolIncompleta,
    );
  }

  // 7. PUBLICADOR_ACTIVO (fase 9.4): solo valores explícitos; cualquier otro hace fallar el arranque
  {
    for (const valor of ['monolito', 'event-bus']) {
      assert.equal(resolverPublicadorActivo({ PUBLICADOR_ACTIVO: valor }), valor);
    }
    for (const valor of [undefined, '', 'true', '1', 'false', '0', 'no', 'ambos', 'Monolito', 'event_bus', 'eventbus']) {
      assert.throws(() => resolverPublicadorActivo({ PUBLICADOR_ACTIVO: valor }), /PUBLICADOR_ACTIVO debe ser 'monolito' o 'event-bus'/);
    }

    const anterior = process.env.PUBLICADOR_ACTIVO;
    try {
      process.env.PUBLICADOR_ACTIVO = 'monolito';
      const inactivo = new PublicadorOutboxService({} as never, {} as never);
      inactivo.onApplicationBootstrap();
      assert.equal((inactivo as unknown as { temporizador?: unknown }).temporizador, undefined);

      process.env.PUBLICADOR_ACTIVO = 'true';
      const invalido = new PublicadorOutboxService({} as never, {} as never);
      assert.throws(() => invalido.onApplicationBootstrap(), /PUBLICADOR_ACTIVO debe ser/);
    } finally {
      if (anterior === undefined) delete process.env.PUBLICADOR_ACTIVO;
      else process.env.PUBLICADOR_ACTIVO = anterior;
    }
  }

  console.log('OK: todas las pruebas unitarias de event-bus (9.3) pasaron.');
}

void ejecutarPruebasUnitarias();
