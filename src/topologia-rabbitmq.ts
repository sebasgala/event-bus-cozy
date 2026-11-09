// Topología de RabbitMQ como código (fase 9.3, §4 de docs/integracion-v2/diseno-fase-9.md):
//   - Un exchange topic durable por dominio: cozy.usuarios, cozy.catalogo, cozy.disponibilidad, cozy.reservas.
//   - Un exchange de dead-letter (DLX): cozy.dlx.
//   - Colas durables por consumidor con su DLQ (<cola>.dlq) tras 5 intentos fallidos.

export const DOMINIOS_EVENTOS = ['usuarios', 'catalogo', 'disponibilidad', 'reservas'] as const;
export type DominioEvento = (typeof DOMINIOS_EVENTOS)[number];

export const EXCHANGE_POR_DOMINIO: Record<DominioEvento, string> = {
  usuarios: 'cozy.usuarios',
  catalogo: 'cozy.catalogo',
  disponibilidad: 'cozy.disponibilidad',
  reservas: 'cozy.reservas',
};

export const EXCHANGE_DLX = 'cozy.dlx';
export const MAX_INTENTOS_CONSUMIDOR = 5;

export interface DefinicionEnlace {
  exchange: string;
  patron: string;
}

export interface DefinicionCola {
  nombre: string;
  dlq: string;
  consumidor: string;
  enlaces: DefinicionEnlace[];
}

export const COLAS_EVENT_BUS: readonly DefinicionCola[] = [
  {
    nombre: 'disponibilidad.catalogo-estado',
    dlq: 'disponibilidad.catalogo-estado.dlq',
    consumidor: 'disponibilidad',
    enlaces: [
      { exchange: EXCHANGE_POR_DOMINIO.catalogo, patron: 'catalogo.#' },
      { exchange: EXCHANGE_POR_DOMINIO.catalogo, patron: 'alojamiento.#' },
    ],
  },
  {
    nombre: 'catalogo.reservas-resumen',
    dlq: 'catalogo.reservas-resumen.dlq',
    consumidor: 'catalogo',
    enlaces: [{ exchange: EXCHANGE_POR_DOMINIO.reservas, patron: 'alojamiento.reserva.*' }],
  },
  {
    nombre: 'auditoria.eventos-ejemplo',
    dlq: 'auditoria.eventos-ejemplo.dlq',
    consumidor: 'event-bus-ejemplo',
    enlaces: [
      { exchange: EXCHANGE_POR_DOMINIO.usuarios, patron: '#' },
      { exchange: EXCHANGE_POR_DOMINIO.catalogo, patron: '#' },
      { exchange: EXCHANGE_POR_DOMINIO.disponibilidad, patron: '#' },
      { exchange: EXCHANGE_POR_DOMINIO.reservas, patron: '#' },
    ],
  },
] as const;

export interface CanalTopologiaAmqp {
  assertExchange(exchange: string, tipo: string, opciones?: { durable?: boolean }): Promise<unknown>;
  assertQueue(
    cola: string,
    opciones?: {
      durable?: boolean;
      arguments?: Record<string, unknown>;
    },
  ): Promise<unknown>;
  bindQueue(cola: string, exchange: string, patron: string): Promise<unknown>;
}

// Declara de forma idempotente los exchanges por dominio, el DLX, las colas de consumidor y sus DLQ.
export async function declararTopologiaRabbitMq(canal: CanalTopologiaAmqp): Promise<void> {
  await canal.assertExchange(EXCHANGE_DLX, 'topic', { durable: true });

  for (const dominio of DOMINIOS_EVENTOS) {
    await canal.assertExchange(EXCHANGE_POR_DOMINIO[dominio], 'topic', { durable: true });
  }

  for (const cola of COLAS_EVENT_BUS) {
    // 1. Cola de dead-letter (DLQ) durable y enlazada al DLX
    await canal.assertQueue(cola.dlq, { durable: true });
    await canal.bindQueue(cola.dlq, EXCHANGE_DLX, cola.dlq);

    // 2. Cola principal durable con redirección al DLX cuando un mensaje se rechaza definitivamente
    await canal.assertQueue(cola.nombre, {
      durable: true,
      arguments: {
        'x-dead-letter-exchange': EXCHANGE_DLX,
        'x-dead-letter-routing-key': cola.dlq,
      },
    });

    for (const enlace of cola.enlaces) {
      await canal.bindQueue(cola.nombre, enlace.exchange, enlace.patron);
    }
  }
}

