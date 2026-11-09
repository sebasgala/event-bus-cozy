import { Injectable, Logger, OnApplicationBootstrap, OnModuleDestroy } from '@nestjs/common';
import { InjectDataSource } from '@nestjs/typeorm';
import { DataSource } from 'typeorm';
import { exigirEventoSinPii } from './lopdp';
import { RabbitMqService, SobreCloudEvent } from './rabbitmq.service';
import { DominioEvento, EXCHANGE_POR_DOMINIO } from './topologia-rabbitmq';

// Publicador único de event-bus (fase 9.3, Opción A + decisión D2):
//   1. Toma el candado de comun.feed_publicador (FOR UPDATE SKIP LOCKED).
//   2. Lee las filas pendientes ("sequence" IS NULL) de las cuatro outbox de dominio
//      (outbox_usuarios, outbox_catalogo, outbox_disponibilidad, outbox_reservas) en orden
//      estricto por ("registradoEn", "origen", "id").
//   3. Verifica que el evento no contenga PII (LOPDP §7.1).
//   4. Publica cada evento en el exchange topic de su dominio (cozy.<origen>) y espera
//      la confirmación de RabbitMQ (publisher confirm).
//   5. SOLO después de que RabbitMQ confirma la recepción, asigna "sequence" en la outbox
//      correspondiente (sin borrar nunca la fila), inserta el evento en comun.feed_eventos
//      y actualiza comun.feed_publicador.ultimo.

export const LOTE_PUBLICACION = 500;
const INTERVALO_MS = 200;

export const SQL_CANDADO = `SELECT "ultimo"::bigint AS "ultimo" FROM "comun"."feed_publicador" WHERE "id" = 1 FOR UPDATE SKIP LOCKED`;

const COLUMNAS = `"id", "eventoId", "tipo", "sujeto", "alojamientoId", "correlacionId", "datos", "creadoEn", "registradoEn"`;

// $1 = límite de lote, $2 = último sequence publicado leído con el candado
export const SQL_LEER_PENDIENTES = `
WITH pendientes AS (
  SELECT * FROM (
    SELECT 'usuarios'::text AS "origen", ${COLUMNAS} FROM "usuarios"."outbox_usuarios" WHERE "sequence" IS NULL
    UNION ALL SELECT 'catalogo'::text, ${COLUMNAS} FROM "catalogo"."outbox_catalogo" WHERE "sequence" IS NULL
    UNION ALL SELECT 'disponibilidad'::text, ${COLUMNAS} FROM "disponibilidad"."outbox_disponibilidad" WHERE "sequence" IS NULL
    UNION ALL SELECT 'reservas'::text, ${COLUMNAS} FROM "reservas"."outbox_reservas" WHERE "sequence" IS NULL
  ) u
  ORDER BY "registradoEn", "origen", "id"
  LIMIT $1
)
SELECT p.*, ($2::bigint + row_number() OVER (ORDER BY p."registradoEn", p."origen", p."id"))::bigint AS "nuevo"
FROM pendientes p
ORDER BY p."registradoEn", p."origen", p."id"
`;

export const SQL_INSERTAR_FEED = `
INSERT INTO "comun"."feed_eventos" ("sequence", "id", "tipo", "sujeto", "alojamientoId", "correlacionId", "datos", "creadoEn")
VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8)
`;

export const SQL_MARCAR_OUTBOX: Record<DominioEvento, string> = {
  usuarios: `UPDATE "usuarios"."outbox_usuarios" SET "sequence" = $1 WHERE "id" = $2`,
  catalogo: `UPDATE "catalogo"."outbox_catalogo" SET "sequence" = $1 WHERE "id" = $2`,
  disponibilidad: `UPDATE "disponibilidad"."outbox_disponibilidad" SET "sequence" = $1 WHERE "id" = $2`,
  reservas: `UPDATE "reservas"."outbox_reservas" SET "sequence" = $1 WHERE "id" = $2`,
};

export const SQL_ACTUALIZAR_CONTADOR = `
UPDATE "comun"."feed_publicador" SET "ultimo" = $1 WHERE "id" = 1
`;

export const SQL_PENDIENTES = `
SELECT ((SELECT count(*) FROM "usuarios"."outbox_usuarios" WHERE "sequence" IS NULL)
      + (SELECT count(*) FROM "catalogo"."outbox_catalogo" WHERE "sequence" IS NULL)
      + (SELECT count(*) FROM "disponibilidad"."outbox_disponibilidad" WHERE "sequence" IS NULL)
      + (SELECT count(*) FROM "reservas"."outbox_reservas" WHERE "sequence" IS NULL))::int AS "pendientes"
`;

export interface FilaOutboxPendiente {
  origen: DominioEvento;
  id: number | string;
  eventoId: string;
  tipo: string;
  sujeto: string;
  alojamientoId: number | null;
  correlacionId: string | null;
  datos: Record<string, unknown>;
  creadoEn: Date | string;
  registradoEn: Date | string;
  nuevo: string | number;
}

export function nodoCodigo(env: Record<string, string | undefined> = process.env): string {
  return (env.NODO_CODIGO ?? 'COZY').toUpperCase().replace(/[^A-Z0-9_]/g, '') || 'COZY';
}

// Fase 9.4: exactamente un publicador del feed corre a la vez. Sin un valor explícito el proceso no arranca.
export type PublicadorActivo = 'monolito' | 'event-bus';

export function resolverPublicadorActivo(env: Record<string, string | undefined> = process.env): PublicadorActivo {
  const valor = env.PUBLICADOR_ACTIVO;
  if (valor === 'monolito' || valor === 'event-bus') return valor;
  const recibido = valor === undefined ? 'sin definir' : JSON.stringify(valor);
  throw new Error(`PUBLICADOR_ACTIVO debe ser 'monolito' o 'event-bus' (recibido: ${recibido}). Solo uno de los dos publica el feed.`);
}

@Injectable()
export class PublicadorOutboxService implements OnApplicationBootstrap, OnModuleDestroy {
  private readonly log = new Logger('PublicadorOutbox');
  private temporizador?: NodeJS.Timeout;
  private enCurso: Promise<void> | null = null;

  constructor(
    @InjectDataSource() private readonly dataSource: DataSource,
    private readonly rabbit: RabbitMqService,
  ) {}

  onApplicationBootstrap(): void {
    if (resolverPublicadorActivo() !== 'event-bus') {
      this.log.log('Publicador de event-bus inactivo: PUBLICADOR_ACTIVO=monolito.');
      return;
    }
    this.temporizador = setInterval(() => void this.pasada(), INTERVALO_MS);
  }

  async onModuleDestroy(): Promise<void> {
    if (this.temporizador) clearInterval(this.temporizador);
    await this.enCurso;
  }

  // Ejecuta pasadas hasta vaciar las outbox pendientes; no solapa dos pasadas en el mismo proceso.
  private pasada(): Promise<void> {
    this.enCurso ??= (async () => {
      try {
        while ((await this.publicarLote()) === LOTE_PUBLICACION);
      } catch (e) {
        this.log.error(`No se pudo publicar lote en RabbitMQ/feed: ${(e as Error).message}`);
      } finally {
        this.enCurso = null;
      }
    })();
    return this.enCurso;
  }

  // Lee hasta `lote` eventos pendientes bajo el candado de comun.feed_publicador, los publica en orden
  // en RabbitMQ esperando publisher confirm, y solo marca sequence + escribe feed_eventos tras la confirmación.
  async publicarLote(lote = LOTE_PUBLICACION): Promise<number> {
    return this.dataSource.transaction(async (manager) => {
      const [candado] = await manager.query(SQL_CANDADO);
      if (!candado) return 0;

      const pendientes: FilaOutboxPendiente[] = await manager.query(SQL_LEER_PENDIENTES, [lote, candado.ultimo]);
      if (pendientes.length === 0) return 0;

      const fuente = `urn:${nodoCodigo().toLowerCase()}:alojamientos`;
      const confirmados: FilaOutboxPendiente[] = [];
      let errorPublicacion: Error | null = null;

      for (const fila of pendientes) {
        try {
          // Verificación LOPDP (§7.1): ningún evento puede contener datos personales (PII)
          exigirEventoSinPii(fila.datos);

          const sequence = Number(fila.nuevo);
          const sobre: SobreCloudEvent = {
            id: fila.eventoId,
            source: fuente,
            specversion: '1.0',
            type: fila.tipo,
            time: new Date(fila.creadoEn).toISOString(),
            sequence,
            subject: fila.sujeto,
            ...(fila.correlacionId ? { correlationid: fila.correlacionId } : {}),
            datacontenttype: 'application/json',
            data: fila.datos,
          };

          const exchange = EXCHANGE_POR_DOMINIO[fila.origen];
          await this.rabbit.publicarConConfirmacion(exchange, fila.tipo, sobre, {
            'x-origen': fila.origen,
            ...(fila.alojamientoId !== null && fila.alojamientoId !== undefined
              ? { 'x-alojamiento-id': fila.alojamientoId }
              : {}),
          });

          // Solo se agrega a confirmados DESPUÉS de que RabbitMQ devuelve el publisher confirm
          confirmados.push(fila);
        } catch (err) {
          errorPublicacion = err as Error;
          break;
        }
      }

      if (confirmados.length === 0 && errorPublicacion) {
        throw errorPublicacion;
      }

      // Persiste únicamente los eventos que RabbitMQ ya confirmó, en el mismo orden de sequence
      for (const fila of confirmados) {
        const sequence = Number(fila.nuevo);
        await manager.query(SQL_INSERTAR_FEED, [
          sequence,
          fila.eventoId,
          fila.tipo,
          fila.sujeto,
          fila.alojamientoId,
          fila.correlacionId,
          JSON.stringify(fila.datos),
          fila.creadoEn,
        ]);
        await manager.query(SQL_MARCAR_OUTBOX[fila.origen], [sequence, fila.id]);
      }

      const ultimoConfirmado = Number(confirmados[confirmados.length - 1].nuevo);
      await manager.query(SQL_ACTUALIZAR_CONTADOR, [ultimoConfirmado]);

      if (errorPublicacion) {
        this.log.warn(
          `Se confirmaron ${confirmados.length}/${pendientes.length} eventos antes de fallo en RabbitMQ (${errorPublicacion.message}); el resto se reintentará.`,
        );
      }

      return confirmados.length;
    });
  }

  // Cantidad de eventos aún sin sequence en las cuatro outbox
  async pendientes(): Promise<number> {
    const [fila] = await this.dataSource.query(SQL_PENDIENTES);
    return Number(fila.pendientes);
  }
}

