import { Injectable, Logger, OnApplicationBootstrap } from '@nestjs/common';
import { CanalConfirmacionAmqp, MensajeAmqp } from './cliente-amqp';
import { exigirEventoSinPii } from './lopdp';
import { RabbitMqService, SobreCloudEvent } from './rabbitmq.service';
import { MAX_INTENTOS_CONSUMIDOR } from './topologia-rabbitmq';

export const COLA_CONSUMIDOR_EJEMPLO = 'auditoria.eventos-ejemplo';

export interface ResumenEventoConsumido {
  id: string;
  type: string;
  sequence: number;
  subject: string;
  correlationid?: string;
  procesadoEn: string;
}

// EJEMPLO, NO ES UN CONSUMIDOR REAL: muestra el patrón que seguirán los consumidores de cada servicio (deduplicar por id,
// reintentar y mandar a la DLQ tras 5 fallos). No ejecuta lógica de negocio; guarda su estado en memoria, así que se pierde al
// reiniciar. Los consumidores reales (disponibilidad, catálogo) irán en su propio repo y deduplicarán en su esquema.
// Consumidor de ejemplo en event-bus (fase 9.3, §4 del diseño):
//   - Escucha la cola `auditoria.eventos-ejemplo`.
//   - Deduplica de forma idempotente por `evento.id`: si recibe el mismo `id` otra vez,
//     lo ignora sin volver a ejecutar la lógica de negocio y responde ACK.
//   - Verifica cumplimiento LOPDP (sin PII en `data`).
//   - Si el procesamiento falla 5 veces (MAX_INTENTOS_CONSUMIDOR), rechaza sin reencolar
//     (nack con requeue=false) para que RabbitMQ lo envíe a `<cola>.dlq` vía `cozy.dlx`.
@Injectable()
export class ConsumidorEjemploService implements OnApplicationBootstrap {
  private readonly log = new Logger('ConsumidorEjemplo');
  private readonly idsProcesados = new Set<string>();
  private readonly intentosPorMensaje = new Map<string, number>();
  private readonly historial: ResumenEventoConsumido[] = [];
  private duplicados = 0;
  private enviadosADlq = 0;

  constructor(private readonly rabbit: RabbitMqService) {}

  onApplicationBootstrap(): void {
    void this.rabbit.registrarConsumidor(COLA_CONSUMIDOR_EJEMPLO, async (msg, canal) => {
      await this.procesarMensaje(msg, canal);
    });
  }

  async procesarMensaje(
    msg: MensajeAmqp,
    canal: Pick<CanalConfirmacionAmqp, 'ack' | 'nack'>,
    manejadorNegocio?: (evento: SobreCloudEvent) => Promise<void> | void,
  ): Promise<'procesado' | 'duplicado' | 'reintentar' | 'dlq'> {
    let evento: SobreCloudEvent;
    try {
      evento = JSON.parse(msg.content.toString('utf8')) as SobreCloudEvent;
      if (!evento || typeof evento.id !== 'string' || !evento.id.trim()) {
        throw new Error('Evento inválido: falta el identificador `id`.');
      }
    } catch (err) {
      this.log.warn(`Mensaje malformado rechazado a DLQ: ${(err as Error).message}`);
      this.enviadosADlq++;
      canal.nack(msg, false, false);
      return 'dlq';
    }

    // Deduplicación idempotente por id del evento
    if (this.idsProcesados.has(evento.id)) {
      this.duplicados++;
      this.intentosPorMensaje.delete(evento.id);
      canal.ack(msg);
      return 'duplicado';
    }

    try {
      exigirEventoSinPii(evento.data);
      if (manejadorNegocio) {
        await manejadorNegocio(evento);
      }

      this.idsProcesados.add(evento.id);
      this.intentosPorMensaje.delete(evento.id);
      this.historial.push({
        id: evento.id,
        type: evento.type,
        sequence: Number(evento.sequence),
        subject: evento.subject,
        ...(evento.correlationid ? { correlationid: evento.correlationid } : {}),
        procesadoEn: new Date().toISOString(),
      });
      if (this.historial.length > 1000) {
        this.historial.shift();
      }

      canal.ack(msg);
      return 'procesado';
    } catch (err) {
      const intentosPrevios = this.intentosPorMensaje.get(evento.id) ?? 0;
      const intentoActual = intentosPrevios + 1;
      this.intentosPorMensaje.set(evento.id, intentoActual);

      if (intentoActual >= MAX_INTENTOS_CONSUMIDOR) {
        this.log.error(
          `Evento ${evento.id} falló ${intentoActual} veces (${(err as Error).message}); enviando a DLQ (${COLA_CONSUMIDOR_EJEMPLO}.dlq).`,
        );
        this.intentosPorMensaje.delete(evento.id);
        this.enviadosADlq++;
        canal.nack(msg, false, false);
        return 'dlq';
      }

      this.log.warn(
        `Fallo al procesar evento ${evento.id} (intento ${intentoActual}/${MAX_INTENTOS_CONSUMIDOR}): ${(err as Error).message}. Se reintentará.`,
      );
      canal.nack(msg, false, true);
      return 'reintentar';
    }
  }

  fueProcesado(eventoId: string): boolean {
    return this.idsProcesados.has(eventoId);
  }

  obtenerEstadisticas(): {
    procesados: number;
    duplicadosIgnorados: number;
    enviadosADlq: number;
    ultimosEventos: ResumenEventoConsumido[];
  } {
    return {
      procesados: this.idsProcesados.size,
      duplicadosIgnorados: this.duplicados,
      enviadosADlq: this.enviadosADlq,
      ultimosEventos: [...this.historial.slice(-20)],
    };
  }
}

