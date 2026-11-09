import { Injectable, Logger, OnApplicationBootstrap, OnModuleDestroy, Optional } from '@nestjs/common';
import { CanalConfirmacionAmqp, conectarAmqp, ConexionAmqp, MensajeAmqp } from './cliente-amqp';
import { declararTopologiaRabbitMq } from './topologia-rabbitmq';

export interface SobreCloudEvent {
  id: string;
  source: string;
  specversion: '1.0';
  type: string;
  time: string;
  sequence: number;
  subject: string;
  correlationid?: string;
  datacontenttype: 'application/json';
  data: Record<string, unknown>;
}

export type FabricaConexionAmqp = (url: string) => Promise<ConexionAmqp>;

@Injectable()
export class RabbitMqService implements OnApplicationBootstrap, OnModuleDestroy {
  private readonly log = new Logger('RabbitMqService');
  private conexion: ConexionAmqp | null = null;
  private canal: CanalConfirmacionAmqp | null = null;
  private conectando: Promise<CanalConfirmacionAmqp> | null = null;
  private destruido = false;
  private temporizadorReconexion?: NodeJS.Timeout;
  private suscripciones: { cola: string; manejador: (msg: MensajeAmqp) => Promise<void> }[] = [];

  constructor(@Optional() private readonly fabricaConexion: FabricaConexionAmqp = conectarAmqp) {}

  onApplicationBootstrap(): void {
    void this.asegurarCanal().catch((err) => {
      this.log.warn(`RabbitMQ no disponible al iniciar (${(err as Error).message}); se reintentará en segundo plano.`);
      this.programarReconexion();
    });
  }

  async onModuleDestroy(): Promise<void> {
    this.destruido = true;
    if (this.temporizadorReconexion) clearTimeout(this.temporizadorReconexion);
    await this.cerrarActual();
  }

  private obtenerUrl(): string {
    // La URL vive en .env fuera del repositorio (RABBITMQ_URL); nunca se imprime en logs
    const configurada = process.env.RABBITMQ_URL?.trim();
    if (configurada) return configurada;
    const host = process.env.RABBITMQ_HOST?.trim() || 'localhost';
    const port = process.env.RABBITMQ_PORT?.trim() || '5672';
    return ['amqp', `${host}:${port}`].join('://');
  }

  async asegurarCanal(): Promise<CanalConfirmacionAmqp> {
    if (this.canal) return this.canal;
    if (this.conectando) return this.conectando;

    this.conectando = (async () => {
      try {
        const conexion = await this.fabricaConexion(this.obtenerUrl());
        conexion.on('error', (err: Error) => {
          this.log.warn(`Error en conexión RabbitMQ: ${err.message}`);
        });
        conexion.on('close', () => {
          this.canal = null;
          this.conexion = null;
          if (!this.destruido) this.programarReconexion();
        });

        const canal = await conexion.createConfirmChannel();
        await declararTopologiaRabbitMq(canal);
        this.conexion = conexion;
        this.canal = canal;

        for (const sub of this.suscripciones) {
          await this.activarSuscripcionEnCanal(canal, sub.cola, sub.manejador);
        }
        this.log.log('Topología de RabbitMQ declarada (4 exchanges por dominio, DLX y colas con DLQ).');
        return canal;
      } finally {
        this.conectando = null;
      }
    })();

    return this.conectando;
  }

  private programarReconexion(): void {
    if (this.destruido || this.temporizadorReconexion) return;
    this.temporizadorReconexion = setTimeout(() => {
      this.temporizadorReconexion = undefined;
      void this.asegurarCanal().catch(() => this.programarReconexion());
    }, 2000);
    this.temporizadorReconexion.unref?.();
  }

  // Publica en el exchange del dominio y espera el publisher confirm de RabbitMQ antes de resolver.
  async publicarConConfirmacion(
    exchange: string,
    routingKey: string,
    evento: SobreCloudEvent,
    cabecerasExtra: Record<string, unknown> = {},
  ): Promise<void> {
    const canal = await this.asegurarCanal();
    const cuerpo = Buffer.from(JSON.stringify(evento), 'utf8');
    await new Promise<void>((resolver, rechazar) => {
      try {
        canal.publish(
          exchange,
          routingKey,
          cuerpo,
          {
            persistent: true,
            contentType: 'application/cloudevents+json',
            messageId: evento.id,
            correlationId: evento.correlationid,
            timestamp: Math.floor(new Date(evento.time).getTime() / 1000),
            headers: {
              'x-sequence': evento.sequence,
              'x-subject': evento.subject,
              ...cabecerasExtra,
            },
          },
          (err) => {
            if (err) rechazar(err);
            else resolver();
          },
        );
      } catch (err) {
        rechazar(err as Error);
      }
    });
    await canal.waitForConfirms();
  }

  async registrarConsumidor(cola: string, manejador: (msg: MensajeAmqp, canal: CanalConfirmacionAmqp) => Promise<void>): Promise<void> {
    const envoltorio = async (msg: MensajeAmqp) => {
      if (!this.canal) return;
      await manejador(msg, this.canal);
    };
    this.suscripciones.push({ cola, manejador: envoltorio });
    if (this.canal) {
      await this.activarSuscripcionEnCanal(this.canal, cola, envoltorio);
    }
  }

  private async activarSuscripcionEnCanal(
    canal: CanalConfirmacionAmqp,
    cola: string,
    manejador: (msg: MensajeAmqp) => Promise<void>,
  ): Promise<void> {
    await canal.prefetch(10);
    await canal.consume(
      cola,
      (msg) => {
        if (!msg) return;
        void manejador(msg);
      },
      { noAck: false },
    );
  }

  async comprobarSalud(): Promise<{ conectado: boolean }> {
    try {
      const canal = await Promise.race([
        this.asegurarCanal(),
        new Promise<null>((r) => setTimeout(() => r(null), 2000).unref?.()),
      ]);
      return { conectado: Boolean(canal && this.conexion) };
    } catch {
      return { conectado: false };
    }
  }

  private async cerrarActual(): Promise<void> {
    const canal = this.canal;
    const conexion = this.conexion;
    this.canal = null;
    this.conexion = null;
    await canal?.close().catch(() => {});
    await conexion?.close().catch(() => {});
  }
}
