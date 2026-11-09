import './registro-rutas';
import 'reflect-metadata';
import { AsyncLocalStorage } from 'async_hooks';
import { randomUUID } from 'crypto';
import { ConsoleLogger, Logger, RequestMethod, VersioningType } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { NestExpressApplication } from '@nestjs/platform-express';
import { getDataSourceToken } from '@nestjs/typeorm';
import { NextFunction, Request, Response } from 'express';
import { DataSource } from 'typeorm';
import { EventBusAppModule } from './app.module';

const contextoCorrelacion = new AsyncLocalStorage<{ correlationId: string }>();

class LoggerEventBus extends ConsoleLogger {
  private prefijo(): string {
    const cid = contextoCorrelacion.getStore()?.correlationId;
    return cid ? `[cid=${cid}] ` : '';
  }

  override log(message: unknown, context?: string): void {
    super.log(`${this.prefijo()}${String(message)}`, context ?? this.context);
  }

  override warn(message: unknown, context?: string): void {
    super.warn(`${this.prefijo()}${String(message)}`, context ?? this.context);
  }

  override error(message: unknown, stack?: string, context?: string): void {
    super.error(`${this.prefijo()}${String(message)}`, stack, context ?? this.context);
  }
}

async function comprobarRolComun(app: NestExpressApplication): Promise<void> {
  const log = new Logger('ConexionPorRol:event-bus');
  const usaRolComun = !!process.env.DB_CLAVE_COMUN?.trim();
  const ds = app.get<DataSource>(getDataSourceToken());
  const [{ rol }] = await ds.query<[{ rol: string }]>('SELECT current_user AS rol');
  if (usaRolComun && rol !== 'comun') {
    throw new Error(`La conexión de event-bus abrió sesión como ${rol}, no como comun`);
  }
  log.log(
    usaRolComun
      ? `Conexión con su rol de base (comun=${rol})`
      : `Sin DB_CLAVE_COMUN: conexión con el usuario general (${rol}).`,
  );
}

export async function bootstrap(): Promise<NestExpressApplication> {
  const app = await NestFactory.create<NestExpressApplication>(EventBusAppModule, {
    logger: new LoggerEventBus(),
  });

  await comprobarRolComun(app);

  // GET /health responde en la raíz sin prefijo (/health, VERSION_NEUTRAL); el feed de Booking (/api/v2/alojamientos/eventos) vive solo en el backend
  app.setGlobalPrefix('api', {
    exclude: [{ path: 'health', method: RequestMethod.GET }],
  });
  app.enableVersioning({
    type: VersioningType.URI,
    defaultVersion: '1',
  });

  app.use((req: Request, res: Response, next: NextFunction) => {
    const entrante = req.headers['x-correlation-id'];
    const correlationId =
      typeof entrante === 'string' && entrante.trim().length > 0 ? entrante.trim().slice(0, 128) : randomUUID();
    req.headers['x-correlation-id'] = correlationId;
    res.setHeader('X-Correlation-ID', correlationId);
    contextoCorrelacion.run({ correlationId }, () => next());
  });

  app.enableCors();
  app.enableShutdownHooks();

  const port = Number(process.env.PORT_EVENT_BUS || process.env.PORT_COMUN || process.env.PORT || 3005);
  await app.listen(port);
  new Logger('Bootstrap:event-bus').log(
    `Servicio event-bus corriendo en: http://localhost:${port} (salud: http://localhost:${port}/health)`,
  );
  return app;
}

if (require.main === module) {
  void bootstrap();
}

