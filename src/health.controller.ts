import { Controller, Get, HttpException, HttpStatus, Optional, VERSION_NEUTRAL } from '@nestjs/common';
import { InjectDataSource } from '@nestjs/typeorm';
import { DataSource } from 'typeorm';
import { RabbitMqService } from './rabbitmq.service';

@Controller({ path: 'health', version: VERSION_NEUTRAL })
export class HealthController {
  constructor(
    private readonly rabbit: RabbitMqService,
    @Optional() @InjectDataSource() private readonly dataSource?: DataSource,
  ) {}

  @Get()
  async check() {
    const { conectado: rabbitOk } = await this.rabbit.comprobarSalud();

    let dbOk = true;
    if (this.dataSource) {
      try {
        await this.dataSource.query('SELECT 1');
      } catch {
        dbOk = false;
      }
    }

    const cuerpo = {
      status: rabbitOk && dbOk ? 'ok' : 'error',
      modulo: 'event-bus',
      rabbitmq: rabbitOk ? 'up' : 'down',
      database: dbOk ? 'up' : 'down',
      uptime: Math.floor(process.uptime()),
    };

    if (!rabbitOk || !dbOk) {
      throw new HttpException(cuerpo, HttpStatus.SERVICE_UNAVAILABLE);
    }

    return cuerpo;
  }
}

