import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { TypeOrmModule } from '@nestjs/typeorm';
import { opcionesBaseDatosEventBus } from './conexiones';
import { ConsumidorEjemploService } from './consumidor-ejemplo.service';
import { HealthController } from './health.controller';
import { PublicadorOutboxService } from './publicador-outbox.service';
import { RabbitMqService } from './rabbitmq.service';

@Module({
  imports: [
    ConfigModule.forRoot({
      isGlobal: true,
      envFilePath: ['.env', '../backend/.env', '../.env'],
    }),
    TypeOrmModule.forRootAsync({
      useFactory: () => ({
        type: 'postgres' as const,
        ...opcionesBaseDatosEventBus(process.env),
        autoLoadEntities: false,
        synchronize: false,
      }),
    }),
  ],
  controllers: [HealthController],
  providers: [RabbitMqService, PublicadorOutboxService, ConsumidorEjemploService],
  exports: [RabbitMqService, PublicadorOutboxService, ConsumidorEjemploService],
})
export class EventBusAppModule {}

