# Event Bus (`event-bus-cozy`)

Servicio transaccional de mensajería con RabbitMQ y publicador del feed de eventos para Booking B2B.

## Características
- **Cliente AMQP 0-9-1:** Implementación propia ligera con soporte de reconexión y publisher confirms.
- **Topología RabbitMQ:** Exchanges por dominio (`cozy.<dominio>`), DLX (`cozy.dlx`) y colas con DLQ.
- **Publicador de Outbox:** Lee transaccionalmente las outbox de dominio (`outbox_usuarios`, `outbox_catalogo`, `outbox_disponibilidad`, `outbox_reservas`) y publica en RabbitMQ asignando `sequence` garantizado a `comun.feed_eventos`.
- **Salud:** Endpoint `GET /health` con verificación del estado de RabbitMQ y base de datos.

## Instalación y compilación
```bash
npm ci
npm run build
```

## Pruebas
```bash
npm test
```

