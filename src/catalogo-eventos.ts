import type { SobreCloudEvent } from './rabbitmq.service';

// Catálogo de eventos que circulan por el bus. El `type` del CloudEvent es también la routing key en el exchange
// topic del dominio (cozy.<dominio>). La clave de idempotencia es el `id` del sobre (eventoId de la outbox): el
// consumidor deduplica por él, por eso no se repite dentro de `data`.
// Ningún `data` lleva datos personales (LOPDP §7.1, ver lopdp.ts).

// EventType cerrado del contrato alojamientos-openapi-v2 (v2.2.0). Son los únicos que pueden llegar al feed B2B
// (comun.feed_eventos tiene un CHECK con esta misma lista).
export const TIPOS_EVENTO_CONTRATO = [
  'alojamiento.reserva.confirmada',
  'alojamiento.reserva.cancelada',
  'alojamiento.checkin.completado',
  'alojamiento.checkout.completado',
  'alojamiento.stock.actualizado',
] as const;
export type TipoEventoContrato = (typeof TIPOS_EVENTO_CONTRATO)[number];

// Eventos internos de Cozy: viajan por RabbitMQ entre microservicios y NO forman parte del feed del contrato.
export const EVENTO_HABITACION_LIMPIA_LISTA = 'habitacion.limpia.lista';
export const TIPOS_EVENTO_INTERNOS = [EVENTO_HABITACION_LIMPIA_LISTA] as const;
export type TipoEventoInterno = (typeof TIPOS_EVENTO_INTERNOS)[number];

export type TipoEvento = TipoEventoContrato | TipoEventoInterno;

export function esEventoDelContrato(tipo: string): tipo is TipoEventoContrato {
  return (TIPOS_EVENTO_CONTRATO as readonly string[]).includes(tipo);
}

// Sobre CloudEvents tipado por evento
export type EventoCozy<T extends TipoEvento, D> = Omit<SobreCloudEvent, 'type' | 'data'> & { type: T; data: D };

// ───────────────────────── Eventos del contrato (los emite reservas/) ─────────────────────────
// `alojamientoId` dentro de data es el código público del alojamiento (HTL-…), como pide EventDataReservaV2.
// tipoHabitacionId, checkin y checkout son opcionales porque reservas/ aún no los envía; el contrato admite
// propiedades adicionales en EventDataReservaV2, así que agregarlos no lo rompe.

export interface DatosEventoReserva {
  orderId: string;
  external_order_id: string;
  saga_id?: string;
  estado: string;
  alojamientoId: string;
  total?: string;
  tipoHabitacionId?: string;
  /** YYYY-MM-DD */
  checkin?: string;
  /** YYYY-MM-DD */
  checkout?: string;
}

export interface DatosEventoCheckin {
  orderId: string;
  external_order_id: string;
  estado: 'CHECKED_IN';
  alojamientoId: string;
  tipoHabitacionCodigo: string;
  checkInHora: string;
  tipoHabitacionId?: string;
  checkin?: string;
  checkout?: string;
}

export interface DatosEventoCheckout {
  orderId: string;
  external_order_id: string;
  estado: 'CHECKED_OUT';
  alojamientoId: string;
  tipoHabitacionCodigo: string;
  checkOutHora: string;
  consumosAdicionales: string;
  tipoHabitacionId?: string;
  checkin?: string;
  checkout?: string;
}

export interface DatosEventoStock {
  alojamientoId: string;
  tipoHabitacionCodigo: string;
  /** YYYY-MM-DD */
  fecha: string;
  unidadesDisponibles: number;
}

export type ReservaConfirmadaEvent = EventoCozy<'alojamiento.reserva.confirmada', DatosEventoReserva>;
export type ReservaCanceladaEvent = EventoCozy<'alojamiento.reserva.cancelada', DatosEventoReserva>;
export type CheckinCompletadoEvent = EventoCozy<'alojamiento.checkin.completado', DatosEventoCheckin>;
export type CheckoutCompletadoEvent = EventoCozy<'alojamiento.checkout.completado', DatosEventoCheckout>;
export type StockActualizadoEvent = EventoCozy<'alojamiento.stock.actualizado', DatosEventoStock>;

// ───────────────────────── habitacion.limpia.lista (interno) ─────────────────────────
// Una habitación física quedó LIMPIA y vuelve a estar lista para un check-in.

export interface DatosHabitacionLimpiaLista {
  reservaId: string;
  alojamientoId: string;
  tipoHabitacionCodigo: string;
  tipoHabitacionId?: string;
  habitacionFisicaId: string;
  /** YYYY-MM-DD */
  checkin: string;
  /** YYYY-MM-DD */
  checkout: string;
  usuarioId?: string;
  /** ISO 8601 con zona, p. ej. 2026-11-12T10:30:00-05:00 */
  timestamp: string;
}

export type HabitacionLimpiaListaEvent = EventoCozy<typeof EVENTO_HABITACION_LIMPIA_LISTA, DatosHabitacionLimpiaLista>;

const FECHA = /^\d{4}-\d{2}-\d{2}$/;
const INSTANTE_CON_ZONA = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(:\d{2}(\.\d{1,3})?)?(Z|[+-]\d{2}:\d{2})$/;
const OBLIGATORIOS = ['reservaId', 'alojamientoId', 'tipoHabitacionCodigo', 'habitacionFisicaId', 'checkin', 'checkout', 'timestamp'] as const;

const esFechaReal = (v: string) => FECHA.test(v) && !Number.isNaN(Date.parse(`${v}T00:00:00Z`)) && new Date(`${v}T00:00:00Z`).toISOString().startsWith(v);

// Lista de problemas del `data` de habitacion.limpia.lista; vacía si es válido.
export function erroresHabitacionLimpiaLista(datos: unknown): string[] {
  if (datos === null || typeof datos !== 'object' || Array.isArray(datos)) return ['data debe ser un objeto'];
  const d = datos as Record<string, unknown>;
  const errores: string[] = [];
  for (const campo of OBLIGATORIOS) {
    if (typeof d[campo] !== 'string' || (d[campo] as string).trim() === '') errores.push(`${campo} es obligatorio y debe ser texto`);
  }
  if (d.usuarioId !== undefined && (typeof d.usuarioId !== 'string' || d.usuarioId.trim() === '')) errores.push('usuarioId debe ser texto');
  if (typeof d.checkin === 'string' && !esFechaReal(d.checkin)) errores.push('checkin debe tener el formato YYYY-MM-DD');
  if (typeof d.checkout === 'string' && !esFechaReal(d.checkout)) errores.push('checkout debe tener el formato YYYY-MM-DD');
  if (typeof d.checkin === 'string' && typeof d.checkout === 'string' && esFechaReal(d.checkin) && esFechaReal(d.checkout) && d.checkout <= d.checkin) {
    errores.push('checkout debe ser posterior a checkin');
  }
  if (typeof d.timestamp === 'string' && (!INSTANTE_CON_ZONA.test(d.timestamp) || Number.isNaN(Date.parse(d.timestamp)))) {
    errores.push('timestamp debe ser un instante ISO 8601 con zona');
  }
  return errores;
}

export function esHabitacionLimpiaLista(datos: unknown): datos is DatosHabitacionLimpiaLista {
  return erroresHabitacionLimpiaLista(datos).length === 0;
}
