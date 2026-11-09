// Cumplimiento LOPDP (§7.1 del contrato v2 y diseño de fase 9):
// Los eventos que circulan por las outbox, RabbitMQ y el feed hacia Booking NUNCA llevan datos personales (PII):
// únicamente identificadores (orderId, alojamientoId, habitacionId, saga_id, etc.) y cifras (unidades, montos, fechas).

export const CAMPOS_PII_PROHIBIDOS = new Set([
  'email',
  'correo',
  'password',
  'passwordhash',
  'telefono',
  'phone',
  'celular',
  'documento',
  'documentoidentidad',
  'numerodocumento',
  'cedula',
  'pasaporte',
  'ruc',
  'nombre',
  'apellido',
  'nombrecompleto',
  'first_name',
  'last_name',
  'full_name',
  'customer_details',
  'huesped',
  'titular',
  'direccion',
  'address',
  'tarjeta',
  'card_number',
  'cvv',
  'ip',
]);

export class EventoConPiiException extends Error {
  constructor(public readonly campo: string) {
    super(`Violación LOPDP: el evento contiene el campo personal prohibido '${campo}'. Solo se permiten identificadores y cifras.`);
  }
}

function normalizarClave(clave: string): string {
  return clave.trim().toLowerCase();
}

// Verifica recursivamente que el objeto `datos` de un evento no contenga ningún campo de datos personales (PII).
export function exigirEventoSinPii(datos: unknown, ruta = 'data'): void {
  if (datos === null || datos === undefined || typeof datos !== 'object') return;
  if (Array.isArray(datos)) {
    datos.forEach((item, idx) => exigirEventoSinPii(item, `${ruta}[${idx}]`));
    return;
  }
  for (const [clave, valor] of Object.entries(datos as Record<string, unknown>)) {
    if (CAMPOS_PII_PROHIBIDOS.has(normalizarClave(clave))) {
      throw new EventoConPiiException(`${ruta}.${clave}`);
    }
    exigirEventoSinPii(valor, `${ruta}.${clave}`);
  }
}

// Elimina cualquier campo PII si se necesita sanear un objeto antes de registrarlo en logs.
export function limpiarPii<T>(valor: T): T {
  if (valor === null || valor === undefined || typeof valor !== 'object') return valor;
  if (Array.isArray(valor)) return valor.map((v) => limpiarPii(v)) as unknown as T;
  const limpio: Record<string, unknown> = {};
  for (const [clave, subvalor] of Object.entries(valor as Record<string, unknown>)) {
    if (CAMPOS_PII_PROHIBIDOS.has(normalizarClave(clave))) continue;
    limpio[clave] = limpiarPii(subvalor);
  }
  return limpio as T;
}

