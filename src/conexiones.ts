import { PostgresConnectionOptions } from 'typeorm/driver/postgres/PostgresConnectionOptions';

type Entorno = Record<string, string | undefined>;

export class ConexionPorRolIncompleta extends Error {}

function usarSsl(env: Entorno): boolean {
  const explicito = env.DATABASE_SSL?.trim().toLowerCase();
  if (explicito === 'true' || explicito === '1') return true;
  if (explicito === 'false' || explicito === '0') return false;
  return env.NODE_ENV === 'production';
}

export function opcionesBaseDatosEventBus(
  env: Entorno = process.env,
): Pick<PostgresConnectionOptions, 'url' | 'host' | 'port' | 'username' | 'password' | 'database' | 'ssl' | 'applicationName'> {
  const ssl = usarSsl(env) ? { rejectUnauthorized: false } : false;
  const applicationName = 'cozy-event-bus';
  const claveComun = env.DB_CLAVE_COMUN?.trim();
  const url = env.DATABASE_URL?.trim();

  if (claveComun) {
    if (url) {
      const u = new URL(url);
      return {
        host: decodeURIComponent(u.hostname),
        port: Number(u.port) || 5432,
        database: decodeURIComponent(u.pathname.replace(/^\//, '')),
        username: 'comun',
        password: claveComun,
        ssl,
        applicationName,
      };
    }
    return {
      host: env.DB_HOST || 'localhost',
      port: Number(env.DB_PORT) || 5432,
      database: env.DB_NAME || 'cozy_v2',
      username: 'comun',
      password: claveComun,
      ssl,
      applicationName,
    };
  }

  // Si hay otras claves DB_CLAVE_* definidas pero falta DB_CLAVE_COMUN, falla en el arranque sin imprimir claves
  const otrasClaves = ['DB_CLAVE_USUARIOS', 'DB_CLAVE_CATALOGO', 'DB_CLAVE_DISPONIBILIDAD', 'DB_CLAVE_RESERVAS'].filter(
    (k) => !!env[k]?.trim(),
  );
  if (otrasClaves.length > 0) {
    throw new ConexionPorRolIncompleta(
      `Conexión por rol incompleta para event-bus (rol comun): falta DB_CLAVE_COMUN (están definidas ${otrasClaves.join(', ')}).`,
    );
  }

  if (url) {
    return { url, ssl, applicationName };
  }
  return {
    host: env.DB_HOST || 'localhost',
    port: Number(env.DB_PORT) || 5432,
    username: env.DB_USER || 'postgres',
    password: env.DB_PASSWORD || 'postgres',
    database: env.DB_NAME || 'cozy_v2',
    ssl,
    applicationName,
  };
}

