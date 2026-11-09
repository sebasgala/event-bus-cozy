import { existsSync } from 'fs';
import Module = require('module');
import { resolve } from 'path';

// Permite resolver dependencias desde ../backend/node_modules cuando event-bus se ejecuta
// dentro del monorepo antes de separarse a su propio repositorio en el paso 9.6.
const rutaBackendNodeModules = resolve(__dirname, '..', '..', 'backend', 'node_modules');
if (existsSync(rutaBackendNodeModules)) {
  const separador = process.platform === 'win32' ? ';' : ':';
  const actuales = process.env.NODE_PATH ? process.env.NODE_PATH.split(separador).filter(Boolean) : [];
  if (!actuales.includes(rutaBackendNodeModules)) {
    process.env.NODE_PATH = [...actuales, rutaBackendNodeModules].join(separador);
    (Module as unknown as { _initPaths?: () => void })._initPaths?.();
  }
}

