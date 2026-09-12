/**
 * @file index.js
 * @description Gestor de procesos FFmpeg mediante Docker para la retransmisión de transmisiones en vivo (Live Streaming).
 * Microservicio ligero encargado de la orquestación, inicio, monitoreo y detención de contenedores Docker con FFmpeg.
 * 
 * Permite tomar transmisiones ingresadas a SRS Media Server (vía WebRTC/RTMP) o generar señales sintéticas (testsrc)
 * y retransmitirlas hacia destinos RTMP/RTMPS como Kick o Twitch.
 * 
 * @module ffmpeg-manager
 * @author WorldDance Team
 * @version 1.0.0
 */

const express = require('express');
const http = require('http');
const { WebSocketServer } = require('ws');
const { spawn, execSync } = require('child_process');

/**
 * Instancia del servidor Express.
 * @type {express.Application}
 */
const app = express();

/**
 * Puerto de escucha del servicio (por defecto 3000).
 * @type {number|string}
 */
const PORT = process.env.PORT || 3000;

/**
 * Nombre de la red Docker del stack de docker-compose. Los contenedores `linuxserver/ffmpeg`
 * se lanzan como "hermanos" vía el socket de Docker montado (no anidados dentro de este
 * contenedor), así que por defecto caerían en la red `bridge` genérica del host y NO podrían
 * resolver `wd-srs-media-server` por nombre. Hay que unirlos explícitamente a la red que crea
 * Docker Compose (`{nombre_proyecto}_default`, confirmado con `docker compose config`).
 * @type {string}
 */
const DOCKER_NETWORK = process.env.DOCKER_NETWORK || 'worlddance_default';

// Middleware para procesar cuerpos de solicitud JSON
app.use(express.json());

/**
 * Almacenamiento en memoria para el rastreo y control de instancias de streaming activas.
 * La clave corresponde al `streamId` (ID del evento) y el valor contiene la información del proceso y contenedor Docker.
 *
 * @type {Map<string, {process: ChildProcess, containerName: string, sourceType: string, destinationUrl: string, active: boolean, status: string, startedAt: string, recentLogs: string[], exitCode: number|null}>}
 */
const activeStreams = new Map();

/**
 * Almacenamiento en memoria de las ingestas activas por WebSocket (navegador -> ffmpeg-manager -> RTMP a SRS).
 * Independiente de `activeStreams`: esta es la ingesta local (cámara/pantalla), la otra es el reempuje a Kick.
 *
 * @type {Map<string, {process: ChildProcess, containerName: string, startedAt: string, recentLogs: string[], status: string, exitCode: number|null}>}
 */
const activeIngests = new Map();

/** Cuántas líneas recientes de stderr de FFmpeg se conservan por proceso, para diagnóstico. */
const MAX_LOG_LINES = 40;

/**
 * Si el proceso de FFmpeg muere dentro de esta ventana desde que arrancó, se asume que fue un
 * fallo de conexión (no pudo abrir la fuente o el destino) y no un cierre normal/esperado —
 * "is_live: true" en Kick no significa nada si FFmpeg murió a los 2 segundos intentando conectar.
 */
const EARLY_FAILURE_WINDOW_MS = 8000;

/** Cuánto tiempo se conserva visible en /api/stream/status un registro FAILED antes de purgarlo. */
const FAILED_RETENTION_MS = 120000;

/**
 * Traduce patrones comunes del stderr de FFmpeg a una causa legible, para no tener que interpretar
 * el log crudo cada vez. Devuelve `null` si no reconoce ningún patrón conocido.
 * @param {string[]} logLines
 * @returns {string|null}
 */
function diagnoseFailure(logLines) {
  const text = logLines.join('\n');
  if (/Connection refused/i.test(text)) {
    return 'Conexión rechazada: no se pudo alcanzar el servidor de origen o de destino (¿SRS/RTMP no está escuchando en esa dirección?).';
  }
  if (/Input\/output error|End of file|EOF/i.test(text)) {
    return 'La fuente de video se cerró o dejó de enviar datos (Input stream closed) antes de lo esperado.';
  }
  if (/401|Unauthorized/i.test(text)) {
    return 'El servidor de destino rechazó la conexión por credenciales inválidas (401 Unauthorized).';
  }
  if (/Name or service not known|nodename nor servname|Temporary failure in name resolution/i.test(text)) {
    return 'No se pudo resolver el nombre de host del servidor de origen/destino.';
  }
  if (/Connection timed out|Operation timed out/i.test(text)) {
    return 'Tiempo de espera agotado intentando conectar con el servidor de origen o destino.';
  }
  if (/No such file or directory/i.test(text)) {
    return 'La fuente especificada no existe o no es accesible desde el contenedor.';
  }
  return null;
}

/**
 * Si `destinationUrl` usa RTMPS (TLS, normalmente puerto 443), calcula el equivalente en RTMP
 * plano (puerto 1935) al mismo host y ruta. AWS IVS (la infraestructura real detrás del ingest de
 * Kick, reconocible por el hostname "*.global-contribute.live-video.net") acepta ambos protocolos
 * a propósito, precisamente porque el handshake TLS nativo de muchos builds de FFmpeg (incluido el
 * de `linuxserver/ffmpeg`) falla con "[tls] IO error: End of file" contra este tipo de endpoints
 * (típicamente por SNI/ALPN incompleto del lado del cliente) mientras que el RTMP sin cifrar
 * funciona sin problema — la propia clave de transmisión ya es el secreto real, no el transporte.
 * Devuelve `null` si la URL no es RTMPS (no hay fallback que ofrecer).
 */
function buildRtmpFallback(destinationUrl) {
  if (!/^rtmps:\/\//i.test(destinationUrl)) return null;
  const rest = destinationUrl.slice('rtmps://'.length);
  const slashIdx = rest.indexOf('/');
  const hostPort = slashIdx === -1 ? rest : rest.slice(0, slashIdx);
  const path = slashIdx === -1 ? '' : rest.slice(slashIdx);
  const host = hostPort.split(':')[0];
  return `rtmp://${host}:1935${path}`;
}

/**
 * @param {Map<string, object>} map
 * @param {string} streamId
 * @param {import('child_process').ChildProcess} process
 * @param {string} logPrefix
 * @param {{ onEarlyFailure?: (diagnosis: string|null, entry: object) => boolean }} [options] -
 *        `onEarlyFailure` se invoca ANTES de marcar la entrada como FAILED; si devuelve `true`
 *        (ej. porque lanzó un reintento con otra URL, que ya reemplazó la entrada del mapa), no se
 *        sobreescribe con el estado FAILED de este intento.
 */
function trackFfmpegProcess(map, streamId, process, logPrefix, options = {}) {
  const { onEarlyFailure } = options;

  process.stderr.on('data', (data) => {
    const text = data.toString();
    console.error(`${logPrefix} ${text}`);
    const entry = map.get(streamId);
    if (entry) {
      text.split('\n').filter(Boolean).forEach(line => {
        entry.recentLogs.push(line);
        if (entry.recentLogs.length > MAX_LOG_LINES) entry.recentLogs.shift();
      });
    }
  });

  process.on('error', (err) => {
    console.error(`${logPrefix} No se pudo iniciar el proceso Docker/FFmpeg:`, err.message);
  });

  process.on('close', (code) => {
    const entry = map.get(streamId);
    const ranMs = entry ? Date.now() - new Date(entry.startedAt).getTime() : Infinity;
    // Un proceso de retransmisión/ingesta en vivo NUNCA debería terminar por sí solo a los pocos
    // segundos de arrancar (código 0 incluido: una salida "limpia" e inmediata suele ser la fuente
    // cerrándose de inmediato, ej. el pull a SRS sin publicador real). Solo un cierre explícito por
    // /api/stream/stop pasa por otra ruta (execSync docker rm -f), no por aquí.
    const isEarlyFailure = code !== null && ranMs < EARLY_FAILURE_WINDOW_MS;

    if (isEarlyFailure && entry) {
      const diagnosis = diagnoseFailure(entry.recentLogs);
      console.error(
        `${logPrefix} FFmpeg terminó con código ${code} a los ${ranMs}ms de iniciar `
        + `(fuente=${entry.sourceType ?? 'ingesta'}, destino=${entry.destinationUrl ?? `rtmp SRS ${streamId}`}): `
        + (diagnosis ?? 'no se pudo determinar la causa exacta a partir del log.') + '\n'
        + `Últimas líneas de stderr:\n` + entry.recentLogs.slice(-10).join('\n')
      );

      if (onEarlyFailure && onEarlyFailure(diagnosis, entry)) {
        // El callback ya relanzó el proceso (ej. fallback RTMP) y reemplazó esta entrada del mapa
        // con la del nuevo intento; no la pisamos aquí con el estado FAILED de este intento viejo.
        return;
      }

      entry.active = false;
      entry.status = 'FAILED';
      entry.exitCode = code;
      entry.errorSummary = diagnosis;
      // Se conserva un rato para que el polling de estado (Angular/Java) pueda leer el error real
      // en vez de encontrarse un simple 404 "NOT_FOUND" que no distingue "nunca existió" de "falló".
      setTimeout(() => map.delete(streamId), FAILED_RETENTION_MS);
    } else {
      console.log(`${logPrefix} Proceso finalizado con código de salida ${code}`);
      map.delete(streamId);
    }
  });
}

/**
 * Detiene y elimina (si existe) el contenedor Docker de ingesta asociado a un streamId.
 * @param {string} streamId
 */
function stopIngest(streamId) {
  const ingest = activeIngests.get(streamId);
  if (!ingest) return;

  console.log(`[Ingest - ${streamId}] Deteniendo ingesta y removiendo contenedor ${ingest.containerName}...`);
  try {
    ingest.process.stdin.end();
  } catch (e) {}
  try {
    execSync(`docker rm -f ${ingest.containerName}`, { stdio: 'pipe' });
  } catch (e) {}
  activeIngests.delete(streamId);
}

/**
 * Construye los argumentos de entrada de FFmpeg según el sourceType, lanza el contenedor Docker de
 * retransmisión y engancha su seguimiento (logs + detección de fallo temprano). Extraído de la
 * ruta HTTP para poder reutilizarlo tanto en el arranque inicial como en el reintento automático
 * por RTMP plano cuando el intento por RTMPS falla temprano (ver `buildRtmpFallback`).
 */
function launchPushStream(streamId, destinationUrl, sourceType, sourceUrl, isFallbackAttempt) {
  const containerName = `ffmpeg-stream-${streamId}`;
  try {
    execSync(`docker rm -f ${containerName}`, { stdio: 'pipe' });
  } catch (e) {}

  let inputArgs = [];
  if (sourceType === 'camera' || sourceType === 'screen') {
    const srsInputUrl = sourceUrl || `rtmp://wd-srs-media-server:1935/live/${streamId}`;
    inputArgs = ['-re', '-i', srsInputUrl];
  } else {
    inputArgs = [
      '-re',
      '-f', 'lavfi', '-i', 'testsrc=size=1280x720:rate=30',
      '-f', 'lavfi', '-i', 'sine=frequency=1000:sample_rate=48000'
    ];
  }

  // Perfil de codificación exigido por Kick/AWS IVS para la retransmisión de salida: el origen
  // (SRS/cámara) puede llegar en "baseline" y a framerate variable (~15fps observado), y AWS IVS
  // descarta o degrada el tráfico que no cumple perfil main/high, yuv420p, framerate constante y
  // GOP de 2s exactos. "-r 30" fuerza framerate de salida constante independientemente del de
  // entrada; "-profile:v high" evita que libx264 caiga en "baseline" con el preset "ultrafast";
  // "-b:v/-maxrate/-bufsize" acotan el bitrate a un valor que Kick acepta de forma consistente.
  //
  // "-vf scale=in_range=full:out_range=limited:out_color_matrix=bt709" corrige el warning de
  // swscaler "deprecated pixel format used": la fuente llega como yuvj420p (420p con rango
  // completo 0-255, la variante "j" que FFmpeg marca como obsoleta) y se convierte a yuv420p (rango
  // limitado 16-235, lo que Kick/AWS IVS espera); sin declarar explícitamente "in_range=full" el
  // filtro no puede resolver esa ambigüedad de rango y emite el warning en cada frame. Solo con
  // "out_color_matrix" (sin "in_range") el warning persiste, por eso se declaran ambos.
  //
  // "-tune zerolatency" desactiva B-frames y el lookahead de libx264 (pensados para VOD, no para
  // directo), que es lo que de verdad reduce el consumo de CPU en un encode en vivo con "ultrafast".
  // NO se usa "-vsync"/"-fps_mode" para forzar CFR explícitamente: "-vsync" fue eliminado en FFmpeg
  // 9.0 ("Unrecognized option 'vsync'", el proceso ni siquiera arranca) y "-fps_mode" no es
  // necesario aquí porque "-r 30" ya fuerza por sí solo el mismo comportamiento (el "fps filter"
  // interno duplica frames para completar 30fps a partir de una fuente de ~15fps) sin depender de
  // esa opción. Ese es también el origen real del "dup=385" observado en los logs: si el ~15fps de
  // origen es estable y no un problema transitorio de red,
  // la forma real de reducir esa duplicación (y la carga de CPU que conlleva) es bajar "-r" a un
  // valor más cercano al de la fuente (ajustando "-g" proporcionalmente, ya que debe ser ~2x el
  // framerate) — no se aplica aquí porque cambia el framerate de salida entregado a Kick, una
  // decisión de producto/calidad y no solo de rendimiento.
  //
  // "-threads 0" dice explícitamente a libx264 que autodetecte el número de hilos según los CPUs
  // disponibles en el contenedor; es el comportamiento por defecto en builds modernos de FFmpeg,
  // pero se declara explícito para no depender de heurísticas implícitas del build de la imagen.
  const ffmpegArgs = [
    'run',
    '--name', containerName,
    '--rm',
    '--network', DOCKER_NETWORK,
    '-i',
    'linuxserver/ffmpeg',
    ...inputArgs,
    '-vf', 'scale=in_range=full:out_range=limited:out_color_matrix=bt709',
    '-c:v', 'libx264',
    '-preset', 'ultrafast',
    '-tune', 'zerolatency',
    '-threads', '0',
    '-profile:v', 'high',
    '-pix_fmt', 'yuv420p',
    '-r', '30',
    '-g', '60',
    '-b:v', '2500k',
    '-maxrate', '2500k',
    '-bufsize', '5000k',
    '-c:a', 'aac',
    '-ar', '44100',
    '-b:a', '128k',
    '-f', 'flv',
    destinationUrl
  ];

  console.log(`[FFmpeg - ${streamId}] Iniciando contenedor${isFallbackAttempt ? ' (reintento por RTMP plano)' : ''}: docker ${ffmpegArgs.join(' ')}`);

  const ffmpegProcess = spawn('docker', ffmpegArgs);

  const streamInfo = {
    process: ffmpegProcess,
    containerName,
    sourceType,
    destinationUrl,
    active: true,
    status: 'RUNNING',
    startedAt: new Date().toISOString(),
    recentLogs: [],
    exitCode: null
  };

  activeStreams.set(streamId, streamInfo);

  trackFfmpegProcess(activeStreams, streamId, ffmpegProcess, `[FFmpeg - ${streamId}]`, {
    onEarlyFailure: () => {
      if (isFallbackAttempt) return false; // ya estábamos en el reintento; no hay otro fallback que ofrecer.

      const fallbackUrl = buildRtmpFallback(destinationUrl);
      if (!fallbackUrl) return false;

      console.warn(
        `[FFmpeg - ${streamId}] La conexión RTMPS falló temprano (posible incompatibilidad TLS/SNI `
        + `del cliente FFmpeg con el servidor de destino); reintentando automáticamente por RTMP `
        + `plano en el puerto 1935: ${fallbackUrl}`
      );
      launchPushStream(streamId, fallbackUrl, sourceType, sourceUrl, true);
      return true;
    }
  });
}

/**
 * @route POST /api/stream/start
 * @description Inicia una nueva transmisión FFmpeg dentro de un contenedor Docker dedicado.
 *
 * Configura los parámetros de entrada según el `sourceType` (cámara, pantalla o fuente sintética `testsrc`)
 * y ejecuta el contenedor `linuxserver/ffmpeg` retransmitiendo el flujo de video y audio en formato FLV/RTMP
 * hacia el servidor de destino especificado (`destinationUrl`). Si ese destino es RTMPS y falla
 * temprano, `launchPushStream` reintenta automáticamente por RTMP plano (ver `buildRtmpFallback`).
 *
 * @param {express.Request} req - Objeto de solicitud HTTP Express.
 * @param {string} req.body.streamId - Identificador único de la transmisión / evento (Requerido).
 * @param {string} req.body.destinationUrl - URL RTMP/RTMPS de destino en Kick o servidor externo (Requerido).
 * @param {string} [req.body.sourceType='testsrc'] - Tipo de fuente de origen ('camera', 'screen', 'testsrc').
 * @param {string} [req.body.sourceUrl] - URL RTMP de origen SRS en caso de personalizar la entrada.
 * @param {express.Response} res - Objeto de respuesta HTTP Express.
 * @returns {Object} Respuesta JSON con el estado de inicio de la transmisión.
 */
app.post('/api/stream/start', (req, res) => {
  const { streamId, destinationUrl, sourceType = 'testsrc', sourceUrl } = req.body;

  // Validación de parámetros obligatorios
  if (!streamId || !destinationUrl) {
    return res.status(400).json({ error: 'streamId y destinationUrl son requeridos' });
  }

  // Verificar si ya existe una transmisión activa con el mismo streamId
  const existing = activeStreams.get(streamId);
  if (existing && existing.active) {
    return res.status(400).json({ error: `La transmisión '${streamId}' ya se encuentra activa` });
  }

  // Verificación de conectividad con el demonio de Docker antes de intentar lanzar nada.
  try {
    execSync(`docker rm -f ffmpeg-stream-${streamId}`, { stdio: 'pipe' });
  } catch (e) {
    const errOutput = e.stderr ? e.stderr.toString() : e.message;
    if (errOutput.includes('failed to connect') || errOutput.includes('Is the docker daemon running')) {
      console.error(`[FFmpeg - ${streamId}] Error conectando a Docker Daemon: ${errOutput}`);
      return res.status(500).json({
        error: 'No se pudo conectar con el demonio de Docker (Docker Desktop no está en ejecución)',
        details: errOutput
      });
    }
  }

  const validSourceType = ['camera', 'screen', 'testsrc'].includes(sourceType) ? sourceType : 'testsrc';
  if (validSourceType !== sourceType) {
    // El backend Java ya valida sourceType antes de llamar aquí; si de todos modos llega algo
    // fuera de la whitelist, es mejor dejar rastro explícito de que se sustituyó por testsrc en
    // vez de que la retransmisión "funcione" en silencio con un patrón sintético inesperado.
    console.warn(`[FFmpeg - ${streamId}] sourceType recibido ("${sourceType}") no es válido; se usa 'testsrc' en su lugar.`);
  }

  launchPushStream(streamId, destinationUrl, validSourceType, sourceUrl, false);

  return res.status(200).json({ status: 'started', streamId, sourceType: validSourceType, destinationUrl });
});

/**
 * @route POST /api/stream/stop
 * @description Detiene inmediatamente una transmisión activa destruyendo su contenedor Docker asociado.
 * 
 * Ejecuta `docker rm -f ffmpeg-stream-${streamId}` de forma sincrónica para garantizar el cierre forzado
 * de los sockets RTMP y terminar la emisión en Kick en tiempo real.
 * 
 * @param {express.Request} req - Objeto de solicitud HTTP Express.
 * @param {string} req.body.streamId - Identificador único de la transmisión a detener (Requerido).
 * @param {express.Response} res - Objeto de respuesta HTTP Express.
 * @returns {Object} Respuesta JSON confirmando la detención del stream.
 */
app.post('/api/stream/stop', (req, res) => {
  const { streamId } = req.body;

  if (!streamId) {
    return res.status(400).json({ error: 'streamId es requerido' });
  }

  const containerName = `ffmpeg-stream-${streamId}`;
  console.log(`[FFmpeg - ${streamId}] Deteniendo transmisión y eliminando contenedor ${containerName}...`);

  // 1. Ejecución sincrónica de eliminación forzada del contenedor Docker para liberar el socket RTMP
  try {
    execSync(`docker rm -f ${containerName}`, { stdio: 'pipe' });
    console.log(`[FFmpeg - ${streamId}] Contenedor Docker ${containerName} removido exitosamente.`);
  } catch (e) {
    const errOutput = e.stderr ? e.stderr.toString() : e.message;
    console.error(`[FFmpeg - ${streamId}] Error al remover contenedor Docker: ${errOutput}`);

    // Detección de fallo de comunicación con el servicio de Docker
    if (errOutput.includes('failed to connect') || errOutput.includes('Is the docker daemon running')) {
      return res.status(500).json({
        error: 'No se pudo conectar con el demonio de Docker (Docker Desktop no está en ejecución)',
        details: errOutput
      });
    }
  }

  // 2. Finalización del proceso hijo de Node.js si aún permanece en memoria
  const streamInfo = activeStreams.get(streamId);
  if (streamInfo && streamInfo.process) {
    try {
      streamInfo.process.kill('SIGKILL');
    } catch (e) {}
  }

  // Remover la referencia del registro activo
  activeStreams.delete(streamId);

  return res.status(200).json({ status: 'stopped', streamId });
});

/**
 * Construye el cuerpo de respuesta de estado para un streamId, distinguiendo tres casos:
 * nunca existió (404 NOT_FOUND), sigue corriendo (RUNNING) o murió temprano por un fallo de
 * conexión (FAILED, con las últimas líneas de stderr de FFmpeg para diagnóstico real).
 * @param {string} streamId
 * @returns {{ httpStatus: number, body: object }}
 */
function buildStatusResponse(streamId) {
  const streamInfo = activeStreams.get(streamId);

  if (!streamInfo) {
    return { httpStatus: 404, body: { streamId, active: false, status: 'NOT_FOUND' } };
  }

  if (streamInfo.status === 'FAILED') {
    return {
      httpStatus: 200,
      body: {
        streamId,
        active: false,
        status: 'FAILED',
        exitCode: streamInfo.exitCode,
        errorSummary: streamInfo.errorSummary ?? null,
        error: streamInfo.recentLogs.slice(-10).join('\n'),
        sourceType: streamInfo.sourceType,
        destinationUrl: streamInfo.destinationUrl,
        startedAt: streamInfo.startedAt
      }
    };
  }

  return {
    httpStatus: 200,
    body: {
      streamId,
      active: true,
      status: 'RUNNING',
      sourceType: streamInfo.sourceType,
      destinationUrl: streamInfo.destinationUrl,
      startedAt: streamInfo.startedAt
    }
  };
}

/**
 * @route GET /api/stream/status/:streamId
 * @description Obtiene el estado detallado de una transmisión a partir de su ID en los parámetros de ruta.
 * Distingue NOT_FOUND (nunca se inició) de FAILED (arrancó y murió temprano, con el error real de
 * FFmpeg) de RUNNING — antes ambos casos de fallo colapsaban en un genérico NOT_FOUND.
 *
 * @param {express.Request} req - Objeto de solicitud HTTP Express.
 * @param {string} req.params.streamId - ID del evento / transmisión.
 * @param {express.Response} res - Objeto de respuesta HTTP Express.
 * @returns {Object} Estado detallado del proceso FFmpeg.
 */
app.get('/api/stream/status/:streamId', (req, res) => {
  const { httpStatus, body } = buildStatusResponse(req.params.streamId);
  return res.status(httpStatus).json(body);
});

/**
 * @route POST /api/stream/status
 * @description Obtiene el estado detallado de una transmisión enviando el `streamId` en el cuerpo JSON.
 *
 * @param {express.Request} req - Objeto de solicitud HTTP Express.
 * @param {string} req.body.streamId - ID del evento / transmisión.
 * @param {express.Response} res - Objeto de respuesta HTTP Express.
 * @returns {Object} Estado detallado del proceso FFmpeg.
 */
app.post('/api/stream/status', (req, res) => {
  const { streamId } = req.body;
  if (!streamId) {
    return res.status(400).json({ error: 'streamId es requerido' });
  }
  const { httpStatus, body } = buildStatusResponse(streamId);
  return res.status(httpStatus).json(body);
});

/**
 * @route GET /api/streams
 * @description Devuelve el listado de todos los identificadores de transmisiones (`streamId`) activos en memoria.
 * 
 * @param {express.Request} req - Objeto de solicitud HTTP Express.
 * @param {express.Response} res - Objeto de respuesta HTTP Express.
 * @returns {Array<string>} Lista con los streamIds de las transmisiones activas.
 */
app.get('/api/streams', (req, res) => {
  const streamIds = Array.from(activeStreams.keys());
  return res.status(200).json(streamIds);
});

/**
 * Servidor HTTP subyacente (compartido entre Express y el WebSocketServer de ingesta), necesario
 * porque `app.listen()` no expone el evento 'upgrade' que requiere un WebSocket.
 */
const server = http.createServer(app);

/**
 * WebSocketServer en modo `noServer`: no escucha un puerto propio, se conecta manualmente al
 * evento 'upgrade' del servidor HTTP solo para las rutas /ws/ingest/:streamId (ver más abajo).
 *
 * `maxPayload` se fija explícitamente en 10MB: los chunks WebM que produce MediaRecorder en el
 * navegador (sobre todo el primero, que incluye el header EBML/Cues) pueden superar el límite por
 * defecto de frame de WebSocket (64KB) usado tanto por `ws` como por el gateway/Reactor Netty en
 * el medio, lo que cerraba la conexión con code 1009 ("Max frame length exceeded"). Debe ser >=
 * al límite configurado en el api-gateway (spring.cloud.gateway.server.webflux.httpclient.websocket).
 */
const WS_MAX_PAYLOAD_BYTES = 10 * 1024 * 1024;
const wss = new WebSocketServer({ noServer: true, maxPayload: WS_MAX_PAYLOAD_BYTES });

/**
 * @route WS /ws/ingest/:streamId
 * @description Ingesta de cámara/pantalla por WebSocket (TCP puro) en reemplazo de WHIP/WebRTC:
 * el navegador graba el MediaStream local con MediaRecorder (contenedor WebM) y envía los chunks
 * binarios por este socket. Aquí se lanza un contenedor `linuxserver/ffmpeg` que lee esos chunks
 * por stdin, los transcodifica a H.264/AAC y los reempuja como RTMP hacia SRS
 * (`rtmp://wd-srs-media-server:1935/live/{streamId}`), exactamente el mismo punto de entrada que
 * antes alimentaba el puente `rtc_to_rtmp` de SRS. El resto del pipeline (POST /api/stream/start
 * jalando ese RTMP hacia Kick) no cambia.
 */
wss.on('connection', (ws, request, streamId) => {
  console.log(`[Ingest - ${streamId}] WebSocket de ingesta conectado.`);

  // Si ya había una ingesta previa para este streamId (reconexión del cliente), se reemplaza.
  stopIngest(streamId);

  const containerName = `ffmpeg-ingest-${streamId}`;
  try {
    execSync(`docker rm -f ${containerName}`, { stdio: 'pipe' });
  } catch (e) {}

  const ffmpegArgs = [
    'run',
    '--name', containerName,
    '--rm',
    '--network', DOCKER_NETWORK,
    '-i',
    'linuxserver/ffmpeg',
    '-f', 'webm',
    '-i', 'pipe:0',
    '-c:v', 'libx264',
    '-pix_fmt', 'yuv420p',
    '-preset', 'ultrafast',
    '-g', '60',
    '-c:a', 'aac',
    '-ar', '44100',
    '-b:a', '128k',
    '-f', 'flv',
    `rtmp://wd-srs-media-server:1935/live/${streamId}`
  ];

  console.log(`[Ingest - ${streamId}] Iniciando contenedor: docker ${ffmpegArgs.join(' ')}`);
  const ingestProcess = spawn('docker', ffmpegArgs, { stdio: ['pipe', 'pipe', 'pipe'] });

  activeIngests.set(streamId, {
    process: ingestProcess,
    containerName,
    startedAt: new Date().toISOString(),
    recentLogs: [],
    status: 'RUNNING',
    exitCode: null
  });

  trackFfmpegProcess(activeIngests, streamId, ingestProcess, `[Ingest - ${streamId}]`);

  ws.on('message', (data, isBinary) => {
    if (!isBinary) return; // ignora mensajes de control/texto, solo interesan los chunks binarios de MediaRecorder
    if (ingestProcess.stdin.writable) {
      ingestProcess.stdin.write(data);
    }
  });

  ws.on('close', (code) => {
    console.log(`[Ingest - ${streamId}] WebSocket de ingesta cerrado (code=${code}).`);
    stopIngest(streamId);
  });

  ws.on('error', (err) => {
    console.warn(`[Ingest - ${streamId}] Error en WebSocket de ingesta:`, err.message);
  });
});

// Solo se atienden upgrades de conexión para rutas /ws/ingest/:streamId; cualquier otra se rechaza.
server.on('upgrade', (request, socket, head) => {
  const match = /^\/ws\/ingest\/([^/?]+)/.exec(request.url || '');
  if (!match) {
    socket.destroy();
    return;
  }
  const streamId = match[1];
  wss.handleUpgrade(request, socket, head, (ws) => {
    wss.emit('connection', ws, request, streamId);
  });
});

// Inicialización de la escucha del servidor HTTP (Express + upgrades WebSocket) en el puerto configurado
server.listen(PORT, () => {
  console.log(`Servidor de gestión de FFmpeg escuchando en el puerto ${PORT}`);
});

