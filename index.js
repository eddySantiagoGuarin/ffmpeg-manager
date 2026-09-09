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
 * @type {Map<string, {process: ChildProcess, containerName: string, sourceType: string, destinationUrl: string, active: boolean, startedAt: string}>}
 */
const activeStreams = new Map();

/**
 * Almacenamiento en memoria de las ingestas activas por WebSocket (navegador -> ffmpeg-manager -> RTMP a SRS).
 * Independiente de `activeStreams`: esta es la ingesta local (cámara/pantalla), la otra es el reempuje a Kick.
 *
 * @type {Map<string, {process: ChildProcess, containerName: string, startedAt: string}>}
 */
const activeIngests = new Map();

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
 * @route POST /api/stream/start
 * @description Inicia una nueva transmisión FFmpeg dentro de un contenedor Docker dedicado.
 * 
 * Configura los parámetros de entrada según el `sourceType` (cámara, pantalla o fuente sintética `testsrc`)
 * y ejecuta el contenedor `linuxserver/ffmpeg` retransmitiendo el flujo de video y audio en formato FLV/RTMP
 * hacia el servidor de destino especificado (`destinationUrl`).
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

  const containerName = `ffmpeg-stream-${streamId}`;

  // 1. Limpieza preventiva sincrónica de contenedores previos huérfanos con el mismo nombre
  try {
    execSync(`docker rm -f ${containerName}`, { stdio: 'pipe' });
  } catch (e) {
    const errOutput = e.stderr ? e.stderr.toString() : e.message;
    // Captura de fallo de conectividad con Docker Desktop en el anfitrión
    if (errOutput.includes('failed to connect') || errOutput.includes('Is the docker daemon running')) {
      console.error(`[FFmpeg - ${streamId}] Error conectando a Docker Daemon: ${errOutput}`);
      return res.status(500).json({
        error: 'No se pudo conectar con el demonio de Docker (Docker Desktop no está en ejecución)',
        details: errOutput
      });
    }
  }

  // 2. Construcción dinámica de argumentos de origen de FFmpeg según el sourceType
  let inputArgs = [];
  const validSourceType = ['camera', 'screen', 'testsrc'].includes(sourceType) ? sourceType : 'testsrc';

  if (validSourceType === 'camera' || validSourceType === 'screen') {
    // Origen de señal en vivo desde el servidor SRS Media Server
    const srsInputUrl = sourceUrl || `rtmp://wd-srs-media-server:1935/live/${streamId}`;
    inputArgs = ['-re', '-i', srsInputUrl];
  } else {
    // Origen sintético de prueba (testsrc a 1280x720 30fps con tono sine de 1000Hz)
    inputArgs = [
      '-re',
      '-f', 'lavfi', '-i', 'testsrc=size=1280x720:rate=30',
      '-f', 'lavfi', '-i', 'sine=frequency=1000:sample_rate=48000'
    ];
  }

  // 3. Configuración del comando Docker y parámetros de codificación de FFmpeg (H.264 + AAC)
  const ffmpegArgs = [
    'run',
    '--name', containerName,
    '--rm',
    '--network', DOCKER_NETWORK,
    '-i',
    'linuxserver/ffmpeg',
    ...inputArgs,
    '-c:v', 'libx264',
    '-pix_fmt', 'yuv420p',
    '-preset', 'ultrafast',
    '-g', '60',
    '-c:a', 'aac',
    '-ar', '44100',
    '-b:a', '128k',
    '-f', 'flv',
    destinationUrl
  ];

  console.log(`[FFmpeg - ${streamId}] Iniciando contenedor: docker ${ffmpegArgs.join(' ')}`);

  // 4. Invocación asíncrona del proceso contenedor mediante child_process.spawn
  const ffmpegProcess = spawn('docker', ffmpegArgs);

  const streamInfo = {
    process: ffmpegProcess,
    containerName,
    sourceType: validSourceType,
    destinationUrl,
    active: true,
    startedAt: new Date().toISOString()
  };

  // Registrar el flujo de trabajo en el mapa activo
  activeStreams.set(streamId, streamInfo);

  // Registro de trazas de salida de error estándar (stderr) producidas por FFmpeg
  ffmpegProcess.stderr.on('data', (data) => {
    console.error(`[FFmpeg - ${streamId}] ${data.toString()}`);
  });

  // Manejador del evento de cierre de proceso para liberar la memoria del mapa cuando finalice
  ffmpegProcess.on('close', (code) => {
    console.log(`[FFmpeg - ${streamId}] Proceso finalizado con código de salida ${code}`);
    activeStreams.delete(streamId);
  });

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
 * @route GET /api/stream/status/:streamId
 * @description Obtiene el estado detallado de una transmisión activa a partir de su ID en los parámetros de ruta.
 * 
 * @param {express.Request} req - Objeto de solicitud HTTP Express.
 * @param {string} req.params.streamId - ID del evento / transmisión.
 * @param {express.Response} res - Objeto de respuesta HTTP Express.
 * @returns {Object} Estado detallado del proceso FFmpeg (RUNNING o NOT_FOUND).
 */
app.get('/api/stream/status/:streamId', (req, res) => {
  const { streamId } = req.params;
  const streamInfo = activeStreams.get(streamId);

  if (!streamInfo || !streamInfo.active) {
    return res.status(404).json({ streamId, active: false, status: 'NOT_FOUND' });
  }

  return res.status(200).json({
    streamId,
    active: true,
    status: 'RUNNING',
    sourceType: streamInfo.sourceType,
    destinationUrl: streamInfo.destinationUrl,
    startedAt: streamInfo.startedAt
  });
});

/**
 * @route POST /api/stream/status
 * @description Obtiene el estado detallado de una transmisión activa enviando el `streamId` en el cuerpo JSON.
 * 
 * @param {express.Request} req - Objeto de solicitud HTTP Express.
 * @param {string} req.body.streamId - ID del evento / transmisión.
 * @param {express.Response} res - Objeto de respuesta HTTP Express.
 * @returns {Object} Estado detallado del proceso FFmpeg (RUNNING o NOT_FOUND).
 */
app.post('/api/stream/status', (req, res) => {
  const { streamId } = req.body;
  if (!streamId) {
    return res.status(400).json({ error: 'streamId es requerido' });
  }
  const streamInfo = activeStreams.get(streamId);

  if (!streamInfo || !streamInfo.active) {
    return res.status(404).json({ streamId, active: false, status: 'NOT_FOUND' });
  }

  return res.status(200).json({
    streamId,
    active: true,
    status: 'RUNNING',
    sourceType: streamInfo.sourceType,
    destinationUrl: streamInfo.destinationUrl,
    startedAt: streamInfo.startedAt
  });
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
    startedAt: new Date().toISOString()
  });

  ingestProcess.stderr.on('data', (data) => {
    console.error(`[Ingest - ${streamId}] ${data.toString()}`);
  });

  ingestProcess.on('close', (code) => {
    console.log(`[Ingest - ${streamId}] Proceso de ingesta finalizado con código de salida ${code}`);
    activeIngests.delete(streamId);
  });

  ingestProcess.on('error', (err) => {
    console.error(`[Ingest - ${streamId}] No se pudo iniciar el contenedor de ingesta:`, err.message);
  });

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

