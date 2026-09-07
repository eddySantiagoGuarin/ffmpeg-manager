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

// Inicialización de la escucha del servidor HTTP en el puerto configurado
app.listen(PORT, () => {
  console.log(`Servidor de gestión de FFmpeg escuchando en el puerto ${PORT}`);
});

