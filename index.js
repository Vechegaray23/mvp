// index.js
// Twilio Media Streams <-> OpenAI Realtime (Speech-to-Speech) con transcripciones completas
// Ejecuta: node index.js
// Requisitos: Node 18+, @fastify/websocket, @fastify/formbody, ws, dotenv

import Fastify from 'fastify';
import fastifyWs from '@fastify/websocket';
import fastifyFormBody from '@fastify/formbody';
import WebSocket from 'ws';
import dotenv from 'dotenv';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

// Carga .env
dotenv.config();
const envResult = dotenv.config({ debug: true, override: true });
console.log('dotenv.config() →', envResult);
console.log(
  'API key cargada →',
  process.env.OPENAI_API_KEY?.slice(0, 10),
  '…len=',
  process.env.OPENAI_API_KEY?.length
);

// Variables de entorno
const { OPENAI_API_KEY } = process.env;
const PORT = process.env.PORT || 5050;

if (!OPENAI_API_KEY) {
  console.error('Missing OPENAI_API_KEY. Defínela en .env');
  process.exit(1);
}

// __dirname en ESM
const __dirname = path.dirname(fileURLToPath(import.meta.url));

// Carpeta para transcripciones
const TRANSCRIPTS_DIR = path.join(__dirname, 'transcripts');
if (!fs.existsSync(TRANSCRIPTS_DIR)) fs.mkdirSync(TRANSCRIPTS_DIR, { recursive: true });

// Utilidades
function nowIso() { return new Date().toISOString(); }

// Inicializa Fastify
const fastify = Fastify();
fastify.register(fastifyFormBody);
fastify.register(fastifyWs);

// Configuración del asistente
const VOICE = 'alloy'; // usa un slug válido de voz
const SYSTEM_MESSAGE =
  'Eres un asistente que siempre habla español de Chile, claro y directo. ' +
  'Haz solo una pregunta a la vez y sigue el flujo definido por el sistema. ' +
  'Evita bromas si el usuario no las solicita.';

// Eventos a loguear (útil para depurar)
const LOG_EVENT_TYPES = [
  'error',
  'rate_limits.updated',
  'session.created',
  'response.done',
  'response.content.done',
  'input_audio_buffer.speech_started',
  'input_audio_buffer.speech_stopped',
  'input_audio_buffer.committed',
  // Transcripción usuario
  'conversation.item.audio_transcription.completed',
  'conversation.item.input_audio_transcription.completed',
  // Transcripción asistente (audio y texto)
  'response.audio_transcript.delta',
  'response.audio_transcript.done',
  'response.text.delta',
  'response.text.done',
  'response.output_text.delta',
  'response.output_text.done'
];

// Opcional: mostrar cálculos de timing para truncation
const SHOW_TIMING_MATH = false;

// Root
fastify.get('/', async (_req, reply) => {
  reply.send({ message: 'Twilio Media Stream Server is running!' });
});

// TwiML para llamadas entrantes (apunta tu número Twilio a esta ruta)
fastify.all('/incoming-call', async (request, reply) => {
  const wsHost = request.headers['x-forwarded-host'] || request.headers.host;
  const wsUrl = `wss://${wsHost}/media-stream`;

  const twimlResponse = `<?xml version="1.0" encoding="UTF-8"?>
<Response>
  <Connect>
    <Stream url="${wsUrl}" />
  </Connect>
</Response>`;

  reply.type('text/xml').send(twimlResponse);
});

// WebSocket de Media Streams (Twilio)
fastify.register(async (fastify) => {
  fastify.get('/media-stream', { websocket: true }, (connection, req) => {
    console.log('Twilio client connected');

    // Estado por conexión
    let streamSid = null;
    let callSid = null;

    let latestMediaTimestamp = 0;
    let lastAssistantItem = null; // para conversation.item.truncate
    let markQueue = [];
    let responseStartTimestampTwilio = null;

    // Acumuladores para transcripción completa
    const transcript = []; // [{ts, role, text, source, ids...}]
    const assistantOutText = new Map(); // response_id -> texto (fallback)
    const assistantOutAudioTranscript = new Map(); // response_id -> transcript alineado al audio

    const pushLine = (role, text, source, ids = {}) => {
      if (!text) return;
      const line = { ts: nowIso(), role, text, source, ...ids };
      transcript.push(line);
      console.log(`[TRANSCRIPT][${role}]`, text);
      // Persistencia incremental opcional:
      persistTranscriptIfReady();
    };

    const persistTranscriptIfReady = () => {
      if (!callSid) return;
      const base = path.join(TRANSCRIPTS_DIR, `${callSid}`);
      try {
        const jsonl = transcript.map(o => JSON.stringify(o)).join('\n') + '\n';
        fs.writeFileSync(`${base}.jsonl`, jsonl, 'utf8');

        const txt = transcript
          .map(o => `[${o.ts}] ${o.role.toUpperCase()}: ${o.text}`)
          .join('\n') + '\n';
        fs.writeFileSync(`${base}.txt`, txt, 'utf8');

        // Comentario de estado:
        // console.log(`[TRANSCRIPT] actualizado → ${base}.txt / .jsonl`);
      } catch (e) {
        console.error('Error escribiendo transcript:', e);
      }
    };

    // Conexión a OpenAI Realtime
    const openAiWs = new WebSocket(
      'wss://api.openai.com/v1/realtime?model=gpt-4o-mini-realtime-preview',
      {
        headers: {
          Authorization: `Bearer ${OPENAI_API_KEY}`,
          'OpenAI-Beta': 'realtime=v1'
        }
      }
    );

    // Configuración inicial de sesión Realtime
    const initializeSession = () => {
      const sessionUpdate = {
        type: 'session.update',
        session: {
          // VAD servidor para turn-taking bajo en latencia
          turn_detection: { type: 'server_vad', prefix_padding_ms: 300, silence_duration_ms: 200 },
          // MUY IMPORTANTE: alinear códec con Twilio (μ-law 8k)
          input_audio_format: 'g711_ulaw',
          output_audio_format: 'g711_ulaw',
          // Voz del asistente
          voice: VOICE,
          // Instrucciones del sistema
          instructions: SYSTEM_MESSAGE,
          // Modalidades
          modalities: ['text', 'audio'],
          temperature: 0.8,
          // Activar transcripción de ENTRADA (usuario)
          input_audio_transcription: {
            model: 'gpt-4o-mini-transcribe', // o 'whisper-1' / 'gpt-4o-transcribe'
            language: 'es' // ayuda al reconocimiento en español
          }
        }
      };

      console.log('Sending session update:', JSON.stringify(sessionUpdate));
      if (openAiWs.readyState === WebSocket.OPEN) {
        openAiWs.send(JSON.stringify(sessionUpdate));
      }

      // Si quieres que la IA hable primero, descomenta:
      // sendInitialConversationItem();
    };

    // Opcional: IA habla primero
    const sendInitialConversationItem = () => {
      const initialConversationItem = {
        type: 'conversation.item.create',
        item: {
          type: 'message',
          role: 'user',
          content: [{ type: 'input_text', text: 'Hola, necesito ayuda.' }]
        }
      };
      if (openAiWs.readyState === WebSocket.OPEN) {
        openAiWs.send(JSON.stringify(initialConversationItem));
        openAiWs.send(JSON.stringify({ type: 'response.create', response: { modalities: ['audio'] } }));
      }
    };

    // Interrupción cuando el usuario empieza a hablar
    const handleSpeechStartedEvent = () => {
      if (markQueue.length > 0 && responseStartTimestampTwilio != null) {
        const elapsedTime = latestMediaTimestamp - responseStartTimestampTwilio;
        if (SHOW_TIMING_MATH) console.log(`elapsed for truncate: ${latestMediaTimestamp} - ${responseStartTimestampTwilio} = ${elapsedTime}ms`);

        if (lastAssistantItem) {
          const truncateEvent = {
            type: 'conversation.item.truncate',
            item_id: lastAssistantItem,
            content_index: 0,
            audio_end_ms: Math.max(0, elapsedTime)
          };
          if (openAiWs.readyState === WebSocket.OPEN) {
            openAiWs.send(JSON.stringify(truncateEvent));
          }
        }

        if (streamSid) {
          connection.send(JSON.stringify({ event: 'clear', streamSid }));
        }

        // Reset
        markQueue = [];
        lastAssistantItem = null;
        responseStartTimestampTwilio = null;
      }
    };

    // Enviar marcas a Twilio (para saber cuándo terminó un chunk)
    const sendMark = (connection, streamSid) => {
      if (streamSid) {
        const markEvent = { event: 'mark', streamSid, mark: { name: 'responsePart' } };
        connection.send(JSON.stringify(markEvent));
        markQueue.push('responsePart');
      }
    };

    // OpenAI listo
    openAiWs.on('open', () => {
      console.log('Connected to OpenAI Realtime API');
      setTimeout(initializeSession, 100);
    });

    // Mensajes desde OpenAI → reenviar audio a Twilio y capturar transcripciones
    openAiWs.on('message', (data) => {
      try {
        const response = JSON.parse(data);

        if (LOG_EVENT_TYPES.includes(response.type)) {
          console.log(`OpenAI event: ${response.type}`);
        }

        // --- AUDIO DEL ASISTENTE HACIA TWILIO ---
        if (response.type === 'response.audio.delta' && response.delta) {
          if (streamSid) {
            const audioDelta = {
              event: 'media',
              streamSid,
              media: { payload: response.delta } // g711_ulaw base64
            };
            connection.send(JSON.stringify(audioDelta));
          }

          // Primer chunk de un nuevo response → punto de referencia para truncation
          if (!responseStartTimestampTwilio) {
            responseStartTimestampTwilio = latestMediaTimestamp;
            if (SHOW_TIMING_MATH) console.log(`start ts for response: ${responseStartTimestampTwilio}ms`);
          }

          if (response.item_id) {
            lastAssistantItem = response.item_id;
          }

          // Marca para saber cuándo Twilio terminó de reproducir ese segmento
          sendMark(connection, streamSid);
        }

        // --- INTERRUPCIÓN CUANDO EL USUARIO COMIENZA A HABLAR ---
        if (response.type === 'input_audio_buffer.speech_started') {
          handleSpeechStartedEvent();
        }

        // --- TRANSCRIPCIÓN DEL USUARIO ---
        if (
          response.type === 'conversation.item.audio_transcription.completed' ||
          response.type === 'conversation.item.input_audio_transcription.completed'
        ) {
          const text = response.transcript?.text || response.text || response.transcript || '';
          const itemId = response.item_id || response.item?.id;
          pushLine('user', text, response.type, { item_id: itemId });
        }

        // --- TRANSCRIPCIÓN DEL ASISTENTE (alineada al audio) ---
        if (response.type === 'response.audio_transcript.delta') {
          const rid = response.response_id || response.response?.id;
          const prev = assistantOutAudioTranscript.get(rid) || '';
          assistantOutAudioTranscript.set(rid, prev + (response.delta || ''));
        }
        if (response.type === 'response.audio_transcript.done') {
          const rid = response.response_id || response.response?.id;
          const full = assistantOutAudioTranscript.get(rid);
          if (full) pushLine('assistant', full, 'response.audio_transcript.done', { response_id: rid });
        }

        // --- FALLBACK: TEXTO DEL ASISTENTE (por si no llega audio_transcript) ---
        if (response.type === 'response.text.delta' || response.type === 'response.output_text.delta') {
          const rid = response.response_id || response.response?.id;
          const prev = assistantOutText.get(rid) || '';
          assistantOutText.set(rid, prev + (response.delta || ''));
        }
        if (response.type === 'response.text.done' || response.type === 'response.output_text.done') {
          const rid = response.response_id || response.response?.id;
          const full = assistantOutText.get(rid);
          if (full && !assistantOutAudioTranscript.get(rid)) {
            pushLine('assistant', full, response.type, { response_id: rid });
          }
        }

        // Al finalizar una respuesta completa, puedes forzar persistencia
        if (response.type === 'response.done') {
          persistTranscriptIfReady();
        }

      } catch (error) {
        console.error('Error processing OpenAI message:', error, 'Raw message:', data);
      }
    });

    // Mensajes desde Twilio → enviar audio a OpenAI
    connection.on('message', (message) => {
      try {
        const data = JSON.parse(message);

        switch (data.event) {
          case 'media': {
            latestMediaTimestamp = data.media.timestamp;
            if (SHOW_TIMING_MATH) console.log(`media ts: ${latestMediaTimestamp}ms`);
            if (openAiWs.readyState === WebSocket.OPEN) {
              const audioAppend = {
                type: 'input_audio_buffer.append',
                audio: data.media.payload // μ-law 8k base64
              };
              openAiWs.send(JSON.stringify(audioAppend));
            }
            break;
          }

          case 'start': {
            streamSid = data.start.streamSid;
            callSid = data.start.callSid || callSid; // útil para nombrar archivos
            console.log('Incoming stream started', { streamSid, callSid });

            // Reset
            responseStartTimestampTwilio = null;
            latestMediaTimestamp = 0;
            break;
          }

          case 'mark': {
            if (markQueue.length > 0) markQueue.shift();
            break;
          }

          case 'stop': {
            console.log('Twilio stream stopped', { streamSid, callSid });
            // Al final de la llamada guardamos la conversación
            persistTranscriptIfReady();
            break;
          }

          default:
            console.log('Non-media event from Twilio:', data.event);
            break;
        }
      } catch (error) {
        console.error('Error parsing Twilio message:', error, 'Message:', message);
      }
    });

    // Cierre de conexiones
    const safeClose = () => {
      try { if (openAiWs.readyState === WebSocket.OPEN) openAiWs.close(); } catch {}
      try { connection.close(); } catch {}
    };

    connection.on('close', () => {
      console.log('Twilio client disconnected.');
      safeClose();
    });

    openAiWs.on('close', () => {
      console.log('Disconnected from OpenAI Realtime API');
    });

    openAiWs.on('error', (error) => {
      console.error('OpenAI WebSocket error:', error);
      safeClose();
    });
  });
});

// Arranque del servidor
fastify.listen({ port: PORT, host: '0.0.0.0' }, (err, address) => {
  if (err) {
    console.error(err);
    process.exit(1);
  }
  console.log(`Server listening on ${address}`);
  console.log(`POST /incoming-call  → configura tu número Twilio`);
  console.log(`Transcripts folder   → ${TRANSCRIPTS_DIR}`);
});
