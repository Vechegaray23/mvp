// index.js
import Fastify from 'fastify';
import WebSocket from 'ws';
import dotenv from 'dotenv';
import fastifyFormBody from '@fastify/formbody';
import fastifyWs from '@fastify/websocket';
import fs from 'fs'; // <- agregado: uso síncrono para el dump opcional

// =========================
// Carga de entorno
// =========================
dotenv.config();
const envResult = dotenv.config({ debug: true, override: true });
console.log('dotenv.config() →', envResult);
console.log(
  'API key cargada →',
  process.env.OPENAI_API_KEY?.slice(0,10),
  '…len=',
  process.env.OPENAI_API_KEY?.length
);

const { OPENAI_API_KEY, OPENAI_VOICE, DEBUG_AUDIO_DUMP } = process.env;
if (!OPENAI_API_KEY) {
  console.error('Missing OpenAI API key. Please set it in the .env file.');
  process.exit(1);
}

// =========================
// Config servidor
// =========================
const fastify = Fastify();
fastify.register(fastifyFormBody);
fastify.register(fastifyWs);

// Voz (permitir cambiar por .env)
const VOICE = OPENAI_VOICE || 'alloy';
const PORT = process.env.PORT || 5050;

// =========================
// Logging (opcional)
// =========================
const LOG_EVENT_TYPES = [
  'error',
  'response.content.done',
  'rate_limits.updated',
  'response.done',
  'input_audio_buffer.committed',
  'input_audio_buffer.speech_stopped',
  'input_audio_buffer.speech_started',
  'session.created',
  'session.updated'
];

const SHOW_TIMING_MATH = false;

// =========================
// Definición de ENCUESTA (ajusta a tu guion)
// =========================
const SURVEY = {
  start: "consent",
  nodes: {
    consent: {
      prompt: "Para mejorar el servicio hacemos una encuesta breve de 3 minutos. ¿Aceptas participar? Responde sí o no.",
      expect: "yesno",
      next: { yes: "q1", no: "end" }
    },
    q1: {
      prompt: "En una escala del 1 al 5, ¿qué tan satisfecho estás con nuestro servicio? Responde solo un número.",
      expect: "number", range: [1,5],
      next: "q2"
    },
    q2: {
      prompt: "¿Cuál fue el motivo principal de tu calificación? Responde en una frase.",
      expect: "text",
      next: "q3"
    },
    q3: {
      prompt: "¿Nos recomendarías a un amigo o colega? Responde sí o no.",
      expect: "yesno",
      next: "end"
    },
    end: {
      prompt: "Gracias por responder. ¡Que tengas un buen día!",
      expect: "none"
    }
  }
};

// Herramienta para normalizar y avanzar
const SURVEY_TOOL = [{
  type: "function",
  name: "report_answer",
  description: "Reporta la respuesta del usuario a la pregunta actual y el próximo id.",
  parameters: {
    type: "object",
    properties: {
      question_id: { type: "string" },
      raw_text:    { type: "string", description: "Transcripción literal de la respuesta." },
      normalized:  {
        type: "object",
        properties: {
          yesno:  { type: ["string","null"], enum: ["yes","no",null] },
          number: { type: ["number","null"] },
          text:   { type: ["string","null"] }
        },
        additionalProperties: false
      },
      next_id:    { type: "string" },
      confidence: { type: "number" }
    },
    required: ["question_id", "next_id"]
  }
}];

// Instrucciones estrictas para modo encuesta
const SURVEY_SYSTEM = [
  "Eres un encuestador de voz. Sigue estrictamente el guion.",
  "En cada turno: 1) pronuncia SOLO la pregunta actual, 2) escucha, 3) si hay respuesta válida, llama a la herramienta report_answer con question_id, raw_text, normalized y next_id.",
  "No pronuncies JSON ni expliques el proceso. Si la respuesta es inválida, repregunta brevemente y vuelve a intentar.",
  "Lenguaje claro y neutro; sin chistes; no improvises."
].join("\n");

// =========================
// Rutas HTTP
// =========================
fastify.get('/', async (request, reply) => {
  reply.send({ message: 'Twilio Media Stream Server is running!' });
});

fastify.all('/incoming-call', async (request, reply) => {
  const twimlResponse = `<?xml version="1.0" encoding="UTF-8"?>
    <Response>
      <Connect>
        <Stream url="wss://${request.headers.host}/media-stream" />
      </Connect>
    </Response>`;
  reply.type('text/xml').send(twimlResponse);
});

// =========================
// WebSocket de Media Stream
// =========================
fastify.register(async (fastify) => {
  fastify.get('/media-stream', { websocket: true }, (connection, req) => {
    console.log('Client connected');

    // ----- Estado por conexión -----
    let streamSid = null;
    let latestMediaTimestamp = 0;
    let lastAssistantItem = null;
    let responseStartTimestampTwilio = null;

    // Estado de TTS en vuelo (para barge-in) y formato
    let ttsInFlight = false;
    let currentResponseId = null;
    let formatReady = false;         // ← clave para evitar ruido

    // Estado de encuesta por llamada
    let survey = { id: SURVEY.start, answers: {} };

    // Flags de sesión
    let sessionReady = false;
    let surveyStarted = false;
    let healthcheckDone = false;

    // Buffers para tool-calling y texto
    const toolArgsBuffer = new Map();
    const asrBuffer = [];                       // transcripción usuario (entrada)
    const modelTextBuffers = new Map();         // texto modelo por response_id

    // Helpers de encuesta
    function currentNode() { return SURVEY.nodes[survey.id]; }

    // Pregunta al usuario (voz) + habilita tool calling
    function askQuestion() {
      const node = currentNode();
      const controlFrame = {
        question_id: survey.id,
        expect: node.expect,
        range: node.range || null,
        next_map: node.next || null
      };

      const instructions = [
        SURVEY_SYSTEM,
        "Control de la pregunta actual (NO lo pronuncies):",
        JSON.stringify(controlFrame),
        "Pronuncia exactamente la siguiente pregunta y nada más:",
        node.prompt
      ].join("\n");

      const msg = {
        type: "response.create",
        response: {
          modalities: ["audio","text"],
          instructions,
          tools: SURVEY_TOOL,
          tool_choice: "auto",
          conversation: "none" // ignora historial cada turno
        }
      };
      ttsInFlight = false;           // se activará al primer delta
      currentResponseId = null;
      responseStartTimestampTwilio = null;
      if (SHOW_TIMING_MATH) console.log('→ response.create (askQuestion) enviado');
      openAiWs.send(JSON.stringify(msg));
    }

    // Healthcheck de audio (solo una vez, cuando formatReady==true)
    function playHealthcheck() {
      if (healthcheckDone) return;
      healthcheckDone = true;
      const msg = {
        type: "response.create",
        response: {
          modalities: ["audio","text"],
          conversation: "none",
          instructions: "Pronuncia exactamente: 'Prueba de audio en mu-law ocho kilohercios'."
        }
      };
      openAiWs.send(JSON.stringify(msg));
    }

    // Aplicar respuesta y avanzar
    function applyAnswer(args = {}) {
      const qid = args.question_id || survey.id;
      survey.answers[qid] = {
        raw_text: args.raw_text || null,
        normalized: args.normalized || {},
        confidence: typeof args.confidence === "number" ? args.confidence : null
      };

      // Siguiente id
      let next = args.next_id;
      const node = SURVEY.nodes[qid];
      if (!next) {
        if (node?.expect === "yesno" && node.next && args.normalized?.yesno) {
          next = node.next[ args.normalized.yesno ] || "end";
        } else if (node?.next) {
          next = node.next;
        } else {
          next = "end";
        }
      }

      survey.id = next;

      if (survey.id !== "end") {
        askQuestion();
      } else {
        // Mensaje de cierre (turno final)
        const finalMsg = {
          type: "response.create",
          response: {
            modalities: ["audio","text"],
            instructions: SURVEY_SYSTEM + "\nPronuncia este mensaje final y nada más:\n" + SURVEY.nodes.end.prompt,
            conversation: "none"
          }
        };
        openAiWs.send(JSON.stringify(finalMsg));
        // Persistencia (reemplaza por tu DB / webhook)
        console.log("RESPUESTAS ENCUESTA:", JSON.stringify(survey.answers, null, 2));
      }
    }

    // ----- Conexión a OpenAI Realtime -----
    const openAiWs = new WebSocket('wss://api.openai.com/v1/realtime?model=gpt-4o-mini-realtime-preview', {
      headers: {
        Authorization: `Bearer ${OPENAI_API_KEY}`,
        "OpenAI-Beta": "realtime=v1"
      }
    });

    // Inicializa Realtime (VAD servidor, μ-law). Empezar encuesta SOLO tras ack + formato OK.
    const sendSessionUpdate = () => {
      const sessionUpdate = {
        type: 'session.update',
        session: {
          turn_detection: {
            type: 'server_vad',
            // Parámetros de VAD para fluidez
            silence_duration_ms: 200,
            prefix_padding_ms: 200,
            interrupt_response: true,
            create_response: true
          },
          input_audio_format: 'g711_ulaw',
          output_audio_format: 'g711_ulaw',
          voice: VOICE,
          instructions: SURVEY_SYSTEM,
          modalities: ["audio","text"],
          input_audio_transcription: { model: "gpt-4o-mini-transcribe", language: "es" },
          temperature: 0.6
        }
      };
      console.log('Sending session update:', JSON.stringify(sessionUpdate));
      openAiWs.send(JSON.stringify(sessionUpdate));
    };

    // ----- Barge-in: interrumpe TTS cuando el usuario habla -----
    const handleSpeechStartedEvent = () => {
      if (!ttsInFlight) return;
      if (SHOW_TIMING_MATH) console.log('BARGE-IN: speech_started → clear + truncate');

      if (lastAssistantItem && responseStartTimestampTwilio != null) {
        const elapsedTime = latestMediaTimestamp - responseStartTimestampTwilio;
        const truncateEvent = {
          type: 'conversation.item.truncate',
          item_id: lastAssistantItem,
          content_index: 0,
          audio_end_ms: Math.max(0, elapsedTime)
        };
        openAiWs.send(JSON.stringify(truncateEvent));
      }

      if (streamSid) {
        connection.send(JSON.stringify({ event: 'clear', streamSid }));
      }

      ttsInFlight = false;
      lastAssistantItem = null;
      responseStartTimestampTwilio = null;
    };

    // ====== OPENAI WS ======
    openAiWs.on('open', () => {
      console.log('Connected to the OpenAI Realtime API');
      setTimeout(sendSessionUpdate, 100);
    });

    openAiWs.on('message', (raw) => {
      try {
        const ev = JSON.parse(raw);

        if (LOG_EVENT_TYPES.includes(ev.type)) {
          console.log(`Received event: ${ev.type}`, ev);
        }

        // Ack de sesión: detectar formatos efectivos y voz, y habilitar flujo
        if (ev.type === 'session.created' || ev.type === 'session.updated') {
          const s = ev.session || {};
          const outFmt = s.output_audio_format || s?.output_audio?.format;
          const inFmt  = s.input_audio_format  || s?.input_audio?.format;
          console.log('SESSION EFFECTIVE FORMATS → in:', inFmt, 'out:', outFmt);
          if (s.voice) console.log('VOICE EFFECTIVE →', s.voice);
          sessionReady = true;
          formatReady = (outFmt === 'g711_ulaw');

          // Healthcheck primero, luego encuesta
          if (formatReady && !healthcheckDone) {
            playHealthcheck();
          }

          if (formatReady && !surveyStarted) {
            surveyStarted = true;
            survey = { id: SURVEY.start, answers: {} };
            askQuestion();
          }
        }

        // Audio del modelo → Twilio (μ-law base64) SOLO si formato confirmado
        if (ev.type === 'response.audio.delta' && ev.delta) {
          if (!formatReady) {
            console.warn('Saltando frame de audio: formato aún no confirmado como g711_ulaw.');
          } else if (streamSid) {
            // Dump opcional del primer chunk para debug
            if (DEBUG_AUDIO_DUMP && !globalThis.__dumpedFirstULaw) {
              try {
                const rawBuf = Buffer.from(ev.delta, 'base64');
                fs.writeFileSync('./debug_first_chunk.ul', rawBuf);
                console.log('Dumped first μ-law chunk to debug_first_chunk.ul');
                globalThis.__dumpedFirstULaw = true;
              } catch {}
            }

            const audioDelta = {
              event: 'media',
              streamSid,
              media: { payload: ev.delta } // μ-law 8k base64
            };
            connection.send(JSON.stringify(audioDelta));
          }

          if (!responseStartTimestampTwilio) {
            responseStartTimestampTwilio = latestMediaTimestamp;
            if (SHOW_TIMING_MATH) console.log(`Start timestamp nueva respuesta: ${responseStartTimestampTwilio}ms`);
          }
          if (ev.item_id) lastAssistantItem = ev.item_id;
          ttsInFlight = true;

          currentResponseId = currentResponseId || ev.response_id || ev.item_id || 'resp';
        }

        // FIN de la salida de audio del modelo → enviar un 'mark' a Twilio (una sola vez)
        if (
          ev.type === 'response.audio.done' ||
          ev.type === 'response.output_audio.done' ||
          ev.type === 'response.done'
        ) {
          if (streamSid && currentResponseId) {
            connection.send(JSON.stringify({
              event: 'mark',
              streamSid,
              mark: { name: `end_${currentResponseId}` }
            }));
          }
          ttsInFlight = false;
          currentResponseId = null;
        }

        // Barge-in (usuario empezó a hablar)
        if (ev.type === 'input_audio_buffer.speech_started') {
          handleSpeechStartedEvent();
        }

        // Tool calling: acumula y procesa
        if (ev.type === "response.function_call.arguments.delta") {
          const id = ev.call_id;
          const prev = toolArgsBuffer.get(id) || "";
          toolArgsBuffer.set(id, prev + (ev.delta || ""));
        }
        if (ev.type === "response.function_call.arguments.done") {
          const id = ev.call_id;
          const full = toolArgsBuffer.get(id) || "{}";
          toolArgsBuffer.delete(id);
          let args = {};
          try { args = JSON.parse(full); } catch { /* noop */ }
          applyAnswer(args);
        }

        // ===== Transcripciones (ASR) =====
        if (ev.type && ev.type.startsWith('input_audio_transcription')) {
          const piece = ev.transcript || ev.text || ev.delta || '';
          if (piece) asrBuffer.push(piece);
          if (ev.type.endsWith('completed') || ev.type.endsWith('done')) {
            const finalAsr = asrBuffer.join('');
            asrBuffer.length = 0;
            console.log('ASR (usuario):', finalAsr);
          }
        }

        // ===== Texto del modelo (además del audio) =====
        if (ev.type === 'response.output_text.delta') {
          const id = ev.response_id || 'default';
          const prev = modelTextBuffers.get(id) || '';
          modelTextBuffers.set(id, prev + (ev.delta || ''));
        }
        if (ev.type === 'response.output_text.done') {
          const id = ev.response_id || 'default';
          const txt = modelTextBuffers.get(id) || '';
          modelTextBuffers.delete(id);
          console.log('MODEL TEXT:', txt);
        }

      } catch (error) {
        console.error('Error processing OpenAI message:', error, 'Raw:', raw);
      }
    });

    openAiWs.on('close', () => {
      console.log('Disconnected from the OpenAI Realtime API');
    });

    openAiWs.on('error', (error) => {
      console.error('Error in the OpenAI WebSocket:', error);
    });

    // ====== TWILIO WS ======
    connection.on('message', (message) => {
      try {
        const data = JSON.parse(message);

        switch (data.event) {
          case 'media':
            latestMediaTimestamp = data.media.timestamp;
            if (SHOW_TIMING_MATH) console.log(`Media ts: ${latestMediaTimestamp}ms`);
            if (openAiWs.readyState === WebSocket.OPEN) {
              openAiWs.send(JSON.stringify({
                type: 'input_audio_buffer.append',
                audio: data.media.payload
              }));
            }
            break;

          case 'start':
            streamSid = data.start.streamSid;
            console.log('Incoming stream has started', streamSid);
            // Reset tiempos
            responseStartTimestampTwilio = null;
            latestMediaTimestamp = 0;
            break;

          case 'mark':
            console.log('Twilio mark reached:', data.mark?.name);
            break;

          default:
            console.log('Received non-media event:', data.event);
            break;
        }
      } catch (error) {
        console.error('Error parsing message:', error, 'Message:', message);
      }
    });

    connection.on('close', () => {
      if (openAiWs.readyState === WebSocket.OPEN) openAiWs.close();
      console.log('Client disconnected.');
    });
  });
});



// =========================
// Arranque del servidor
// =========================

fastify.listen({ port: PORT, host: '0.0.0.0' }, (err) => {
    if (err) {
        console.error(err);
        process.exit(1);
    }
    console.log(`Server is listening on port ${PORT}`);
});
