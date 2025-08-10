// index.js
import Fastify from 'fastify';
import WebSocket from 'ws';
import dotenv from 'dotenv';
import fastifyFormBody from '@fastify/formbody';
import fastifyWs from '@fastify/websocket';

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

const { OPENAI_API_KEY } = process.env;
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

const VOICE = 'alloy';
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
  'session.created'
];

const SHOW_TIMING_MATH = false;

// =========================
// Definición de ENCUESTA
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
    let markQueue = [];
    let responseStartTimestampTwilio = null;

    // Estado de encuesta por llamada
    let survey = { id: SURVEY.start, answers: {} };

    // Buffer para argumentos de tool calling
    const toolArgsBuffer = new Map();

    // Helpers de encuesta
    function currentNode() { return SURVEY.nodes[survey.id]; }

    // Preguntar al usuario (voz) + habilitar tool calling
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
          // 👇 Debe ser ['audio','text'] si quieres voz
          modalities: ["audio","text"],
          instructions,
          tools: SURVEY_TOOL,
          tool_choice: "auto"
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
        // Mensaje de cierre
        openAiWs.send(JSON.stringify({
          type: "response.create",
          response: {
            modalities: ["audio","text"],
            instructions: SURVEY_SYSTEM + "\nPronuncia este mensaje final y nada más:\n" + SURVEY.nodes.end.prompt
          }
        }));
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

    // Inicializa Realtime (VAD servidor, μ-law) y arranca la encuesta
    const initializeSession = () => {
      const sessionUpdate = {
        type: 'session.update',
        session: {
          turn_detection: { type: 'server_vad' },
          input_audio_format: 'g711_ulaw',
          output_audio_format: 'g711_ulaw',
          voice: VOICE,
          instructions: SURVEY_SYSTEM,
          modalities: ["audio","text"],
          // 👇 Requerido: modelo de transcripción y lenguaje
          input_audio_transcription: { model: "gpt-4o-mini-transcribe", language: "es" },
          // 👇 El preview actual exige >= 0.6
          temperature: 0.6
        }
      };
      console.log('Sending session update:', JSON.stringify(sessionUpdate));
      openAiWs.send(JSON.stringify(sessionUpdate));

      // Arranque de encuesta
      survey = { id: SURVEY.start, answers: {} };
      askQuestion();
    };

    // ----- Barge-in: interrumpe TTS cuando el usuario habla -----
    const handleSpeechStartedEvent = () => {
      if (markQueue.length > 0 && responseStartTimestampTwilio != null) {
        const elapsedTime = latestMediaTimestamp - responseStartTimestampTwilio;
        if (SHOW_TIMING_MATH) {
          console.log(`Calculando truncado: ${latestMediaTimestamp} - ${responseStartTimestampTwilio} = ${elapsedTime}ms`);
        }

        if (lastAssistantItem) {
          const truncateEvent = {
            type: 'conversation.item.truncate',
            item_id: lastAssistantItem,
            content_index: 0,
            audio_end_ms: elapsedTime
          };
          if (SHOW_TIMING_MATH) console.log('Enviando truncate:', JSON.stringify(truncateEvent));
          openAiWs.send(JSON.stringify(truncateEvent));
        }

        // Limpia el buffer de audio pendiente en Twilio
        connection.send(JSON.stringify({ event: 'clear', streamSid }));
        // Reset
        markQueue = [];
        lastAssistantItem = null;
        responseStartTimestampTwilio = null;
      }
    };

    // Marca para saber cuándo Twilio terminó de reproducir el chunk
    const sendMark = () => {
      if (streamSid) {
        connection.send(JSON.stringify({
          event: 'mark',
          streamSid,
          mark: { name: 'responsePart' }
        }));
        markQueue.push('responsePart');
      }
    };

    // ----- Eventos OpenAI WS -----
    openAiWs.on('open', () => {
      console.log('Connected to the OpenAI Realtime API');
      setTimeout(initializeSession, 100);
    });

    openAiWs.on('message', (data) => {
      try {
        const response = JSON.parse(data);

        if (LOG_EVENT_TYPES.includes(response.type)) {
          console.log(`Received event: ${response.type}`, response);
        }

        // Audio TTS del modelo → Twilio (μ-law base64)
        if (response.type === 'response.audio.delta' && response.delta) {
          const audioDelta = {
            event: 'media',
            streamSid,
            media: { payload: response.delta }
          };
          connection.send(JSON.stringify(audioDelta));

          // Primer chunk de una nueva respuesta: referencia de tiempo
          if (!responseStartTimestampTwilio) {
            responseStartTimestampTwilio = latestMediaTimestamp;
            if (SHOW_TIMING_MATH) console.log(`Start timestamp nueva respuesta: ${responseStartTimestampTwilio}ms`);
          }

          if (response.item_id) {
            lastAssistantItem = response.item_id;
          }

          sendMark();
        }

        // Barge-in (usuario empezó a hablar)
        if (response.type === 'input_audio_buffer.speech_started') {
          handleSpeechStartedEvent();
        }

        // Tool calling: acumula y procesa
        if (response.type === "response.function_call.arguments.delta") {
          const id = response.call_id;
          const prev = toolArgsBuffer.get(id) || "";
          toolArgsBuffer.set(id, prev + (response.delta || ""));
        }

        if (response.type === "response.function_call.arguments.done") {
          const id = response.call_id;
          const full = toolArgsBuffer.get(id) || "{}";
          toolArgsBuffer.delete(id);

          let args = {};
          try { args = JSON.parse(full); } catch { /* noop */ }
          applyAnswer(args);
        }

      } catch (error) {
        console.error('Error processing OpenAI message:', error, 'Raw message:', data);
      }
    });

    openAiWs.on('close', () => {
      console.log('Disconnected from the OpenAI Realtime API');
    });

    openAiWs.on('error', (error) => {
      console.error('Error in the OpenAI WebSocket:', error);
    });

    // ----- Eventos Twilio WS -----
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
            if (markQueue.length > 0) markQueue.shift();
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
