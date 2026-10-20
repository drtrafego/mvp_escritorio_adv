import { createServer } from "node:http";
import { readFile, writeFile, mkdir } from "node:fs/promises";
import { existsSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { initialState } from "./prototipo/src/model.mjs";
import { processTelegramCommand, processTelegramCallback } from "./adaptadores/telegram-bot-bridge.mjs";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const PORT = process.env.PORT || 3000;
const GABINETE_API_URL = process.env.GABINETE_API_URL || "";
const DATA_DIR = path.join(__dirname, "data");
const STATE_FILE = path.join(DATA_DIR, "office-state.json");

const MIME_TYPES = {
  ".html": "text/html; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".js": "application/javascript; charset=utf-8",
  ".mjs": "application/javascript; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".png": "image/png",
  ".svg": "image/svg+xml",
  ".txt": "text/plain; charset=utf-8",
};

// Gerenciamento de Clientes SSE para sincronização em tempo real
const sseClients = new Set();

function broadcastSse(eventType, data) {
  const payload = `event: ${eventType}\ndata: ${JSON.stringify(data)}\n\n`;
  for (const client of sseClients) {
    try {
      client.write(payload);
    } catch {
      sseClients.delete(client);
    }
  }
}

// Inicializa armazenamento em disco se necessário
async function loadOrInitState() {
  if (!existsSync(DATA_DIR)) {
    await mkdir(DATA_DIR, { recursive: true });
  }
  if (existsSync(STATE_FILE)) {
    try {
      const raw = await readFile(STATE_FILE, "utf8");
      return JSON.parse(raw);
    } catch (e) {
      console.warn("[Server] Aviso: estado local corrompido, reinicializando...", e.message);
    }
  }
  const fresh = initialState();
  await writeFile(STATE_FILE, JSON.stringify(fresh, null, 2), "utf8");
  return fresh;
}

async function saveState(state) {
  if (!existsSync(DATA_DIR)) {
    await mkdir(DATA_DIR, { recursive: true });
  }
  await writeFile(STATE_FILE, JSON.stringify(state, null, 2), "utf8");
  broadcastSse("state_update", { updatedAt: new Date().toISOString() });
}

const server = createServer(async (req, res) => {
  const url = new URL(req.url, `http://${req.headers.host}`);
  const pathname = url.pathname;

  // 1. Server-Sent Events (SSE) para atualização em tempo real
  if (pathname === "/api/office/stream" || pathname === "/api/office/events") {
    res.writeHead(200, {
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache, no-transform",
      Connection: "keep-alive",
      "Access-Control-Allow-Origin": "*",
    });
    res.write("retry: 3000\n\n");
    res.write(`event: connected\ndata: ${JSON.stringify({ status: "connected", time: new Date().toISOString() })}\n\n`);
    sseClients.add(res);

    req.on("close", () => {
      sseClients.delete(res);
    });
    return;
  }

  // 2. Leitura e Gravação do Estado Persistente do Escritório
  if (pathname === "/api/office/state") {
    if (req.method === "GET") {
      const state = await loadOrInitState();
      res.writeHead(200, { "Content-Type": "application/json", "Cache-Control": "no-store" });
      return res.end(JSON.stringify(state));
    }

    if (req.method === "POST") {
      let body = "";
      req.on("data", chunk => (body += chunk));
      req.on("end", async () => {
        try {
          const parsed = JSON.parse(body);
          await saveState(parsed);
          res.writeHead(200, { "Content-Type": "application/json" });
          return res.end(JSON.stringify({ ok: true, savedAt: new Date().toISOString() }));
        } catch (err) {
          res.writeHead(400, { "Content-Type": "application/json" });
          return res.end(JSON.stringify({ error: "Payload inválido", details: err.message }));
        }
      });
      return;
    }
  }

  // 3. Execução de Comandos do Telegram / Ações Rápidas
  if (pathname === "/api/office/command" && req.method === "POST") {
    let body = "";
    req.on("data", chunk => (body += chunk));
    req.on("end", async () => {
      try {
        const { cmd, user } = JSON.parse(body);
        const result = await processTelegramCommand(cmd, user || "Dr. Daniel Felix");
        broadcastSse("telegram_message", { cmd, result });
        res.writeHead(200, { "Content-Type": "application/json" });
        return res.end(JSON.stringify(result));
      } catch (err) {
        res.writeHead(500, { "Content-Type": "application/json" });
        return res.end(JSON.stringify({ error: err.message }));
      }
    });
    return;
  }

  // 4. Ações Pontuais do Advogado (Aprovar Petição, Confirmar Prazo, Dar Baixa, Outbox)
  if (pathname === "/api/office/action" && req.method === "POST") {
    let body = "";
    req.on("data", chunk => (body += chunk));
    req.on("end", async () => {
      try {
        const { action, payload, user } = JSON.parse(body);
        const state = await loadOrInitState();

        if (action === "approve_petition") {
          const pet = state.petitions?.find(p => p.id === payload.id);
          if (pet) {
            pet.status = "approved";
            pet.statusLabel = "Aprovada por Dr. Daniel Felix · Pronta p/ Protocolo";
            pet.approvedBy = user || "Dr. Daniel Felix";
            pet.approvedAt = new Intl.DateTimeFormat("pt-BR", { day: "2-digit", month: "2-digit", year: "numeric", hour: "2-digit", minute: "2-digit" }).format(new Date());
          }
        } else if (action === "confirm_deadline") {
          const d = state.deadlinesList?.find(x => x.id === payload.id);
          if (d) {
            d.status = "Confirmado por Dr. Daniel Felix";
            d.confirmedBy = user || "Dr. Daniel Felix";
          }
        } else if (action === "pay_financeiro") {
          const item = state.financeiro?.items?.find(x => x.id === payload.id);
          if (item) {
            item.status = "paid";
            item.paidAt = new Intl.DateTimeFormat("pt-BR", { day: "2-digit", month: "2-digit", year: "numeric" }).format(new Date());
            const cli = state.clients?.find(c => c.id === item.clientId);
            if (cli) cli.honorariosPaid = (cli.honorariosPaid || 0) + Number(item.amount || 0);
          }
        } else if (action === "send_outbox") {
          const o = state.financeiro?.outbox?.find(x => x.id === payload.id);
          if (o) o.status = "sent";
        }

        await saveState(state);
        res.writeHead(200, { "Content-Type": "application/json" });
        return res.end(JSON.stringify({ ok: true, action }));
      } catch (err) {
        res.writeHead(500, { "Content-Type": "application/json" });
        return res.end(JSON.stringify({ error: err.message }));
      }
    });
    return;
  }

  // 5. Endpoint de Compatibilidade com o Gabinete do MVP
  if (pathname === "/api/escritorio/estado") {
    if (GABINETE_API_URL) {
      try {
        const remoteRes = await fetch(`${GABINETE_API_URL}/api/escritorio/estado`, {
          headers: { Cookie: req.headers.cookie || "" },
        });
        const data = await remoteRes.json();
        res.writeHead(remoteRes.status, { "Content-Type": "application/json" });
        return res.end(JSON.stringify(data));
      } catch (err) {
        res.writeHead(503, { "Content-Type": "application/json" });
        return res.end(JSON.stringify({ error: "Gabinete remoto indisponível", details: err.message }));
      }
    }

    const state = await loadOrInitState();
    const estadoLocal = {
      schemaVersion: 1,
      mode: "live",
      generatedAt: new Date().toISOString(),
      office: "Daniel Felix",
      summary: {
        totalProcessos: state.processesList?.length || 128,
        venceEm7Dias: (state.deadlinesList || []).length,
        sugeridos: (state.deadlinesList || []).filter(d => d.status === "Sugestão").length,
        intimacoesSemAnalise: (state.noticesList || []).length,
        clientesSugeridos: (state.clients || []).length,
      },
      metrics: {
        label: "Escritório Daniel Felix",
        processes: state.processesList?.length || 128,
        deadlines: (state.deadlinesList || []).length,
        leads: (state.clients || []).length * 4,
        contracts: (state.contractsList || []).length,
        petitions: (state.petitions || []).length,
        received: (state.financeiro?.items || []).filter(x => x.status === "paid").reduce((s, x) => s + Number(x.amount || 0), 0),
        receivable: (state.financeiro?.items || []).filter(x => x.status !== "paid").reduce((s, x) => s + Number(x.amount || 0), 0),
        series: [12, 19, 15, 23, 28, 24, 30],
        funnel: [32, 18, 11, 7],
      },
      processes: state.processesList || [],
      deadlines: state.deadlinesList || [],
      notices: state.noticesList || [],
      health: [
        { fonte: "DataJud", status: "ok", quando: "Hoje às 06:00", novos: 5 },
        { fonte: "DJEN", status: "ok", quando: "Hoje às 07:15", novos: 3 },
      ],
      execution: {
        connected: true,
        worker: "daniel-worker-1",
        queue: "office_jobs",
      },
    };

    res.writeHead(200, { "Content-Type": "application/json", "Cache-Control": "no-store" });
    return res.end(JSON.stringify(estadoLocal));
  }

  // 6. Roteamento de Arquivos Estáticos
  let filePath = pathname === "/" ? path.join(__dirname, "prototipo", "index.html") : path.join(__dirname, pathname);

  if (!existsSync(filePath) && existsSync(path.join(__dirname, "prototipo", pathname))) {
    filePath = path.join(__dirname, "prototipo", pathname);
  }

  if (existsSync(filePath)) {
    try {
      const ext = path.extname(filePath).toLowerCase();
      const contentType = MIME_TYPES[ext] || "application/octet-stream";
      const content = await readFile(filePath);
      res.writeHead(200, { "Content-Type": contentType });
      return res.end(content);
    } catch {
      res.writeHead(500, { "Content-Type": "text/plain" });
      return res.end("Erro interno ao ler arquivo.");
    }
  }

  res.writeHead(404, { "Content-Type": "text/plain" });
  res.end("404 Não Encontrado");
});

server.listen(PORT, async () => {
  await loadOrInitState();
  console.log(`\n========================================================`);
  console.log(`🏢 ESCRITÓRIO VIRTUAL DANIEL FELIX (Persistente + SSE)`);
  console.log(`📍 Servidor rodando em: http://localhost:${PORT}`);
  console.log(`========================================================\n`);
});
