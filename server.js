/* Servidor do jogo "Humanos, Aliens e Robôs"
   - Sem dependências: só o Node.js (>= 18).
   - Guarda as salas em memória (e num arquivo rooms.json, para sobreviver a reinícios curtos).
   - Cada jogador envia só o que mudou (patches); o servidor aplica e repassa para a sala. */
const http = require('http');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');

const PORT = process.env.PORT || 8080;
const DATA_FILE = process.env.DATA_FILE || path.join(__dirname, 'rooms.json');
const ALLOWED_ORIGIN = process.env.ALLOWED_ORIGIN || ''; // ex.: https://seuusuario.github.io (vazio = qualquer)
const MAX_MSG = 4 * 1024 * 1024;                          // limite de tamanho de mensagem
const ROOM_TTL = 72 * 3600 * 1000;                        // sala sem atividade por 72h é apagada
const COLORS = ['#ff6b6b', '#4dabf7', '#51cf66', '#ffd43b', '#cc5de8', '#ff922b'];

/* ---------- salas ---------- */
let rooms = {};
const lastActive = {};
try {
  const saved = JSON.parse(fs.readFileSync(DATA_FILE, 'utf8'));
  rooms = saved.rooms || {};
  Object.keys(rooms).forEach(c => { lastActive[c] = Date.now(); });
  console.log(`Salas restauradas: ${Object.keys(rooms).length}`);
} catch (e) { /* primeira execução */ }

let saveTimer = null;
function persist() {
  if (saveTimer) return;
  saveTimer = setTimeout(() => {
    saveTimer = null;
    fs.writeFile(DATA_FILE, JSON.stringify({ rooms }), () => {});
  }, 2000);
}

const isObj = v => v && typeof v === 'object' && !Array.isArray(v);
function applyOps(room, ops) {
  ops.forEach(op => {
    if (!op || !Array.isArray(op.p) || !op.p.length) return;
    if (op.p.some(k => typeof k !== 'string' || k === '__proto__' || k === 'constructor' || k === 'prototype')) return;
    // players e host só mudam por entrar/sair
    if (op.p.length >= 1 && (op.p[0] === 'players' || op.p[0] === 'host')) return;
    let o = room;
    for (let i = 0; i < op.p.length - 1; i++) {
      if (!isObj(o[op.p[i]])) o[op.p[i]] = {};
      o = o[op.p[i]];
    }
    const last = op.p[op.p.length - 1];
    if (op.d) delete o[last]; else o[last] = op.v;
  });
}

/* ---------- WebSocket (RFC 6455, mínimo) ---------- */
const clients = new Set(); // { socket, subs:Set, alive, buf, frag }

function sendFrame(socket, opcode, payload) {
  const len = payload.length;
  let header;
  if (len < 126) header = Buffer.from([0x80 | opcode, len]);
  else if (len < 65536) { header = Buffer.alloc(4); header[0] = 0x80 | opcode; header[1] = 126; header.writeUInt16BE(len, 2); }
  else { header = Buffer.alloc(10); header[0] = 0x80 | opcode; header[1] = 127; header.writeBigUInt64BE(BigInt(len), 2); }
  if (!socket.destroyed) socket.write(Buffer.concat([header, payload]));
}
const sendJson = (c, obj) => sendFrame(c.socket, 0x1, Buffer.from(JSON.stringify(obj)));
function broadcast(code, obj, except) {
  const data = Buffer.from(JSON.stringify(obj));
  clients.forEach(c => { if (c !== except && c.subs.has(code)) sendFrame(c.socket, 0x1, data); });
}

function handleData(c, chunk) {
  c.buf = Buffer.concat([c.buf, chunk]);
  while (c.buf.length >= 2) {
    const b0 = c.buf[0], b1 = c.buf[1];
    const fin = (b0 & 0x80) !== 0, opcode = b0 & 0x0f, masked = (b1 & 0x80) !== 0;
    let len = b1 & 0x7f, off = 2;
    if (len === 126) { if (c.buf.length < 4) return; len = c.buf.readUInt16BE(2); off = 4; }
    else if (len === 127) { if (c.buf.length < 10) return; len = Number(c.buf.readBigUInt64BE(2)); off = 10; }
    if (len > MAX_MSG) return c.socket.destroy();
    if (c.buf.length < off + (masked ? 4 : 0) + len) return;
    let payload;
    if (masked) {
      const mask = c.buf.slice(off, off + 4);
      payload = Buffer.from(c.buf.slice(off + 4, off + 4 + len));
      for (let i = 0; i < payload.length; i++) payload[i] ^= mask[i & 3];
      off += 4;
    } else payload = Buffer.from(c.buf.slice(off, off + len));
    c.buf = c.buf.slice(off + len);

    if (opcode === 0x8) { sendFrame(c.socket, 0x8, Buffer.alloc(0)); return c.socket.end(); }
    if (opcode === 0x9) { sendFrame(c.socket, 0xA, payload); continue; }
    if (opcode === 0xA) { c.alive = true; continue; }
    if (opcode === 0x1 || opcode === 0x0) {
      c.frag = opcode === 0x1 ? [payload] : [...(c.frag || []), payload];
      if (fin) {
        const text = Buffer.concat(c.frag).toString('utf8');
        c.frag = null;
        let msg; try { msg = JSON.parse(text); } catch (e) { continue; }
        try { handleMessage(c, msg); } catch (e) { console.error('erro:', e); }
      }
    }
  }
}

/* ---------- mensagens do jogo ---------- */
const validCode = s => typeof s === 'string' && /^[A-Z]{4}$/.test(s);
const cleanName = s => String(s || 'Jogador').replace(/[<>]/g, '').slice(0, 20) || 'Jogador';

function handleMessage(c, m) {
  if (!m || typeof m !== 'object') return;
  const reply = obj => sendJson(c, { id: m.id, ...obj });

  switch (m.t) {
    case 'ping': return sendJson(c, { t: 'pong' });

    case 'get': { // pede a sala (e passa a receber as mudanças dela)
      if (!validCode(m.code)) return reply({ t: 'room', code: m.code, room: null });
      const room = rooms[m.code] || null;
      if (room) { c.subs.add(m.code); lastActive[m.code] = Date.now(); }
      return reply({ t: 'room', code: m.code, room });
    }

    case 'create': {
      if (!validCode(m.code) || !m.room || !Array.isArray(m.room.players) || !m.room.players[0]) return reply({ error: 'Pedido inválido.' });
      if (rooms[m.code]) return reply({ ok: false });
      const p = m.room.players[0];
      const room = { host: String(p.id), players: [{ id: String(p.id), name: cleanName(p.name), color: COLORS[0] }] };
      rooms[m.code] = room;
      lastActive[m.code] = Date.now();
      c.subs.add(m.code);
      persist();
      return reply({ ok: true, room });
    }

    case 'join': {
      const room = rooms[m.code];
      if (!validCode(m.code) || !room) return reply({ error: 'Sala não encontrada.' });
      const pid = String(m.player && m.player.id || '');
      if (!pid) return reply({ error: 'Pedido inválido.' });
      if (!room.players.some(p => p.id === pid)) {
        if (room.players.length >= 6) return reply({ error: 'Sala cheia.' });
        room.players.push({
          id: pid, name: cleanName(m.player.name),
          color: COLORS.find(col => !room.players.some(p => p.color === col)) || COLORS[0]
        });
        broadcast(m.code, { t: 'patch', code: m.code, ops: [{ p: ['players'], v: room.players }] }, c);
      }
      c.subs.add(m.code);
      lastActive[m.code] = Date.now();
      persist();
      return reply({ ok: true, room });
    }

    case 'leave': {
      const room = rooms[m.code];
      if (!room) return;
      room.players = room.players.filter(p => p.id !== String(m.pid));
      c.subs.delete(m.code);
      if (!room.players.length) {
        delete rooms[m.code];
        delete lastActive[m.code];
        broadcast(m.code, { t: 'gone', code: m.code });
      } else {
        if (room.host === String(m.pid)) room.host = room.players[0].id;
        broadcast(m.code, { t: 'patch', code: m.code, ops: [{ p: ['players'], v: room.players }, { p: ['host'], v: room.host }] });
      }
      persist();
      return;
    }

    case 'patch': {
      const room = rooms[m.code];
      if (!room || !c.subs.has(m.code) || !Array.isArray(m.ops)) return;
      applyOps(room, m.ops);
      lastActive[m.code] = Date.now();
      broadcast(m.code, { t: 'patch', code: m.code, ops: m.ops }, c);
      persist();
      return;
    }
  }
}

/* ---------- HTTP + upgrade ---------- */
const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8', 'Access-Control-Allow-Origin': '*' });
  res.end(`Servidor do jogo Humanos, Aliens e Robôs: no ar.\nSalas ativas: ${Object.keys(rooms).length} · Conexões: ${clients.size}\n`);
});

server.on('upgrade', (req, socket) => {
  const key = req.headers['sec-websocket-key'];
  if (!key || (req.headers.upgrade || '').toLowerCase() !== 'websocket') return socket.destroy();
  if (ALLOWED_ORIGIN && req.headers.origin !== ALLOWED_ORIGIN) {
    socket.write('HTTP/1.1 403 Forbidden\r\n\r\n');
    return socket.destroy();
  }
  const accept = crypto.createHash('sha1').update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
  socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + accept + '\r\n\r\n');
  socket.setNoDelay(true);
  const c = { socket, subs: new Set(), alive: true, buf: Buffer.alloc(0), frag: null };
  clients.add(c);
  socket.on('data', d => handleData(c, d));
  socket.on('close', () => clients.delete(c));
  socket.on('error', () => { clients.delete(c); socket.destroy(); });
});

// Mantém as conexões vivas e derruba as que sumiram
setInterval(() => {
  clients.forEach(c => {
    if (!c.alive) { c.socket.destroy(); clients.delete(c); return; }
    c.alive = false;
    sendFrame(c.socket, 0x9, Buffer.alloc(0));
  });
}, 30000);

// Limpa salas abandonadas
setInterval(() => {
  const now = Date.now();
  Object.keys(rooms).forEach(code => {
    if (now - (lastActive[code] || 0) > ROOM_TTL) { delete rooms[code]; delete lastActive[code]; }
  });
  persist();
}, 3600 * 1000);

server.listen(PORT, () => console.log(`Servidor ouvindo na porta ${PORT}`));
