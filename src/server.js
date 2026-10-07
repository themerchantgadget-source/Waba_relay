'use strict';
/**
 * waba-relay — clean-room relay compatible with ClawTerminal's "Relay Server".
 *
 * Surface (from ClawTerminal docs + in-app quick setup):
 *   - WebSocket endpoint on ws://<host>:8765 (any path accepted)
 *   - Token auth; token retrievable on the host via GET http://localhost:8765/token
 *   - Keeps SSH / Claude CLI sessions alive while the iOS app is backgrounded
 *   - Shared rooms: host gets a 6-char code, guests join read-only, output is broadcast
 *
 * The exact wire protocol is proprietary, so this server is deliberately
 * permissive and logs every frame it cannot classify — check logs/relay.log
 * after a failed "Test Connection" and extend the handlers below.
 */

const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = parseInt(process.env.WABA_RELAY_PORT || '8765', 10);
const ROOT = path.resolve(__dirname, '..');
const STATE_DIR = process.env.WABA_RELAY_STATE || path.join(ROOT, '.relay-state');
const LOG_DIR = process.env.WABA_RELAY_LOGS || path.join(ROOT, 'logs');
const TOKEN_FILE = path.join(STATE_DIR, 'token');
const AUTH_TIMEOUT_MS = 10_000;
const HEARTBEAT_MS = 30_000;

for (const dir of [STATE_DIR, LOG_DIR]) fs.mkdirSync(dir, { recursive: true });
const logStream = fs.createWriteStream(path.join(LOG_DIR, 'relay.log'), { flags: 'a' });

function log(level, msg, extra) {
  const line = `${new Date().toISOString()} [${level}] ${msg}` + (extra !== undefined ? ` ${safe(extra)}` : '');
  logStream.write(line + '\n');
  console.log(line);
}
function safe(v) {
  try { const s = typeof v === 'string' ? v : JSON.stringify(v); return s.length > 4096 ? s.slice(0, 4096) + '…[truncated]' : s; }
  catch { return '[unserializable]'; }
}

// --- token: generated once, persisted, root of all auth ----------------------
function loadOrCreateToken() {
  try {
    const t = fs.readFileSync(TOKEN_FILE, 'utf8').trim();
    if (t.length >= 16) return t;
  } catch { /* first run */ }
  const t = crypto.randomBytes(24).toString('base64url');
  fs.writeFileSync(TOKEN_FILE, t + '\n', { mode: 0o600 });
  log('info', 'generated new auth token');
  return t;
}
const TOKEN = loadOrCreateToken();

// --- tiny helpers ------------------------------------------------------------
const rooms = new Map(); // code -> { host: ws, guests: Set<ws>, created: number }
let nextConnId = 1;

function send(ws, obj) {
  if (ws.readyState === ws.OPEN) ws.send(JSON.stringify(obj));
}
function makeRoomCode() {
  const alphabet = 'ABCDEFGHJKMNPQRSTUVWXYZ23456789'; // no easily-confused chars
  for (;;) {
    const code = Array.from(crypto.randomBytes(6)).map(b => alphabet[b % alphabet.length]).join('');
    if (!rooms.has(code)) return code;
  }
}
function roomOf(ws) { return ws.meta.room ? rooms.get(ws.meta.room) : undefined; }
function leaveRoom(ws, reason) {
  const code = ws.meta.room;
  if (!code) return;
  const room = rooms.get(code);
  ws.meta.room = null;
  if (!room) return;
  if (room.host === ws) {
    for (const g of room.guests) { send(g, { type: 'room.closed', code, reason }); g.meta.room = null; }
    rooms.delete(code);
    log('info', `room ${code} closed (${reason})`);
  } else {
    room.guests.delete(ws);
    send(room.host, { type: 'room.guest.left', code });
    log('info', `guest left room ${code} (${reason})`);
  }
}

// --- message classification (extend here as the real protocol reveals itself)
function classify(type) {
  const t = String(type || '').toLowerCase();
  if (/\b(auth|hello|init|handshake)\b/.test(t)) return 'auth';
  if (/\b(ping|heartbeat|keepalive)\b/.test(t)) return 'ping';
  if (/(host|create|share|start.*room|room.*(create|start|open))/.test(t)) return 'room.create';
  if (/(join|room.*join)/.test(t)) return 'room.join';
  if (/(leave|stop|close|end|unshare|room.*(leave|close|end))/.test(t)) return 'room.leave';
  if (/(message|output|data|broadcast|relay|frame|chunk|stdin|input)/.test(t)) return 'room.broadcast';
  if (/(session|attach|register|subscribe)/.test(t)) return 'session';
  return 'unknown';
}

function handleMessage(ws, raw) {
  let msg;
  try { msg = JSON.parse(raw); } catch {
    log('warn', `conn#${ws.meta.id} non-JSON frame`, raw.toString());
    return;
  }
  const kind = classify(msg.type);
  log('info', `conn#${ws.meta.id} <= ${kind}`, msg);

  switch (kind) {
    case 'auth':
      if (msg.token !== TOKEN) {
        send(ws, { type: 'auth.error', error: 'invalid token' });
        ws.close(4401, 'invalid token');
        return;
      }
      completeAuth(ws, 'message');
      return;

    case 'ping':
      send(ws, { type: 'pong', ts: msg.ts ?? Date.now() });
      return;

    case 'room.create': {
      leaveRoom(ws, 'rehost');
      const code = makeRoomCode();
      rooms.set(code, { host: ws, guests: new Set(), created: Date.now() });
      ws.meta.room = code;
      ws.meta.role = 'host';
      ws.meta.name = msg.name || msg.displayName || 'Host';
      send(ws, { type: 'room.created', code });
      send(ws, { type: 'room.hosting', code }); // alias, unknown which the app expects
      log('info', `conn#${ws.meta.id} hosting room ${code}`);
      return;
    }

    case 'room.join': {
      const code = String(msg.code || msg.room || '').toUpperCase();
      const room = rooms.get(code);
      if (!room) { send(ws, { type: 'room.error', code, error: 'room not found' }); return; }
      leaveRoom(ws, 'rejoin');
      room.guests.add(ws);
      ws.meta.room = code;
      ws.meta.role = 'guest';
      ws.meta.name = msg.name || msg.displayName || 'Guest';
      send(ws, { type: 'room.joined', code, host: room.host.meta.name });
      send(room.host, { type: 'room.guest.joined', code, name: ws.meta.name });
      log('info', `conn#${ws.meta.id} joined room ${code}`);
      return;
    }

    case 'room.leave':
      leaveRoom(ws, 'requested');
      send(ws, { type: 'room.left' });
      return;

    case 'room.broadcast': {
      const room = roomOf(ws);
      if (!room) { send(ws, { type: 'room.error', error: 'not in a room' }); return; }
      const out = Object.assign({}, msg, { from: ws.meta.role, name: ws.meta.name });
      if (room.host === ws) { for (const g of room.guests) send(g, out); }
      else send(room.host, out); // guest input (if the app ever allows it) goes to host
      return;
    }

    case 'session':
      // Session keepalive semantics are TBD — ack so the app can proceed,
      // and keep the raw payload in the log for protocol analysis.
      send(ws, { type: 'ack', for: msg.type, ok: true });
      return;

    default:
      send(ws, { type: 'ack', for: msg.type ?? null, ok: true, note: 'unhandled' });
      log('warn', `conn#${ws.meta.id} UNHANDLED type=${msg.type}`, msg);
  }
}

// --- auth: accept token via ?token=, Authorization: Bearer, or first message
function tryTransportAuth(ws, req) {
  const url = new URL(req.url, 'http://x');
  const header = req.headers.authorization || '';
  const candidate = url.searchParams.get('token')
    || url.searchParams.get('auth')
    || (header.startsWith('Bearer ') ? header.slice(7) : null);
  if (candidate == null) return false; // fall back to first-message auth
  if (candidate === TOKEN) { completeAuth(ws, 'transport'); return true; }
  send(ws, { type: 'auth.error', error: 'invalid token' });
  ws.close(4401, 'invalid token');
  return true;
}

function completeAuth(ws, via) {
  if (ws.meta.authed) return;
  ws.meta.authed = true;
  clearTimeout(ws.meta.authTimer);
  log('info', `conn#${ws.meta.id} authenticated via ${via}`);
  send(ws, {
    type: 'auth.success',
    server: 'waba-relay',
    version: require('../package.json').version,
    ts: Date.now(),
  });
}

// --- HTTP: /token (localhost only) + health ---------------------------------
const server = http.createServer((req, res) => {
  const local = ['127.0.0.1', '::1', '::ffff:127.0.0.1'].includes(req.socket.remoteAddress);
  if (req.url.startsWith('/token')) {
    if (!local) { res.writeHead(403).end('localhost only\n'); return; }
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ token: TOKEN }) + '\n');
    return;
  }
  if (req.url === '/health') {
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ ok: true, rooms: rooms.size, conns: wss ? wss.clients.size : 0 }) + '\n');
    return;
  }
  res.writeHead(404).end('waba-relay: WebSocket endpoint — upgrade required\n');
});

// --- WebSocket ---------------------------------------------------------------
const wss = new WebSocketServer({ server });
wss.on('connection', (ws, req) => {
  ws.meta = { id: nextConnId++, authed: false, room: null, role: null, name: null, alive: true };
  log('info', `conn#${ws.meta.id} open from ${req.socket.remoteAddress} url=${req.url}`, {
    ua: req.headers['user-agent'], protocols: req.headers['sec-websocket-protocol'],
  });

  if (!tryTransportAuth(ws, req)) {
    ws.meta.authTimer = setTimeout(() => ws.close(4408, 'auth timeout'), AUTH_TIMEOUT_MS);
  }

  ws.on('pong', () => { ws.meta.alive = true; });
  ws.on('message', raw => {
    if (!ws.meta.authed) {
      // allow the very first message to be the auth message
      try {
        const m = JSON.parse(raw);
        if (classify(m.type) === 'auth') return handleMessage(ws, raw);
      } catch { /* fall through */ }
      send(ws, { type: 'auth.error', error: 'authentication required' });
      return ws.close(4401, 'authentication required');
    }
    handleMessage(ws, raw);
  });
  ws.on('close', (code, reason) => {
    clearTimeout(ws.meta.authTimer);
    leaveRoom(ws, `disconnect ${code}`);
    log('info', `conn#${ws.meta.id} closed code=${code} reason=${reason}`);
  });
  ws.on('error', err => log('warn', `conn#${ws.meta.id} error: ${err.message}`));
});

setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.meta.alive) { leaveRoom(ws, 'heartbeat timeout'); ws.terminate(); continue; }
    ws.meta.alive = false;
    ws.ping();
  }
}, HEARTBEAT_MS);

server.listen(PORT, '0.0.0.0', () => {
  log('info', `waba-relay listening on 0.0.0.0:${PORT} (ws any path, /token localhost-only)`);
  log('info', `auth token: ${TOKEN}`);
});
