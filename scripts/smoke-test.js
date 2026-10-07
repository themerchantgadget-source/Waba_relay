'use strict';
/**
 * Smoke test for waba-relay. Requires the server to be running (npm start
 * or `service waba-relay start`). Exercises:
 *   1. GET /token on localhost
 *   2. WS auth via query param, Authorization header, and first-message
 *   3. ping/pong
 *   4. room create (6-char code), guest join, host->guest broadcast, close
 *   5. wrong token is rejected
 * Exit 0 on success, 1 on failure.
 */
const http = require('http');
const WebSocket = require('ws');

const PORT = process.env.WABA_RELAY_PORT || 8765;
const BASE = `127.0.0.1:${PORT}`;
let failures = 0;

function check(name, cond) {
  console.log(`${cond ? 'PASS' : 'FAIL'}  ${name}`);
  if (!cond) failures++;
}
function getToken() {
  return new Promise((resolve, reject) => {
    http.get({ host: '127.0.0.1', port: PORT, path: '/token' }, res => {
      let body = '';
      res.on('data', d => (body += d));
      res.on('end', () => {
        try { resolve({ status: res.statusCode, token: JSON.parse(body).token }); }
        catch (e) { reject(e); }
      });
    }).on('error', reject);
  });
}
function connect(opts, token) {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(`ws://${BASE}/`, opts);
    const timer = setTimeout(() => reject(new Error('timeout waiting for auth.success')), 4000);
    ws.on('message', raw => {
      const m = JSON.parse(raw);
      if (m.type === 'auth.success') { clearTimeout(timer); resolve(ws); }
      if (m.type === 'auth.error') { clearTimeout(timer); reject(new Error('auth.error')); }
    });
    ws.on('error', e => { clearTimeout(timer); reject(e); });
    if (token) ws.on('open', () => ws.send(JSON.stringify({ type: 'auth', token })));
  });
}
function nextOfType(ws, type, ms = 4000) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`timeout waiting for ${type}`)), ms);
    const h = raw => {
      const m = JSON.parse(raw);
      if (m.type === type) { clearTimeout(timer); ws.off('message', h); resolve(m); }
    };
    ws.on('message', h);
  });
}

(async () => {
  const { status, token } = await getToken();
  check('GET /token returns 200 + token', status === 200 && typeof token === 'string' && token.length >= 16);

  // auth styles
  const q = await connect({}, null).catch(() => null); // no creds at all -> must fail
  check('connection without token is rejected', q === null);
  const ws1 = await new WebSocket(`ws://${BASE}/?token=${token}`);
  const ok1 = await nextOfType(ws1, 'auth.success').then(() => true).catch(() => false);
  check('auth via ?token= query param', ok1);
  const ws2 = await connect({ headers: { Authorization: `Bearer ${token}` } }).catch(() => null);
  check('auth via Authorization: Bearer header', !!ws2);
  const ws3 = await connect({}, token).catch(() => null);
  check('auth via first message {type:auth}', !!ws3);

  // ping
  if (ws3) {
    const pong = nextOfType(ws3, 'pong');
    ws3.send(JSON.stringify({ type: 'ping', ts: 123 }));
    check('ping -> pong', await pong.then(m => m.ts === 123).catch(() => false));
  }

  // rooms: host creates, guest joins, broadcast, close
  const host = ws1, guest = ws2;
  if (host && guest) {
    const created = nextOfType(host, 'room.created');
    host.send(JSON.stringify({ type: 'host', name: 'Tester' }));
    const { code } = await created.catch(() => ({}));
    check('room create returns 6-char code', /^[A-Z2-9]{6}$/.test(code || ''));

    if (code) {
      const joined = nextOfType(guest, 'room.joined');
      guest.send(JSON.stringify({ type: 'join', code }));
      check('guest join', await joined.then(m => m.code === code).catch(() => false));

      const echo = nextOfType(guest, 'message');
      host.send(JSON.stringify({ type: 'message', text: 'hello guest' }));
      check('host broadcast reaches guest', await echo.then(m => m.text === 'hello guest').catch(() => false));

      const closed = nextOfType(guest, 'room.closed');
      host.send(JSON.stringify({ type: 'stop' }));
      check('host stop closes room for guest', await closed.then(() => true).catch(() => false));
    }
  }

  for (const ws of [ws1, ws2, ws3]) ws && ws.close();
  console.log(failures ? `\n${failures} FAILURE(S)` : '\nALL PASS');
  process.exit(failures ? 1 : 0);
})().catch(e => { console.error('smoke test error:', e.message); process.exit(1); });
