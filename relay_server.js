/**
 * relay_server.js  —  v3 Long-Poll Edition
 * ═══════════════════════════════════════════════════════════════════
 *  Cloud Relay Server cho MT5 Hedging — Không cần Port Forward
 *  Tối ưu độ trễ bằng Long-Polling: delay ≈ 1×RTT mạng
 *
 *  CÁCH HOẠT ĐỘNG:
 *  Short Poll (cũ):  Slave GET→[]→chờ→GET→[]→GET→có lệnh!  delay=poll+RTT
 *  Long Poll (mới):  Slave GET ──── server giữ ──── push→trả ngay!  delay=RTT
 *
 *  Deploy khuyến nghị: Railway.app region Singapore
 *    npm install express && node relay_server.js
 * ═══════════════════════════════════════════════════════════════════
 */
'use strict';

const express      = require('express');
const app          = express();
const PORT         = process.env.PORT         || 3000;
const MSG_TTL_MS   = (process.env.MSG_TTL_SEC || 120) * 1000;
const MAX_QUEUE    = parseInt(process.env.MAX_QUEUE   || '200');
const SECRET_KEY   = process.env.SECRET_KEY   || '';
const LONG_POLL_MS = parseInt(process.env.LP_TIMEOUT  || '2000');

app.use(express.json({ limit: '32kb' }));

// Hàng đợi lệnh: queue[token][channel] = [messages]
const queue = Object.create(null);
function getChannel(token, ch) {
   if (!queue[token])        queue[token]        = Object.create(null);
   if (!queue[token][ch])    queue[token][ch]    = [];
   return queue[token][ch];
}

// Danh sách Slave đang Long-Poll chờ lệnh
const waiters = [];

// Giao lệnh ngay cho Slave đang chờ khi Master push
function notifyWaiters(token, channel) {
   const now = Date.now();
   for (let i = waiters.length - 1; i >= 0; i--) {
      const w = waiters[i];
      if (w.token !== token || w.channel !== channel) continue;
      const ch = getChannel(token, channel);
      if (ch.length === 0) continue;
      const msgs  = ch.splice(0, ch.length);
      const valid = msgs.filter(m => now - m._pushed_at < MSG_TTL_MS);
      if (valid.length === 0) continue;
      clearTimeout(w.timeoutId);
      waiters.splice(i, 1);
      console.log(`[LP-NOTIFY] token=${token.substring(0,6)}** ch=${channel} msgs=${valid.length}`);
      w.res.json(valid.map(({ _pushed_at, ...rest }) => rest));
      return;
   }
}

// Dọn message hết hạn mỗi 60 giây
setInterval(() => {
   const now = Date.now();
   for (const t of Object.keys(queue))
      for (const c of Object.keys(queue[t]))
         queue[t][c] = queue[t][c].filter(m => now - m._pushed_at < MSG_TTL_MS);
}, 60_000);

// Auth middleware
app.use((req, res, next) => {
   if (!SECRET_KEY) return next();
   const key = req.headers['x-relay-key'] || req.query['relay_key'];
   if (key !== SECRET_KEY) return res.status(401).json({ error: 'Unauthorized' });
   next();
});

// POST /push-order
app.post('/push-order', (req, res) => {
   const { token, channel, cmd } = req.body;
   if (!token || token.length < 4) return res.status(400).json({ error: 'token bắt buộc' });
   if (channel !== 'm2s' && channel !== 's2m') return res.status(400).json({ error: 'channel sai' });
   if (!cmd) return res.status(400).json({ error: 'cmd bắt buộc' });

   const ch = getChannel(token, channel);
   if (ch.length >= MAX_QUEUE) return res.status(429).json({ error: 'Queue đầy' });

   if (req.body.ticket != null && cmd === 'OPEN_TRADE')
      if (ch.find(m => m.ticket === req.body.ticket && m.cmd === 'OPEN_TRADE'))
         return res.status(200).json({ status: 'duplicate_ignored' });

   const msg = { ...req.body, _pushed_at: Date.now() };
   delete msg.token;
   ch.push(msg);

   const nWaiters = waiters.filter(w => w.token===token && w.channel===channel).length;
   console.log(`[PUSH] token=${token.substring(0,6)}** ch=${channel} cmd=${cmd} ticket=${req.body.ticket??'-'} waiters=${nWaiters}`);

   // ★ Điểm cốt lõi: notify ngay → Slave nhận lệnh trong 1×RTT
   notifyWaiters(token, channel);

   return res.status(200).json({ status: 'ok', queue_size: ch.length });
});

// GET /pull-order?token=xxx&channel=m2s[&lp=0]
app.get('/pull-order', (req, res) => {
   const { token, channel, lp = '1' } = req.query;
   if (!token || token.length < 4) return res.status(400).json({ error: 'token bắt buộc' });
   if (channel !== 'm2s' && channel !== 's2m') return res.status(400).json({ error: 'channel sai' });

   // Kiểm tra queue ngay
   const ch  = getChannel(token, channel);
   const now = Date.now();
   if (ch.length > 0) {
      const msgs  = ch.splice(0, ch.length);
      const valid = msgs.filter(m => now - m._pushed_at < MSG_TTL_MS);
      if (valid.length > 0) {
         console.log(`[PULL] token=${token.substring(0,6)}** ch=${channel} delivered=${valid.length} (immediate)`);
         return res.json(valid.map(({ _pushed_at, ...rest }) => rest));
      }
   }

   if (lp === '0') return res.json([]);

   // Long-poll: giữ kết nối, trả ngay khi có lệnh
   const timeoutId = setTimeout(() => {
      const idx = waiters.findIndex(w => w.res === res);
      if (idx >= 0) waiters.splice(idx, 1);
      res.json([]); // timeout → Slave gửi request mới ngay
   }, LONG_POLL_MS);

   waiters.push({ token, channel, res, timeoutId });
   req.on('close', () => {
      const idx = waiters.findIndex(w => w.res === res);
      if (idx >= 0) { clearTimeout(waiters[idx].timeoutId); waiters.splice(idx, 1); }
   });
});

// GET /health
app.get('/health', (_req, res) => {
   let total = 0;
   for (const t of Object.keys(queue))
      for (const c of Object.keys(queue[t])) total += queue[t][c].length;
   res.json({
      status: 'running', uptime_sec: Math.floor(process.uptime()),
      active_tokens: Object.keys(queue).length, pending_msgs: total,
      active_waiters: waiters.length, long_poll_ms: LONG_POLL_MS,
   });
});

app.use((_req, res) => res.status(404).json({ error: 'Not found' }));

app.listen(PORT, () => {
   console.log(`MT5 Relay v3 Long-Poll | Port:${PORT} | LP:${LONG_POLL_MS}ms | TTL:${MSG_TTL_MS/1000}s`);
});
