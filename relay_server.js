/**
 * relay_server.js
 * ═══════════════════════════════════════════════════════════════════
 *  Cloud Relay Server — Hệ Thống Hedging MT5 Không Cần Port Forward
 * ═══════════════════════════════════════════════════════════════════
 *  Kiến trúc: Hàng đợi in-memory phân kênh theo token + direction.
 *
 *  ENDPOINT:
 *  ┌──────────────────────────────────────────────────────────────┐
 *  │  POST /push-order   → Master đẩy lệnh lên server            │
 *  │  GET  /pull-order   → Slave lấy lệnh xuống (auto-dequeue)   │
 *  │  GET  /health       → Kiểm tra server                       │
 *  └──────────────────────────────────────────────────────────────┘
 *
 *  FIELD "channel":
 *    "m2s" = Master → Slave  (lệnh giao dịch, STOP_ALL từ Master)
 *    "s2m" = Slave  → Master (ACK, STOP_ALL từ Slave)
 *
 *  Deploy miễn phí: Render.com / Railway.app / Fly.io
 *    npm install express
 *    node relay_server.js
 * ═══════════════════════════════════════════════════════════════════
 */

'use strict';

const express = require('express');
const app     = express();

// ── Cấu hình ──────────────────────────────────────────────────────
const PORT        = process.env.PORT          || 3000;
const MSG_TTL_MS  = (process.env.MSG_TTL_SEC  || 120) * 1000;  // TTL tối đa mỗi tin (ms)
const MAX_QUEUE   = parseInt(process.env.MAX_QUEUE || '200');   // Giới hạn queue mỗi kênh
const SECRET_KEY  = process.env.SECRET_KEY    || '';            // (Tùy chọn) Server-level key

app.use(express.json({ limit: '32kb' }));
app.use(express.urlencoded({ extended: false }));

// ── Cấu trúc lưu trữ ──────────────────────────────────────────────
// queue[token][channel] = [ { ...message, _pushed_at } ]
const queue = Object.create(null);

// ── Tiện ích ──────────────────────────────────────────────────────
function getChannel(token, channel) {
   if (!queue[token])            queue[token]            = Object.create(null);
   if (!queue[token][channel])   queue[token][channel]   = [];
   return queue[token][channel];
}

function purgeExpired() {
   const now = Date.now();
   for (const token of Object.keys(queue)) {
      for (const ch of Object.keys(queue[token])) {
         queue[token][ch] = queue[token][ch].filter(m => now - m._pushed_at < MSG_TTL_MS);
      }
   }
}

// Dọn dẹp tin hết hạn mỗi 60 giây
setInterval(purgeExpired, 60_000);

// ── Middleware: kiểm tra SECRET_KEY (tùy chọn) ────────────────────
function authMiddleware(req, res, next) {
   if (!SECRET_KEY) return next();                         // Không bật = bỏ qua
   const key = req.headers['x-relay-key'] || req.query['relay_key'];
   if (key !== SECRET_KEY)
      return res.status(401).json({ error: 'Unauthorized: relay key không hợp lệ' });
   next();
}
app.use(authMiddleware);

// ══════════════════════════════════════════════════════════════════
//  POST /push-order
//  Đẩy một lệnh vào hàng đợi server.
//
//  Body (JSON):
//  {
//    "token"     : "MY_SECRET_TOKEN",    // Bắt buộc
//    "channel"   : "m2s",                // "m2s" hoặc "s2m" — Bắt buộc
//    "cmd"       : "OPEN_TRADE",         // Loại lệnh
//    "ticket"    : 12345678,             // Ticket Master (ulong → number)
//    "symbol"    : "XAUUSD",
//    "trade_dir" : "BUY",
//    "lot"       : 0.05,
//    "price"     : 2345.60,
//    "vsl"       : 2320.60,
//    "vtp"       : 2395.60,
//    "reason"    : "EQUITY_ZERO",        // Chỉ dùng cho STOP_ALL
//    "ts"        : 1700000000
//  }
// ══════════════════════════════════════════════════════════════════
app.post('/push-order', (req, res) => {
   const { token, channel, cmd } = req.body;

   // Validate bắt buộc
   if (!token || typeof token !== 'string' || token.length < 4)
      return res.status(400).json({ error: 'token bắt buộc và phải >= 4 ký tự' });
   if (channel !== 'm2s' && channel !== 's2m')
      return res.status(400).json({ error: 'channel phải là "m2s" hoặc "s2m"' });
   if (!cmd || typeof cmd !== 'string')
      return res.status(400).json({ error: 'cmd bắt buộc' });

   const ch = getChannel(token, channel);

   // Giới hạn queue tránh spam
   if (ch.length >= MAX_QUEUE)
      return res.status(429).json({ error: 'Hàng đợi đầy. Slave đang offline?' });

   // Chống trùng lệnh: nếu cùng ticket đã có trong queue, bỏ qua
   if (req.body.ticket != null && cmd === 'OPEN_TRADE') {
      const dup = ch.find(m => m.ticket === req.body.ticket && m.cmd === 'OPEN_TRADE');
      if (dup)
         return res.status(200).json({ status: 'duplicate_ignored', queue_size: ch.length });
   }

   // Đẩy vào queue kèm timestamp server
   const message = { ...req.body, _pushed_at: Date.now() };
   delete message.token; // Không lưu token trong payload để tiết kiệm RAM
   ch.push(message);

   console.log(`[PUSH] token=${token.substring(0,6)}** ch=${channel} cmd=${cmd} ` +
               `queue=${ch.length} ticket=${req.body.ticket ?? '-'}`);

   return res.status(200).json({ status: 'ok', queue_size: ch.length });
});

// ══════════════════════════════════════════════════════════════════
//  GET /pull-order?token=xxx&channel=m2s
//  Lấy TẤT CẢ tin đang chờ trong kênh, sau đó XÓA SẠCH kênh đó.
//  Trả về mảng JSON. Nếu rỗng → []
// ══════════════════════════════════════════════════════════════════
app.get('/pull-order', (req, res) => {
   const { token, channel } = req.query;

   if (!token || typeof token !== 'string' || token.length < 4)
      return res.status(400).json({ error: 'token bắt buộc' });
   if (channel !== 'm2s' && channel !== 's2m')
      return res.status(400).json({ error: 'channel phải là "m2s" hoặc "s2m"' });

   // Lấy và xóa ngay để tránh lặp lệnh
   const ch       = getChannel(token, channel);
   const messages = ch.splice(0, ch.length); // Atomic dequeue toàn bộ

   // Lọc tin chưa hết hạn
   const now   = Date.now();
   const valid = messages.filter(m => now - m._pushed_at < MSG_TTL_MS);
   const stale = messages.length - valid.length;

   if (stale > 0)
      console.log(`[PULL] token=${token.substring(0,6)}** ch=${channel} stale_dropped=${stale}`);

   if (valid.length > 0)
      console.log(`[PULL] token=${token.substring(0,6)}** ch=${channel} delivered=${valid.length}`);

   // Loại bỏ field nội bộ trước khi trả về client
   const payload = valid.map(({ _pushed_at, ...rest }) => rest);

   return res.status(200).json(payload);   // [] nếu rỗng
});

// ── GET /health ───────────────────────────────────────────────────
app.get('/health', (_req, res) => {
   let total_msgs = 0;
   for (const tk of Object.keys(queue))
      for (const ch of Object.keys(queue[tk]))
         total_msgs += queue[tk][ch].length;

   res.json({
      status        : 'running',
      uptime_sec    : Math.floor(process.uptime()),
      active_tokens : Object.keys(queue).length,
      pending_msgs  : total_msgs,
      msg_ttl_sec   : MSG_TTL_MS / 1000,
      max_queue     : MAX_QUEUE,
   });
});

// ── 404 Fallback ──────────────────────────────────────────────────
app.use((_req, res) => res.status(404).json({ error: 'Endpoint không tồn tại' }));

// ── Khởi động ─────────────────────────────────────────────────────
app.listen(PORT, () => {
   console.log('═══════════════════════════════════════════════════');
   console.log(' MT5 Hedge Relay Server  —  No Port Forward Needed');
   console.log('═══════════════════════════════════════════════════');
   console.log(` Port      : ${PORT}`);
   console.log(` Msg TTL   : ${MSG_TTL_MS / 1000}s`);
   console.log(` Max Queue : ${MAX_QUEUE} msgs/channel`);
   console.log(` Auth Key  : ${SECRET_KEY ? 'ENABLED' : 'disabled'}`);
   console.log('───────────────────────────────────────────────────');
   console.log(' POST /push-order   — Đẩy lệnh lên');
   console.log(' GET  /pull-order   — Lấy lệnh xuống (auto-dequeue)');
   console.log(' GET  /health       — Trạng thái server');
   console.log('═══════════════════════════════════════════════════');
});
