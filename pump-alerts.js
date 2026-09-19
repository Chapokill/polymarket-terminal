#!/usr/bin/env node
/**
 * pump-fun-telegram-alerts
 *
 * Streams new Solana token creation events from pump.fun (via the public,
 * keyless PumpPortal WebSocket) and forwards them to a Telegram chat in
 * real time.
 *
 * SETUP
 *   1. npm install
 *   2. Message @BotFather on Telegram, run /newbot, copy the bot token.
 *   3. Send your new bot any message, then open in a browser:
 *        https://api.telegram.org/bot<YOUR_TOKEN>/getUpdates
 *      and copy the "chat":{"id": ...} number. That's your chat id.
 *      (Or just message @userinfobot to get your own user id, which
 *      works the same way for a private chat.)
 *   4. Copy .env.example to .env and fill in the two values, OR export
 *      them directly:
 *        export TELEGRAM_BOT_TOKEN=123456:ABC-your-token
 *        export TELEGRAM_CHAT_ID=123456789
 *   5. node pump-alerts.js
 *
 * This has to keep running to keep alerting — it does not run "in the
 * cloud" on its own. Keep it open on your machine, or deploy it to
 * something that stays on 24/7 (a small VPS, Railway, Render, a
 * Raspberry Pi, pm2 on a home server, etc). See README.md.
 */

'use strict';

require('dotenv').config();
const WebSocket = require('ws');

const PUMP_WS_URL = 'wss://pumpportal.fun/api/data';
const RUGCHECK_URL = (mint) => `https://api.rugcheck.xyz/v1/tokens/${mint}/report/summary`;
const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID;

// --- Noise filters -----------------------------------------------------
// Pump.fun mints a huge number of tokens per hour. Alerting on every
// single one will flood your phone and can get the bot rate-limited by
// Telegram. Tune these with env vars; defaults are deliberately loose
// so you see output right away, then tighten them.
const MIN_INITIAL_BUY_SOL = Number(process.env.MIN_INITIAL_BUY_SOL ?? 1); // dev's own opening buy
const MIN_MARKET_CAP_SOL = Number(process.env.MIN_MARKET_CAP_SOL ?? 0); // extra optional filter

// --- Safety gate (RugCheck) --------------------------------------------
// NOTE: this reduces obviously-bad tokens, it does not guarantee safety.
// No filter can promise "zero scam" — treat every alert that passes as a
// starting point for your own research, never as a buy signal.
const RUGCHECK_DELAY_SECONDS = Number(process.env.RUGCHECK_DELAY_SECONDS ?? 20); // let RugCheck index the token first
const MAX_RISK_SCORE = Number(process.env.MAX_RISK_SCORE ?? 40); // 0-100, higher = riskier; RugCheck treats <30 as "Good"
const SKIP_IF_RUGCHECK_UNAVAILABLE = (process.env.SKIP_IF_RUGCHECK_UNAVAILABLE ?? 'true') === 'true';

if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID) {
  console.error(
    'Missing TELEGRAM_BOT_TOKEN and/or TELEGRAM_CHAT_ID.\n' +
      'Set them as environment variables or in a .env file (see .env.example).'
  );
  process.exit(1);
}

// --- Telegram send queue -------------------------------------------------
// A single Telegram chat is safely limited to about 1 message/second.
// Queue outgoing alerts and drain them at a safe pace instead of firing
// them all at once.
const sendQueue = [];
let sending = false;

function queueTelegramMessage(text) {
  sendQueue.push(text);
  drainQueue();
}

async function drainQueue() {
  if (sending) return;
  sending = true;
  while (sendQueue.length) {
    const text = sendQueue.shift();
    try {
      const res = await fetch(
        `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`,
        {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            chat_id: TELEGRAM_CHAT_ID,
            text,
            parse_mode: 'HTML',
            disable_web_page_preview: true,
          }),
        }
      );
      if (!res.ok) {
        console.error('Telegram send failed:', res.status, await res.text());
      }
    } catch (err) {
      console.error('Telegram send error:', err.message);
    }
    await new Promise((r) => setTimeout(r, 1100)); // stay under Telegram's rate limit
  }
  sending = false;
}

function escapeHtml(str) {
  return String(str).replace(/[&<>]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;' }[c]));
}

function formatTokenMessage(t, safety) {
  const name = escapeHtml(t.name || 'Unknown');
  const symbol = escapeHtml(t.symbol || '?');
  const mint = t.mint;
  const initialBuy = t.initialBuy ?? t.solAmount ?? '?';
  const marketCap = t.marketCapSol ?? '?';
  const pumpLink = `https://pump.fun/${mint}`;
  const solscanLink = `https://solscan.io/token/${mint}`;
  const rugcheckLink = `https://rugcheck.xyz/tokens/${mint}`;

  const riskLine =
    safety && safety.score != null
      ? `RugCheck: ${safety.score}/100 risk${safety.topRisk ? ` — ${escapeHtml(safety.topRisk)}` : ''}\n`
      : '';

  return (
    `🆕 <b>${name}</b> ($${symbol})\n` +
    `Mint: <code>${mint}</code>\n` +
    `Dev buy: ${initialBuy} SOL | Mkt cap: ${marketCap} SOL\n` +
    riskLine +
    `<a href="${pumpLink}">pump.fun</a> · <a href="${solscanLink}">solscan</a> · <a href="${rugcheckLink}">rugcheck</a>\n` +
    `<i>Not financial advice — do your own research before buying anything.</i>`
  );
}

// --- RugCheck safety gate ------------------------------------------------
// Returns { pass, score, topRisk } — pass=false means "don't alert".
// Fails closed: if the report can't be fetched, SKIP_IF_RUGCHECK_UNAVAILABLE
// decides whether that counts as pass or fail.
async function checkRugSafety(mint) {
  try {
    const res = await fetch(RUGCHECK_URL(mint));
    if (!res.ok) {
      console.log(`RugCheck: no report yet for ${mint} (HTTP ${res.status})`);
      return { pass: !SKIP_IF_RUGCHECK_UNAVAILABLE, score: null, topRisk: null };
    }
    const data = await res.json();
    const score = Number(data.score_normalised ?? data.score ?? 0);
    const risks = Array.isArray(data.risks) ? data.risks : [];
    const dangerRisk = risks.find((r) => (r.level || r.type || '').toLowerCase() === 'danger');
    const topRisk = dangerRisk?.name || dangerRisk?.description || risks[0]?.name || null;

    const pass = score <= MAX_RISK_SCORE && !dangerRisk;
    return { pass, score, topRisk };
  } catch (err) {
    console.error(`RugCheck error for ${mint}:`, err.message);
    return { pass: !SKIP_IF_RUGCHECK_UNAVAILABLE, score: null, topRisk: null };
  }
}

// --- WebSocket connection with auto-reconnect ---------------------------
let ws;
let reconnectDelayMs = 1000;
const MAX_RECONNECT_DELAY_MS = 30000;

function connect() {
  console.log('Connecting to PumpPortal...');
  ws = new WebSocket(PUMP_WS_URL);

  ws.on('open', () => {
    console.log('Connected. Subscribing to new token events...');
    reconnectDelayMs = 1000;
    ws.send(JSON.stringify({ method: 'subscribeNewToken' }));
  });

  ws.on('message', (raw) => {
    let data;
    try {
      data = JSON.parse(raw.toString());
    } catch {
      return;
    }

    // PumpPortal sends a small ack/status object right after subscribing;
    // only act on actual "create" (new token) events.
    if (!data || data.txType !== 'create' || !data.mint) return;

    const initialBuy = Number(data.initialBuy ?? data.solAmount ?? 0);
    const marketCap = Number(data.marketCapSol ?? 0);

    if (initialBuy < MIN_INITIAL_BUY_SOL) return;
    if (marketCap < MIN_MARKET_CAP_SOL) return;

    console.log(`Candidate: ${data.symbol} ${data.mint} — checking RugCheck in ${RUGCHECK_DELAY_SECONDS}s...`);

    setTimeout(async () => {
      const safety = await checkRugSafety(data.mint);
      if (!safety.pass) {
        console.log(
          `Skipped ${data.symbol} ${data.mint} — RugCheck score ${safety.score ?? 'unavailable'}${
            safety.topRisk ? `, flagged: ${safety.topRisk}` : ''
          }`
        );
        return;
      }
      console.log(`Alerting: ${data.symbol} ${data.mint} — RugCheck score ${safety.score}`);
      queueTelegramMessage(formatTokenMessage(data, safety));
    }, RUGCHECK_DELAY_SECONDS * 1000);
  });

  ws.on('close', () => {
    console.log(`Disconnected. Reconnecting in ${reconnectDelayMs}ms...`);
    setTimeout(connect, reconnectDelayMs);
    reconnectDelayMs = Math.min(reconnectDelayMs * 2, MAX_RECONNECT_DELAY_MS);
  });

  ws.on('error', (err) => {
    console.error('WebSocket error:', err.message);
    ws.close();
  });
}

connect();
