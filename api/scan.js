export const runtime = "nodejs";

// api/scan.js
import { Redis } from "@upstash/redis";
import fetch from "node-fetch";
import zlib from "zlib";
import { fileURLToPath } from "url";

/**
 * ENV
 * - UPSTASH_REDIS_REST_URL
 * - UPSTASH_REDIS_REST_TOKEN
 * - WATCH_ADDRESSES: comma-separated fee/commit addresses to watch
 * - SCAN_CHAIN_PAGES: how many chain pages to fetch per address (default 1)
 * - INSCRIPTION_CONTENT_HOST: optional preferred default (e.g., https://ordinals.com/content)
 * - PNG_TEXT_KEY_HINT: preferred key inside PNG text chunks (default "Serial")
 * - SCAN_SINCE_UNIX: optional override (unix seconds). Default = 2025-08-27 00:00:00 UTC
 * - SCAN_RETRY_LIMIT: attempts before giving up and marking seen (default 12)
 * - SCAN_RETRY_TTL_SEC: TTL for retry counter (default 7200)
 * - BLOCKCHAIR_API_KEY: your Blockchair API key (recommended)
 * - HIRO_API_KEY: optional, for Hiro content endpoint (improves reliability)
 * - REBUILD_URL: optional, your backend endpoint to rebuild used_serials (POST)
 * - INTERNAL_TOKEN: optional header for REBUILD_URL
 */

const REDIS = new Redis({
  url: process.env.UPSTASH_REDIS_REST_URL,
  token: process.env.UPSTASH_REDIS_REST_TOKEN,
});

const MEMPOOL = "https://mempool.space/api"; // used for mempool (unconfirmed) + as outspends fallback
const BLOCKCHAIR = "https://api.blockchair.com/bitcoin";
const BLOCKCHAIR_API_KEY = process.env.BLOCKCHAIR_API_KEY;
if (!BLOCKCHAIR_API_KEY) {
  throw new Error("BLOCKCHAIR_API_KEY is not set");
}

const PNG_TEXT_KEY_HINT = process.env.PNG_TEXT_KEY_HINT || "Serial";
const HIRO_API_KEY = process.env.HIRO_API_KEY || "";

// Content hosts (rotate/fallback). We append /{id} or /{id}/content when needed.
const CONTENT_HOSTS = [
  process.env.INSCRIPTION_CONTENT_HOST || "https://static.unisat.io/content",
  "https://ordinals.com/content",
  "https://api.hiro.so/ordinals/v1/inscriptions", // we append `/content` for Hiro below
];

const PNG_SIG = Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]);
const WATCH_ADDRESSES = (process.env.WATCH_ADDRESSES || "")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean)
  .slice(0, 20);

const CHAIN_PAGES = Math.max(
  1,
  parseInt(process.env.SCAN_CHAIN_PAGES || "1", 10)
);

// Fixed “since” date: Aug 27, 2025 00:00:00 UTC, unless overridden
const SCAN_SINCE_UNIX = parseInt(
  process.env.SCAN_SINCE_UNIX ||
    String(Math.floor(Date.parse("2025-08-27T00:00:00Z") / 1000)),
  10
);

// Retry controls for serial-missing txs
const SCAN_RETRY_LIMIT = Math.max(
  1,
  parseInt(process.env.SCAN_RETRY_LIMIT || "60", 10) // up to ~5 hours at 5m cadence
);
const SCAN_RETRY_TTL_SEC = Math.max(
  10, // allow very fast cycles
  parseInt(process.env.SCAN_RETRY_TTL_SEC || "300", 10) // default 5 min
);

// =========================== Next.js API handler ===========================
export default async function handler(req, res) {
  const singleAddr = (req?.query?.addr || "").toString().trim();
  const addrs = singleAddr ? [singleAddr] : WATCH_ADDRESSES;

  if (!addrs.length) {
    return res.status(400).json({
      ok: false,
      error:
        "No address specified. Use WATCH_ADDRESSES env or pass ?addr=<btc_address>.",
    });
  }

  try {
    const results = [];
    for (const addr of addrs) {
      const out = await scanAddress(addr);
      // NOTE: Including 'hits' is additive & does not affect Redis writes.
      results.push({ address: addr, processed: out.length, hits: out });
    }

    // Optional post-scan rebuild
    const rebuildUrl = process.env.REBUILD_URL; // e.g. https://nekoback.onrender.com/admin/rebuild_used_serials
    if (rebuildUrl) {
      try {
        await fetch(rebuildUrl, {
          method: "POST",
          headers: { "X-Internal-Token": process.env.INTERNAL_TOKEN || "" },
        });
      } catch (e) {
        console.error("Rebuild call failed:", e);
      }
    }

    return res.status(200).json({ ok: true, since: SCAN_SINCE_UNIX, results });
  } catch (e) {
    console.error(e);
    return res.status(500).json({ ok: false, error: String(e) });
  }
}

// =========================== Core scan logic ===========================
async function scanAddress(address) {
  // A) Unconfirmed (mempool)
  const mempoolTxs =
    (await safeGetJSON(`${MEMPOOL}/address/${address}/txs/mempool`).catch(
      () => []
    )) || [];

  // B) Confirmed (Blockchair)
  const rawChain = await fetchChainTxs(address, CHAIN_PAGES);
  const chainTxs = rawChain.filter((t) => {
    const bt = t?.status?.block_time;
    return typeof bt === "number" && bt >= SCAN_SINCE_UNIX;
  });

  // De-dupe by txid
  const seenSet = new Set();
  const all = [];
  for (const t of [...mempoolTxs, ...chainTxs]) {
    if (t?.txid && !seenSet.has(t.txid)) {
      seenSet.add(t.txid);
      all.push(t);
    }
  }

  const hits = [];
  for (const tx of all) {
    const txid = tx.txid;
    const txKey = `tx:${txid}`;

    // If we've already marked as seen *and* the tx has a serial, skip.
    const alreadySeen = await REDIS.sismember("seen_txids", txid);
    if (alreadySeen) {
      const existing = await REDIS.hgetall(txKey);
      if (existing && existing.serial) continue; // fully processed
      // else: let it fall through to try again (rare)
    }

    // Optional per-tx soft lock (avoid concurrent lambdas stepping on each other)
    const lockKey = `lock:${txid}`;
    const gotLock = await REDIS.setnx(lockKey, "1");
    if (!gotLock) continue;
    await REDIS.expire(lockKey, 30);

    try {
      // Outspends via Blockchair (preferred), fallback to mempool
      const outspends =
        (await getOutspendsViaBlockchair(txid).catch(() => null)) ||
        (await getOutspendsViaMempool(txid).catch(() => null));
      if (!outspends) continue;

      const vouts = tx.vout || [];
      const candidates = [];
      for (let idx = 0; idx < Math.min(outspends.length, vouts.length); idx++) {
        const spent = outspends[idx];
        if (!spent?.spent) continue; // only consider spent outputs (reveal spends)
        const revealTxid = spent.txid;
        if (revealTxid) candidates.push(revealTxid);
      }

      // Deduplicate candidate reveal txids
      const uniqCandidates = [...new Set(candidates)];
      let inscriptionId = null;
      let serial = null;
      let pngText = null;

      // Probe up to a few indices for each reveal candidate
      for (const rtxid of uniqCandidates) {
        const id = await findPngInscriptionId(rtxid);
        if (!id) continue;

        // Try to fetch content (with fallbacks) and parse text chunks
        try {
          const buf = await fetchBinaryWithFallback(id);
          const { ok, text } = parsePngText(buf);
          if (ok) {
            inscriptionId = id;
            pngText = text;
            // Prefer explicit hint key, then "serial" (any case), then name, then generic alnum token
            serial =
              text[PNG_TEXT_KEY_HINT] ||
              getCaseInsensitive(text, "serial") ||
              maybeSerialFromJsonValues(text) ||
              (text.name && /^[A-Za-z0-9]{10,24}$/.test(String(text.name))
                ? String(text.name)
                : null) ||
              findAlnumToken(
                Object.entries(text)
                  .map(([k, v]) => `${k}=${v}`)
                  .join("\n")
              );
            if (serial) break; // stop on first successful parse
          }
        } catch {
          // ignore this candidate, try next
        }
      }

      // Buyer address (payer) is usually input[0].prevout.scriptpubkey_address
      const buyerAddr = firstInputAddress(tx);

      // Timestamps & status
      const isConfirmed = !!tx?.status?.confirmed;
      const seenAt = Math.floor(Date.now() / 1000);
      const confirmedAt = isConfirmed ? tx.status.block_time || seenAt : "";

      // Retry accounting
      const retryKey = `retry:${txid}`;
      const currentRetries = parseInt((await REDIS.get(retryKey)) || "0", 10);
      const hasSerial = !!serial;

      // If no real id, at least try i0 so the record carries something
      const storedInscriptionId =
        inscriptionId || `${uniqCandidates[0] || txid}i0`;

      // Build write
      const pipe = REDIS.multi()
        .hset(txKey, {
          inscriptionId: storedInscriptionId,
          buyerAddr: buyerAddr || "",
          watchedAddr: address,
          status: isConfirmed ? "confirmed" : "unconfirmed",
          seenAt: String(seenAt),
          confirmedAt: String(confirmedAt || ""),
          serial: hasSerial
            ? serial
            : (await getExistingField(txKey, "serial")) || "",
          pngText: pngText
            ? JSON.stringify(pngText)
            : (await getExistingField(txKey, "pngText")) || "",
          lastScanAt: String(seenAt),
        })
        .sadd(`addr:${address}:txs`, txid);

      if (buyerAddr) pipe.sadd(`buyer:${buyerAddr}:txs`, txid); // keep buyer index
      pipe.expire(txKey, 60 * 60 * 24 * 30); // keep 30 days

      if (hasSerial || currentRetries + 1 >= SCAN_RETRY_LIMIT) {
        // Mark complete
        pipe.sadd("seen_txids", txid).del(retryKey);
      } else {
        // Keep retrying later
        const burstTtl = 20; // seconds, fast rechecks at the start
        const burstTries = 9; // ~3 minutes of rapid retries (9 * 20s)
        const nextTtl =
          currentRetries < burstTries ? burstTtl : SCAN_RETRY_TTL_SEC;
        pipe.incr(retryKey).expire(retryKey, nextTtl);
      }

      // NEW: count this serial immediately (includes unconfirmed hits).
      // SADD is a set add: safe to call again later when it confirms (no duplicates).
      if (hasSerial) {
        pipe.sadd("used_serials", serial);
      }

      await pipe.exec();

      hits.push({
        txid,
        inscriptionId: storedInscriptionId,
        buyerAddr,
        serial: hasSerial ? serial : "",
        status: isConfirmed ? "confirmed" : "unconfirmed",
        retried: hasSerial ? 0 : currentRetries + 1,
      });
    } finally {
      // Release lock
      await REDIS.del(lockKey);
    }
  }

  return hits;
}

/* ------------------------ Helpers ------------------------ */

function firstInputAddress(tx) {
  try {
    const vin = tx?.vin || [];
    const a = vin[0]?.prevout?.scriptpubkey_address;
    return a || null;
  } catch {
    return null;
  }
}

// ----------- Blockchair: chain pagination (adapt to mempool-like shape) -----------
async function fetchChainTxs(address, pages) {
  const perPage = 50;
  const out = [];
  let offset = 0;

  for (let i = 0; i < pages; i++) {
    const url =
      `${BLOCKCHAIR}/dashboards/address/${address}` +
      `?transaction_details=true&limit=${perPage}&offset=${offset}&key=${BLOCKCHAIR_API_KEY}`;

    const data = await safeGetJSON(url).catch(() => null);
    if (!data || !data.data || !data.data[address]) break;

    const txDict = data.data[address].transactions || {};
    const txHashes = Object.keys(txDict);
    if (!txHashes.length) break;

    // Adapt Blockchair tx -> mempool-like shape your code expects
    for (const txid of txHashes) {
      const t = txDict[txid];
      if (!t || !t.hash) continue;

      const adapted = {
        txid: t.hash,
        status: {
          confirmed: !!t.block_id,
          block_time: t.time
            ? Math.floor(new Date(t.time).getTime() / 1000)
            : undefined,
        },
        vin: Array.isArray(t.inputs)
          ? t.inputs.map((ii) => ({
              prevout: { scriptpubkey_address: ii?.recipient || null },
            }))
          : [],
        vout: Array.isArray(t.outputs)
          ? t.outputs.map((oo) => ({
              scriptpubkey_address: oo?.recipient || null,
              value: oo?.value || null,
            }))
          : [],
      };

      out.push(adapted);
    }

    offset += perPage;
  }

  return out;
}

// ----------- Outspends: Blockchair preferred, mempool fallback -----------
async function getOutspendsViaBlockchair(txid) {
  const url = `${BLOCKCHAIR}/dashboards/transaction/${txid}?key=${BLOCKCHAIR_API_KEY}`;
  const data = await safeGetJSON(url);

  const txData = data?.data?.[txid];
  const outs = txData?.outputs;
  if (!Array.isArray(outs)) return null;

  // Adapt to mempool.space /outspends shape:
  // [{ spent: boolean, txid: <spending_txid_or_null> }, ...]
  return outs.map((o) => ({
    spent: !!o?.spending_transaction_hash,
    txid: o?.spending_transaction_hash || null,
  }));
}

async function getOutspendsViaMempool(txid) {
  const url = `${MEMPOOL}/tx/${txid}/outspends`;
  const data = await safeGetJSON(url);
  if (!Array.isArray(data)) return null;
  // shape already matches: [{ spent, txid }, ...]
  return data;
}

// ----------- Generic JSON + binary fetchers -----------
async function safeGetJSON(url) {
  const ctl = new AbortController();
  const id = setTimeout(() => ctl.abort(), 20000); // 20s
  try {
    const r = await fetch(url, { signal: ctl.signal });
    if (!r.ok) throw new Error(`${url} -> ${r.status}`);
    return r.json();
  } finally {
    clearTimeout(id);
  }
}

async function fetchBinaryOnce(url, opts = {}, timeoutMs = 15000) {
  const ctl = new AbortController();
  const id = setTimeout(() => ctl.abort(), timeoutMs);
  try {
    const r = await fetch(url, { ...opts, signal: ctl.signal });
    if (!r.ok) throw new Error(`${url} -> ${r.status}`);
    return Buffer.from(await r.arrayBuffer());
  } finally {
    clearTimeout(id);
  }
}

// Fallback host cycle: UniSat -> ordinals.com -> Hiro (with optional key)
async function fetchBinaryWithFallback(inscriptionId) {
  const errors = [];
  for (const base of CONTENT_HOSTS) {
    const { url, opts } = buildContentRequest(base, inscriptionId);
    try {
      const buf = await fetchBinaryOnce(url, opts, 15000);
      if (buf && buf.length >= 8 && buf.slice(0, 8).equals(PNG_SIG)) return buf;
      errors.push(`non-PNG from ${base}`);
    } catch (e) {
      errors.push(`${base}: ${e?.message || String(e)}`);
    }
  }
  throw new Error(`All content hosts failed: ${errors.join(" | ")}`);
}

function buildContentRequest(base, inscriptionId) {
  let url = base;
  if (base.includes("api.hiro.so/ordinals/v1/inscriptions")) {
    url = `${base}/${inscriptionId}/content`;
  } else {
    url = `${base}/${inscriptionId}`;
  }

  const headers = {
    Accept: "image/png,application/octet-stream;q=0.9,*/*;q=0.8",
  };
  if (base.includes("api.hiro.so") && HIRO_API_KEY) {
    headers["X-API-Key"] = HIRO_API_KEY;
  }
  return { url, opts: { headers } };
}

// Try indices i0..i5; return first that looks like PNG
async function findPngInscriptionId(txid, maxIndex = 5) {
  for (let i = 0; i <= maxIndex; i++) {
    const id = `${txid}i${i}`;
    try {
      const buf = await fetchBinaryWithFallback(id);
      if (buf && buf.length >= 8 && buf.slice(0, 8).equals(PNG_SIG)) return id;
    } catch {
      // ignore and try next
    }
  }
  return null;
}

async function getExistingField(key, field) {
  try {
    return await REDIS.hget(key, field);
  } catch {
    return null;
  }
}

/* ---- PNG text-chunk parsing (tEXt / zTXt / iTXt) ---- */
function parsePngText(buf) {
  if (!buf || buf.length < 8 || !buf.slice(0, 8).equals(PNG_SIG)) {
    return { ok: false, text: null };
  }
  const text = {};
  let off = 8;
  while (off + 8 <= buf.length) {
    if (off + 8 > buf.length) break;
    const len = buf.readUInt32BE(off);
    off += 4;
    const type = buf.slice(off, off + 4).toString("latin1");
    off += 4;

    if (off + len > buf.length) break;
    const data = buf.slice(off, off + len);
    off += len;

    off += 4; // skip CRC

    if (type === "tEXt") {
      const zero = data.indexOf(0x00);
      if (zero >= 0) {
        const k = data.slice(0, zero).toString("latin1");
        const v = data.slice(zero + 1).toString("latin1");
        text[k] = v;
      }
    } else if (type === "zTXt") {
      const zero = data.indexOf(0x00);
      if (zero >= 0) {
        const k = data.slice(0, zero).toString("latin1");
        const compMethod = data[zero + 1];
        const compData = data.slice(zero + 2);
        if (compMethod === 0) {
          try {
            const v = zlib.unzipSync(compData).toString("utf-8");
            text[k] = v;
          } catch (e) {
            text[k] = `<zTXt decompress error: ${e}>`;
          }
        }
      }
    } else if (type === "iTXt") {
      // keyword\0comp_flag\0comp_method\0lang\0translated\0text (payload)
      const parts = splitNul(data, 6);
      if (parts && parts.length === 6) {
        const [k, compFlag, compMethod, _lang, _trans, payload] = parts;
        try {
          let v;
          if (compFlag?.[0] === 1)
            v = zlib.unzipSync(payload).toString("utf-8");
          else v = Buffer.from(payload).toString("utf-8");
          text[k.toString("latin1")] = v;
        } catch (e) {
          text[k.toString("latin1")] = `<iTXt error: ${e}>`;
        }
      }
    }

    if (type === "IEND") break;
  }
  return { ok: Object.keys(text).length > 0, text };
}

function splitNul(buf, maxParts) {
  const out = [];
  let start = 0;
  for (let i = 0; i < buf.length && out.length < maxParts - 1; i++) {
    if (buf[i] === 0x00) {
      out.push(buf.slice(start, i));
      start = i + 1;
    }
  }
  out.push(buf.slice(start));
  return out;
}

/* ---- Serial extraction helpers ---- */
function getCaseInsensitive(mapObj, key) {
  const keys = Object.keys(mapObj || {});
  const k = keys.find((k) => k.toLowerCase() === String(key).toLowerCase());
  return k ? mapObj[k] : undefined;
}

// If a text value is itself JSON (e.g., {"name":"...","serial":"..."}), pull serial out
function maybeSerialFromJsonValues(textMap) {
  for (const v of Object.values(textMap || {})) {
    if (typeof v !== "string") continue;
    const trimmed = v.trim();
    if (!trimmed) continue;
    try {
      const obj = JSON.parse(trimmed);
      const s =
        obj?.serial ||
        obj?.Serial ||
        (obj?.name && /^[A-Za-z0-9]{10,24}$/.test(String(obj.name))
          ? obj.name
          : null);
      if (s) return String(s);
    } catch {
      // not JSON; continue
    }
  }
  return null;
}

// Last-resort: pull a 10–24 char alphanumeric token from blob text
function findAlnumToken(s) {
  if (typeof s !== "string") return null;
  const m = s.match(/\b[A-Za-z0-9]{10,24}\b/);
  return m ? m[0] : null;
}

// =========================== CLI mode (optional) ===========================
// Run: node api/scan.js --addr <btc_address> [--pages 2]
if (isRunDirectly(import.meta.url)) {
  (async () => {
    try {
      const args = parseArgs();
      if (!args.addr) {
        console.error(
          "Usage: node api/scan.js --addr <btc_address> [--pages N]"
        );
        process.exit(1);
      }
      const pages = args.pages
        ? Math.max(1, parseInt(args.pages, 10))
        : CHAIN_PAGES;
      const res = await scanAddress(args.addr);
      console.log(
        JSON.stringify({ ok: true, address: args.addr, hits: res }, null, 2)
      );
      process.exit(0);
    } catch (e) {
      console.error(e);
      process.exit(2);
    }
  })();
}

function isRunDirectly(metaUrl) {
  try {
    const thisPath = fileURLToPath(metaUrl);
    return process.argv[1] === thisPath;
  } catch {
    return false;
  }
}

function parseArgs() {
  const out = {};
  const a = process.argv.slice(2);
  for (let i = 0; i < a.length; i++) {
    const k = a[i];
    if (k === "--addr") out.addr = a[++i];
    else if (k === "--pages") out.pages = a[++i];
  }
  return out;
}
