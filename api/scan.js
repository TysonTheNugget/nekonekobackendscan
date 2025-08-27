// api/scan.js
import { Redis } from "@upstash/redis";
import fetch from "node-fetch";
import zlib from "zlib";

/**
 * ENV
 * - UPSTASH_REDIS_REST_URL
 * - UPSTASH_REDIS_REST_TOKEN
 * - WATCH_ADDRESSES: comma-separated fee/commit addresses to watch
 * - SCAN_CHAIN_PAGES: how many chain pages to fetch per address (default 1)
 * - INSCRIPTION_CONTENT_HOST: e.g., https://ordinals.com/content
 * - PNG_TEXT_KEY_HINT: preferred key inside PNG text chunks (default "Serial")
 * - SCAN_SINCE_UNIX: optional override (unix seconds). Default = 2025-08-27 00:00:00 UTC
 * - SCAN_RddETRY_LIMIT: attempts before giving up and marking seen (default 12)
 * - SCAN_RETRY_TTL_SEC: TTL for retry counter (default 7200)
 */

const REDIS = new Redis({
  url: process.env.UPSTASH_REDIS_REST_URL,
  token: process.env.UPSTASH_REDIS_REST_TOKEN,
});

const MEMPOOL = "https://mempool.space/api";
const CONTENT_HOST = process.env.INSCRIPTION_CONTENT_HOST || "https://static.unisat.io/content";
const PNG_TEXT_KEY_HINT = process.env.PNG_TEXT_KEY_HINT || "Serial";

const PNG_SIG = Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]);
const WATCH_ADDRESSES = (process.env.WATCH_ADDRESSES || "")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean)
  .slice(0, 20);

const CHAIN_PAGES = Math.max(1, parseInt(process.env.SCAN_CHAIN_PAGES || "1", 10));

// Fixed “since” date: Aug 27, 2025 00:00:00 UTC, unless overridden
const SCAN_SINCE_UNIX = parseInt(
  process.env.SCAN_SINCE_UNIX || String(Math.floor(Date.parse("2025-08-27T00:00:00Z") / 1000)),
  10
);

// Retry controls for serial-missing txs
const SCAN_RETRY_LIMIT = Math.max(1, parseInt(process.env.SCAN_RETRY_LIMIT || "12", 10)); // e.g., 12 tries
const SCAN_RETRY_TTL_SEC = Math.max(60, parseInt(process.env.SCAN_RETRY_TTL_SEC || "7200", 10)); // 2h TTL

export default async function handler(req, res) {
  if (!WATCH_ADDRESSES.length) {
    return res.status(400).json({
      error: "Set WATCH_ADDRESSES env var (comma-separated BTC addresses)",
    });
  }

  try {
    const results = [];
    for (const addr of WATCH_ADDRESSES) {
      const out = await scanAddress(addr);
      results.push({ address: addr, processed: out.length });
    }
    return res.status(200).json({ ok: true, since: SCAN_SINCE_UNIX, results });
  } catch (e) {
    console.error(e);
    return res.status(500).json({ ok: false, error: String(e) });
  }
}

async function scanAddress(address) {
  // Always include mempool (unconfirmed)
  const mempoolTxs = await safeGetJSON(`${MEMPOOL}/address/${address}/txs/mempool`).catch(() => []);

  // For chain (confirmed), fetch pages then filter by SCAN_SINCE_UNIX
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
    // Otherwise, we may retry until serial is obtained or we give up.
    const alreadySeen = await REDIS.sismember("seen_txids", txid);
    if (alreadySeen) {
      const existing = await REDIS.hgetall(txKey);
      if (existing && existing.serial) continue; // fully processed
      // If seen but no serial, fall through to give it one more try (rare).
    }

    // Optional per-tx soft lock (avoid concurrent lambdas stepping on each other)
    const lockKey = `lock:${txid}`;
    const gotLock = await REDIS.setnx(lockKey, "1");
    if (!gotLock) continue; // someone else is working on it
    await REDIS.expire(lockKey, 30); // 30s lock

    try {
      const outspends = await safeGetJSON(`${MEMPOOL}/tx/${txid}/outspends`).catch(() => null);
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

        // Try to fetch content and parse text chunks
        try {
          const buf = await fetchBinary(`${CONTENT_HOST}/${id}`);
          const { ok, text } = parsePngText(buf);
          if (ok) {
            inscriptionId = id;
            pngText = text;
            // Prefer explicit hint key, then "serial" (any case), then name, then generic alnum token
            serial =
              text[PNG_TEXT_KEY_HINT] ||
              getCaseInsensitive(text, "serial") ||
              maybeSerialFromJsonValues(text) ||
              (text.name && /^[A-Za-z0-9]{10,24}$/.test(String(text.name)) ? String(text.name) : null) ||
              findAlnumToken(Object.entries(text).map(([k, v]) => `${k}=${v}`).join("\n"));
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
      const confirmedAt = isConfirmed ? (tx.status.block_time || seenAt) : "";

      // Retry accounting: if we still don't have serial, allow multiple re-tries
      const retryKey = `retry:${txid}`;
      const currentRetries = parseInt((await REDIS.get(retryKey)) || "0", 10);
      const hasSerial = !!serial;

      // If we never found a real id, at least try i0 so the record carries something
      const storedInscriptionId = inscriptionId || `${uniqCandidates[0] || txid}i0`;

      // Build write
      const pipe = REDIS.multi()
        .hset(txKey, {
          inscriptionId: storedInscriptionId,
          buyerAddr: buyerAddr || "",
          watchedAddr: address,
          status: isConfirmed ? "confirmed" : "unconfirmed",
          seenAt: String(seenAt),
          confirmedAt: String(confirmedAt || ""),
          serial: hasSerial ? serial : (await getExistingField(txKey, "serial")) || "",
          pngText: pngText ? JSON.stringify(pngText) : (await getExistingField(txKey, "pngText")) || "",
          lastScanAt: String(seenAt),
        })
        .sadd(`addr:${address}:txs`, txid)
        .expire(txKey, 60 * 60 * 24 * 30); // keep 30 days

      if (hasSerial || isConfirmed || currentRetries + 1 >= SCAN_RETRY_LIMIT) {
        // Mark complete if: we found serial OR it's confirmed OR we've retried enough
        pipe.sadd("seen_txids", txid).del(retryKey);
      } else {
        // Increment retry counter and keep trying on next scans
        pipe.incr(retryKey).expire(retryKey, SCAN_RETRY_TTL_SEC);
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

async function fetchChainTxs(address, pages) {
  const out = [];
  let last = null;
  for (let i = 0; i < pages; i++) {
    const url = last
      ? `${MEMPOOL}/address/${address}/txs/chain/${last}`
      : `${MEMPOOL}/address/${address}/txs`;
    const page = await safeGetJSON(url).catch(() => []);
    if (!page || !page.length) break;
    out.push(...page);
    last = page[page.length - 1]?.txid;
    if (!last) break;
  }
  return out;
}

async function safeGetJSON(url) {
  const r = await fetch(url, { timeout: 15000 });
  if (!r.ok) throw new Error(`${url} -> ${r.status}`);
  return r.json();
}

async function fetchBinary(url) {
  const r = await fetch(url, { timeout: 15000 });
  if (!r.ok) throw new Error(`${url} -> ${r.status}`);
  return Buffer.from(await r.arrayBuffer());
}

// Try indices i0..i5; return first that looks like PNG
async function findPngInscriptionId(txid, maxIndex = 5) {
  for (let i = 0; i <= maxIndex; i++) {
    const id = `${txid}i${i}`;
    try {
      const buf = await fetchBinary(`${CONTENT_HOST}/${id}`);
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
          if (compFlag?.[0] === 1) v = zlib.unzipSync(payload).toString("utf-8");
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
        (obj?.name && /^[A-Za-z0-9]{10,24}$/.test(String(obj.name)) ? obj.name : null);
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
