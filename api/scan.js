// api/scan.js
import { Redis } from "@upstash/redis";
import fetch from "node-fetch";
import zlib from "zlib";

const REDIS = new Redis({
  url: process.env.UPSTASH_REDIS_REST_URL,
  token: process.env.UPSTASH_REDIS_REST_TOKEN
});

const MEMPOOL = "https://mempool.space/api";
const CONTENT_HOST = process.env.INSCRIPTION_CONTENT_HOST || "https://ordinals.com/content";
const PNG_SIG = Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]);
const WATCH_ADDRESSES = (process.env.WATCH_ADDRESSES || "").split(",").map(s => s.trim()).filter(Boolean).slice(0, 20);
const CHAIN_PAGES = Math.max(1, parseInt(process.env.SCAN_CHAIN_PAGES || "1", 10));
const PNG_TEXT_KEY_HINT = process.env.PNG_TEXT_KEY_HINT || "Serial";

export default async function handler(req, res) {
  if (!WATCH_ADDRESSES.length) {
    return res.status(400).json({ error: "Set WATCH_ADDRESSES env var (comma-separated BTC addresses)" });
  }
  try {
    const results = [];
    for (const addr of WATCH_ADDRESSES) {
      const out = await scanAddress(addr);
      results.push({ address: addr, found: out.length });
    }
    return res.status(200).json({ ok: true, results });
  } catch (e) {
    console.error(e);
    return res.status(500).json({ ok: false, error: String(e) });
  }
}

async function scanAddress(address) {
  const mempoolTxs = await safeGetJSON(`${MEMPOOL}/address/${address}/txs/mempool`).catch(() => []);
  const chainTxs = await fetchChainTxs(address, CHAIN_PAGES);

  // De-dupe by txid
  const seen = new Set();
  const all = [];
  for (const t of [...mempoolTxs, ...chainTxs]) {
    if (!seen.has(t.txid)) {
      seen.add(t.txid);
      all.push(t);
    }
  }

  const hits = [];
  for (const tx of all) {
    const txid = tx.txid;
    // skip if we've already processed this txisd
    const already = await REDIS.sismember("seen_txids", txid);
    if (already) continue;

    const outspends = await safeGetJSON(`${MEMPOOL}/tx/${txid}/outspends`).catch(() => null);
    if (!outspends) continue;

    const vouts = tx.vout || [];
    // Heuristic similar to your existing logic: find small-value spend that reveals inscription
    for (let idx = 0; idx < Math.min(outspends.length, vouts.length); idx++) {
      const spent = outspends[idx];
      const vout = vouts[idx] || {};
      if (!spent?.spent) continue;

      // Skip the fee output itself and large change outputs
      const outAddr = vout.scriptpubkey_address;
      const value = vout.value || 0;
      if (outAddr === address) continue;
      if (value > 10000) continue;

      // The tx that spent this output is the reveal (inscription) tx
      const revealTxid = spent.txid;
      if (!revealTxid) continue;

      const inscriptionId = `${revealTxid}i0`;

      // Buyer address: often the first input's address (payer)
      const buyerAddr = firstInputAddress(tx);

      // Determine status & timestamps
      const isConfirmed = !!tx?.status?.confirmed;
      const seenAt = Math.floor(Date.now() / 1000);
      const confirmedAt = isConfirmed ? (tx.status.block_time || seenAt) : null;

      // Fetch inscription content and parse PNG text chunks for serial
      let serial = null, pngText = null;
      try {
        const buf = await fetchBinary(`${CONTENT_HOST}/${inscriptionId}`);
        const { ok, text } = parsePngText(buf);
        if (ok) {
          pngText = text;
          // Loose extraction: if a key matches "Serial" or we find any 10-char digits
          serial = text[PNG_TEXT_KEY_HINT] || findSerialInTextMap(text);
        }
      } catch (_) { /* ignore content errors */ }

      // Save to Redis atomically
      const txKey = `tx:${txid}`;
      await REDIS.multi()
        .hset(txKey, {
          inscriptionId,
          buyerAddr: buyerAddr || "",
          watchedAddr: address,
          status: isConfirmed ? "confirmed" : "unconfirmed",
          seenAt: String(seenAt),
          confirmedAt: confirmedAt ? String(confirmedAt) : "",
          serial: serial || "",
          pngText: pngText ? JSON.stringify(pngText) : ""
        })
        .sadd(`addr:${address}:txs`, txid)
        .sadd("seen_txids", txid)
        // Optional: keep for 30 days
        .expire(txKey, 60 * 60 * 24 * 30)
        .exec();

      hits.push({ txid, inscriptionId, buyerAddr, serial, status: isConfirmed ? "confirmed" : "unconfirmed" });
    }
  }
  return hits;
}

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
  // First page
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

// ---- PNG text-chunk parsing (tEXt / zTXt / iTXt) ----
function parsePngText(buf) {
  if (!buf || buf.length < 8 || !buf.slice(0, 8).equals(PNG_SIG)) {
    return { ok: false, text: null };
  }
  const text = {};
  let off = 8;
  while (off + 8 <= buf.length) {
    const len = buf.readUInt32BE(off); off += 4;
    const type = buf.slice(off, off + 4).toString("latin1"); off += 4;
    const data = buf.slice(off, off + len); off += len;
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
            const v = zlib.unzipSync(compData).toString("latin1");
            text[k] = v;
          } catch (e) {
            text[k] = `<zTXt decompress error: ${e}>`;
          }
        }
      }
    } else if (type === "iTXt") {
      // keyword\0comp_flag\0comp_method\0lang\0translated\0text
      const parts = splitNul(data, 6);
      if (parts && parts.length === 6) {
        const [k, compFlag, compMethod, _lang, _trans, payload] = parts;
        try {
          let v;
          if (compFlag?.[0] === 1) {
            v = zlib.unzipSync(payload).toString("utf-8");
          } else {
            v = Buffer.from(payload).toString("utf-8");
          }
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

function findSerialInTextMap(obj) {
  // Try common keys or extract first 10-digit sequence
  const keys = Object.keys(obj);
  for (const k of keys) {
    if (/serial/i.test(k)) return obj[k];
  }
  const joined = keys.map(k => `${k}=${obj[k]}`).join("\n");
  const m = joined.match(/\b\d{10}\b/);
  return m ? m[0] : null;
}
