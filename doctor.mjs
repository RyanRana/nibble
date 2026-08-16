#!/usr/bin/env node
/**
 * nibble doctor - what is this Redis wasting, and fix it.
 *
 *   node doctor.mjs [redis://...] [--json] [--fix [--apply [--persist]]]
 *
 * One file, zero dependencies. Read-only unless you pass --apply.
 */

import net from 'node:net';
import tls from 'node:tls';

// ───────────────────────────── RESP client ──────────────────────────────
// A Redis client in ~110 lines, so that this file needs no npm install.

function findCRLF(buf, from) {
  for (let i = from; i + 1 < buf.length; i++) if (buf[i] === 13 && buf[i + 1] === 10) return i;
  return -1;
}

/** Returns [value, nextOffset] or null when the buffer holds a partial reply. */
function parse(buf, off) {
  if (off >= buf.length) return null;
  const type = buf[off];
  const crlf = findCRLF(buf, off + 1);
  if (crlf === -1) return null;
  const line = buf.subarray(off + 1, crlf).toString('latin1');
  const next = crlf + 2;

  switch (type) {
    case 0x2b: return [line, next];                       // +simple
    case 0x2d: return [{ err: line }, next];              // -error
    case 0x3a: return [Number(line), next];               // :integer
    case 0x24: {                                          // $bulk
      const len = Number(line);
      if (len === -1) return [null, next];
      if (next + len + 2 > buf.length) return null;
      return [buf.subarray(next, next + len), next + len + 2];
    }
    case 0x2a: {                                          // *array
      const n = Number(line);
      if (n === -1) return [null, next];
      const arr = [];
      let o = next;
      for (let i = 0; i < n; i++) {
        const r = parse(buf, o);
        if (!r) return null;
        arr.push(r[0]);
        o = r[1];
      }
      return [arr, o];
    }
    default: return [{ err: `unexpected RESP type 0x${type.toString(16)}` }, next];
  }
}

function encode(args) {
  const parts = [Buffer.from(`*${args.length}\r\n`)];
  for (const a of args) {
    const b = Buffer.isBuffer(a) ? a : Buffer.from(String(a));
    parts.push(Buffer.from(`$${b.length}\r\n`), b, Buffer.from('\r\n'));
  }
  return Buffer.concat(parts);
}

class Client {
  constructor(sock) {
    this.sock = sock;
    this.buf = Buffer.alloc(0);
    this.waiting = [];
    sock.on('data', (chunk) => {
      this.buf = this.buf.length ? Buffer.concat([this.buf, chunk]) : chunk;
      let off = 0;
      for (;;) {
        const r = parse(this.buf, off);
        if (!r) break;
        off = r[1];
        const w = this.waiting.shift();
        if (w) w(r[0]);
      }
      this.buf = off ? this.buf.subarray(off) : this.buf;
    });
    sock.on('error', (e) => {
      const w = this.waiting.shift();
      if (w) w({ err: String(e.message) });
    });
  }

  static async connect(url) {
    const u = new URL(url);
    const secure = u.protocol === 'rediss:';
    const port = Number(u.port || 6379);
    const host = u.hostname || '127.0.0.1';
    const sock = await new Promise((res, rej) => {
      const s = secure
        ? tls.connect({ host, port, servername: host, rejectUnauthorized: false }, () => res(s))
        : net.createConnection({ host, port }, () => res(s));
      s.setNoDelay(true);
      s.once('error', rej);
    });
    const c = new Client(sock);
    if (u.password) {
      const reply = u.username
        ? await c.cmd('AUTH', decodeURIComponent(u.username), decodeURIComponent(u.password))
        : await c.cmd('AUTH', decodeURIComponent(u.password));
      if (reply?.err) throw new Error(`AUTH failed: ${reply.err}`);
    }
    const db = u.pathname?.slice(1);
    if (db) await c.cmd('SELECT', db);
    return c;
  }

  cmd(...args) {
    return new Promise((res) => {
      this.waiting.push(res);
      this.sock.write(encode(args));
    });
  }

  /** Fire a batch and collect replies in order - one round trip. */
  pipeline(cmds) {
    return new Promise((res) => {
      const out = [];
      let left = cmds.length;
      if (!left) return res(out);
      cmds.forEach((_, i) => {
        this.waiting.push((v) => {
          out[i] = v;
          if (--left === 0) res(out);
        });
      });
      this.sock.write(Buffer.concat(cmds.map(encode)));
    });
  }

  close() { this.sock.destroy(); }
}

const str = (v) => (Buffer.isBuffer(v) ? v.toString('utf8') : v == null || v.err ? '' : String(v));
const num = (v) => (typeof v === 'number' ? v : Number(str(v)) || 0);

// ─────────────────────────── allocator model ────────────────────────────

/** jemalloc size classes as Redis 8 ships them. */
function sizeClass(n) {
  if (n <= 8) return 8;
  if (n <= 128) return Math.ceil(n / 16) * 16;
  const pow = Math.floor(Math.log2(n - 1));
  const step = 2 ** (pow - 2);
  return Math.ceil(n / step) * step;
}
const sdsHeader = (len) => (len < 256 ? 3 : len < 65536 ? 5 : 9);
/** Bytes lost to size-class rounding by a string value of `len` bytes. */
const stringWaste = (len) => {
  const need = len + sdsHeader(len) + 1;
  return sizeClass(need) - need;
};

// ────────────────────────── keyspace sampling ───────────────────────────

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const HEX_RE = /^[0-9a-f]{8,}$/i;
const NUM_RE = /^-?\d+$/;
const IDISH_RE = /^[A-Za-z0-9_-]{16,}$/;

/** Collapse a concrete key to the shape of its namespace. */
function patternOf(key) {
  return key.split(':').map((seg) => {
    if (/^\{.*\}$/.test(seg)) return '{*}';
    if (UUID_RE.test(seg)) return '<uuid>';
    if (NUM_RE.test(seg)) return '<n>';
    if (HEX_RE.test(seg)) return '<hex>';
    if (IDISH_RE.test(seg)) return '<id>';
    return seg;
  }).join(':');
}

async function sampleKeyspace(c, limit) {
  const keys = [];
  let cursor = '0';
  do {
    const r = await c.cmd('SCAN', cursor, 'COUNT', 512);
    if (r?.err) throw new Error(r.err);
    cursor = str(r[0]);
    for (const k of r[1]) keys.push(str(k));
  } while (cursor !== '0' && keys.length < limit);
  return keys.slice(0, limit);
}

async function inspect(c, keys) {
  const out = [];
  for (let i = 0; i < keys.length; i += 128) {
    const chunk = keys.slice(i, i + 128);

    const meta = await c.pipeline(chunk.flatMap((k) => [
      ['TYPE', k], ['MEMORY', 'USAGE', k, 'SAMPLES', '5'], ['TTL', k], ['OBJECT', 'ENCODING', k],
    ]));

    const sizeCmds = [];
    const types = [];
    chunk.forEach((k, j) => {
      const t = str(meta[j * 4]);
      types.push(t);
      if (t === 'string') sizeCmds.push(['STRLEN', k], ['GETRANGE', k, '0', '127']);
      else if (t === 'hash') sizeCmds.push(['HLEN', k], ['PING']);
      else if (t === 'list') sizeCmds.push(['LLEN', k], ['PING']);
      else if (t === 'set') sizeCmds.push(['SCARD', k], ['SRANDMEMBER', k, '3']);
      else if (t === 'zset') sizeCmds.push(['ZCARD', k], ['PING']);
      else if (t === 'stream') sizeCmds.push(['XLEN', k], ['PING']);
      else sizeCmds.push(['PING'], ['PING']);
    });
    const sizes = await c.pipeline(sizeCmds);

    chunk.forEach((k, j) => {
      const type = types[j];
      if (!type || type === 'none') return;
      out.push({
        key: k,
        pattern: patternOf(k),
        type,
        encoding: str(meta[j * 4 + 3]),
        memory: num(meta[j * 4 + 1]),
        ttl: num(meta[j * 4 + 2]),
        size: num(sizes[j * 2]),
        head: type === 'string' ? sizes[j * 2 + 1] : null,
        members: type === 'set' ? sizes[j * 2 + 1] : null,
      });
    });
  }
  return out;
}

/** Every key of a type, bounded. Big keys are invisible to random sampling. */
async function scanByType(c, type, dbsize) {
  const COUNT = 5000;
  // Enough iterations to cover the whole keyspace on anything up to ~2M keys,
  // hard-capped so a 200M-key instance still answers in about a second.
  const maxIters = Math.min(400, Math.ceil(dbsize / COUNT) + 2);
  const found = [];
  let cursor = '0', iters = 0, complete = true;
  do {
    const r = await c.cmd('SCAN', cursor, 'COUNT', COUNT, 'TYPE', type);
    if (!Array.isArray(r)) break;
    cursor = str(r[0]);
    for (const k of r[1]) found.push(str(k));
    if (++iters >= maxIters && cursor !== '0') { complete = false; break; }
  } while (cursor !== '0');
  found.complete = complete;
  return found;
}

function groupNamespaces(samples, dbsize, scanned) {
  const by = new Map();
  for (const s of samples) {
    const g = by.get(s.pattern);
    if (g) g.push(s); else by.set(s.pattern, [s]);
  }
  const out = [];
  for (const [pattern, group] of by) {
    const encodings = {};
    let withTtl = 0, mem = 0, bytes = 0;
    for (const s of group) {
      encodings[s.encoding] = (encodings[s.encoding] ?? 0) + 1;
      if (s.ttl > 0) withTtl++;
      mem += s.memory;
      if (s.type === 'string') bytes += s.size;
    }
    const share = group.length / scanned;
    out.push({
      pattern,
      type: group[0].type,
      sampled: group,
      n: group.length,
      estimatedKeys: Math.round(share * dbsize),
      avgMemory: mem / group.length,
      avgValueBytes: group[0].type === 'string' ? bytes / group.length : 0,
      encodings,
      ttlCoverage: withTtl / group.length,
      estimatedBytes: (mem / group.length) * Math.round(share * dbsize),
    });
  }
  return out.sort((a, b) => b.estimatedBytes - a.estimatedBytes);
}

// ──────────────────────────────── checks ────────────────────────────────

const CRIT = 'critical', WARN = 'warning', OPP = 'opportunity', INFO = 'info';

function fmtBytes(n) {
  const a = Math.abs(n);
  if (a >= 1024 ** 3) return `${(n / 1024 ** 3).toFixed(2)} GiB`;
  if (a >= 1024 ** 2) return `${(n / 1024 ** 2).toFixed(1)} MiB`;
  if (a >= 1024) return `${(n / 1024).toFixed(1)} KiB`;
  return `${Math.round(n)} B`;
}

const DOLLARS_PER_GIB_MONTH = Number(process.env.NIBBLE_USD_PER_GIB || 12.93);
const dollars = (bytes) => (bytes / 1024 ** 3) * DOLLARS_PER_GIB_MONTH;

function looksJson(head) {
  if (!head || !head.length) return false;
  const c = head[0];
  return c === 0x7b || c === 0x5b; // { or [
}

/** Common embedding dimensions, for sniffing raw float32 blobs. */
const DIMS = new Set([256, 384, 512, 768, 1024, 1536, 2048, 3072, 4096]);

async function runChecks(c, ctx) {
  const f = [];
  const { config, namespaces, dbsize, used, version } = ctx;
  const add = (x) => f.push({ saving: 0, ...x });

  // Report a saving when it is material RELATIVE to this instance. A flat byte
  // floor either spams a 200 GiB instance or goes silent on a 2 GiB one.
  const floor = Math.max(512 * 1024, used * 0.01);
  const material = (bytes) => bytes >= floor;

  // ── persistence and eviction: the ways you lose data ────────────────
  const policy = config['maxmemory-policy'];
  const maxmemory = Number(config['maxmemory'] || 0);
  const noTtlNamespaces = namespaces.filter((n) => n.ttlCoverage < 0.5 && n.estimatedKeys > 100);
  const keysWithoutTtl = noTtlNamespaces.reduce((a, n) => a + n.estimatedKeys, 0);

  if (policy?.startsWith('allkeys') && keysWithoutTtl > 0) {
    add({
      severity: CRIT,
      title: `maxmemory-policy is "${policy}" and ~${keysWithoutTtl.toLocaleString()} keys have no TTL`,
      detail:
        'Under an allkeys-* policy every key is an eviction candidate, including durable records. ' +
        'When the instance fills, Redis will delete them and report success on the write that caused it. ' +
        'Measured on a test instance: allkeys-lru destroyed 99.8% of durable records with zero errors raised.',
      fix: 'Switch to `volatile-ttl` and give cache entries a TTL while durable records get none. ' +
           'Keys with no TTL are never eviction candidates under any volatile-* policy.',
      doc: 'docs/production.md#eviction',
    });
  }

  if (maxmemory === 0) {
    add({
      severity: WARN,
      title: 'maxmemory is unset (0 = unlimited)',
      detail: 'Redis will grow until the OS OOM-killer takes it, which loses everything in RAM at once ' +
              'and gives you no signal beforehand.',
      fix: 'Set maxmemory to 50–65% of the node\'s RAM. Redis can use ~2× during a BGSAVE fork, ' +
           'and the managed providers reserve 20–25% for exactly this reason.',
    });
  }

  const aof = config['appendonly'] === 'yes';
  const save = (config['save'] || '').trim();
  if (!aof && !save) {
    add({
      severity: CRIT,
      title: 'no persistence is configured at all (appendonly no, save disabled)',
      detail: 'A crash or restart loses 100% of this dataset. Measured: SIGKILL with these settings ' +
              'destroyed 20,000 of 20,000 acknowledged writes.',
      fix: 'If this instance is a cache and everything in it is reconstructible, ignore this. ' +
           'Otherwise set `appendonly yes` - with `appendfsync everysec` it survived 20,000/20,000.',
      doc: 'docs/production.md#durability',
    });
  } else if (!aof) {
    add({
      severity: WARN,
      title: `persistence is RDB snapshots only (save "${save}")`,
      detail: 'Everything written since the last snapshot is lost on a crash.',
      fix: 'Set `appendonly yes` for a bounded loss window.',
      doc: 'docs/production.md#durability',
    });
  }

  if (aof && config['no-appendfsync-on-rewrite'] === 'yes') {
    add({
      severity: WARN,
      title: 'no-appendfsync-on-rewrite is yes - this silently weakens your AOF',
      detail: 'During every AOF rewrite this degrades durability to `appendfsync no` (~30 s of exposure). ' +
              'It returns before the `always` branch, so it defeats `appendfsync always` entirely.',
      fix: 'Set it to `no` unless you have measured the latency spike it was added to avoid.',
    });
  }

  if (config['maxmemory-clients'] === '0' && maxmemory > 0) {
    add({
      severity: WARN,
      title: 'maxmemory-clients is 0 (unlimited client buffers)',
      detail: 'One slow consumer issuing a large read can grow its output buffer until the instance ' +
              'evicts or OOMs - without your dataset growing at all.',
      fix: 'Set `maxmemory-clients 5%`.',
    });
  }

  // ── per-key overhead: the big structural one ───────────────────────
  for (const ns of namespaces) {
    if (ns.estimatedKeys < 10_000) continue;
    if (ns.pattern.includes('{*}')) continue; // already sharded

    let overheadPerKey;
    if (ns.type === 'string') {
      const payload = sizeClass(ns.avgValueBytes + sdsHeader(ns.avgValueBytes) + 1);
      overheadPerKey = Math.max(0, ns.avgMemory - payload);
    } else {
      // containers: estimate the key's own cost (dictEntry + robj + key sds)
      const keyLen = ns.sampled.reduce((a, s) => a + s.key.length, 0) / ns.n;
      overheadPerKey = 24 + 16 + sizeClass(keyLen + 4) + 8;
    }
    // sharding amortizes key overhead across ~124 records
    const saving = overheadPerKey * ns.estimatedKeys * (1 - 1 / 124);
    if (!material(saving)) continue;

    add({
      severity: OPP,
      saving,
      title: `${ns.pattern} - ~${ns.estimatedKeys.toLocaleString()} keys paying ~${Math.round(overheadPerKey)} B each in key overhead`,
      detail:
        `Every Redis key costs dictEntry + robj + the key's own string before your data. ` +
        `This namespace averages ${fmtBytes(ns.avgMemory)}/key of which ~${Math.round(overheadPerKey)} B is bookkeeping.`,
      fix: 'Store these records as fields inside shared hashes (~124 per hash) instead of one key each. ' +
           'Key overhead is then paid once per 124 records.',
      doc: 'docs/records.md',
    });
  }

  // ── size-class rounding waste ──────────────────────────────────────
  for (const ns of namespaces) {
    if (ns.type !== 'string' || ns.estimatedKeys < 10_000) continue;
    const waste = ns.sampled.reduce((a, s) => a + stringWaste(s.size), 0) / ns.n;
    const saving = waste * ns.estimatedKeys;
    if (waste < 16 || !material(saving)) continue;
    add({
      severity: OPP,
      saving,
      title: `${ns.pattern} - ~${Math.round(waste)} B/key lost to allocator rounding`,
      detail:
        `Values average ${Math.round(ns.avgValueBytes)} B, which rounds up to the ` +
        `${sizeClass(ns.avgValueBytes + sdsHeader(ns.avgValueBytes) + 1)} B size class. ` +
        'jemalloc charges by class, not by byte.',
      fix: `Shrink the encoded value to land just under a class boundary, or pack these records into ` +
           `a shared hash where the rounding is amortized.`,
      doc: 'docs/tuning.md',
    });
  }

  // ── JSON payloads: measure the field-name tax exactly ──────────────
  for (const ns of namespaces) {
    if (ns.type !== 'string' || ns.estimatedKeys < 1_000) continue;
    const jsonish = ns.sampled.filter((s) => looksJson(s.head));
    if (jsonish.length < Math.max(3, ns.n * 0.5)) continue;

    // fetch a few complete values so the estimate is real, not extrapolated
    const probe = jsonish.slice(0, 12);
    const vals = await c.pipeline(probe.map((s) => ['GET', s.key]));
    let total = 0, nameBytes = 0, parsed = 0, arrayNumeric = 0, arrayLen = 0;
    for (const v of vals) {
      const text = str(v);
      if (!text) continue;
      try {
        const obj = JSON.parse(text);
        parsed++;
        total += Buffer.byteLength(text);
        if (Array.isArray(obj)) {
          if (obj.length >= 128 && obj.every((x) => typeof x === 'number')) {
            arrayNumeric++;
            arrayLen = obj.length;
          }
        } else if (obj && typeof obj === 'object') {
          // each field name costs its length + 2 quotes + 1 colon
          for (const k of Object.keys(obj)) nameBytes += k.length + 3;
        }
      } catch { /* not JSON after all */ }
    }
    if (!parsed) continue;

    if (arrayNumeric >= parsed * 0.6) {
      const jsonBytes = total / parsed;
      const int8Bytes = arrayLen + 4;
      const saving = (jsonBytes - int8Bytes) * ns.estimatedKeys;
      add({
        severity: OPP,
        saving,
        title: `${ns.pattern} - embeddings stored as JSON arrays (${arrayLen} dims)`,
        detail:
          `Each vector is ~${fmtBytes(jsonBytes)} of decimal text. As raw float32 it is ` +
          `${fmtBytes(arrayLen * 4)}; quantized to int8 it is ${fmtBytes(int8Bytes)}.`,
        fix: 'Store the vector as bytes, quantized to int8. Measured recall@10 for int8 held 98.7–100% ' +
             'across a full sweep of corpus difficulty. Do not use binary quantization without measuring ' +
             'recall on your own corpus first.',
        doc: 'docs/embeddings.md',
      });
    } else if (nameBytes > 0) {
      const avgNames = nameBytes / parsed;
      const avgTotal = total / parsed;
      const saving = avgNames * ns.estimatedKeys;
      if (!material(saving)) continue;
      add({
        severity: OPP,
        saving,
        title: `${ns.pattern} - ${((avgNames / avgTotal) * 100).toFixed(0)}% of these JSON bytes are field names`,
        detail:
          `Average value ${fmtBytes(avgTotal)}, of which ~${fmtBytes(avgNames)} is repeated key names. ` +
          'Every record re-ships the schema to a reader that already knows it.',
        fix: 'Encode with a positional schema (field names stored zero times). Measured 577 B of JSON ' +
             'down to 111 B. If you cannot change the format, dictionary-primed compression recovered ' +
             'most of it with no schema work.',
        doc: 'docs/packing.md',
      });
    }
  }

  // ── raw float32 embeddings ─────────────────────────────────────────
  for (const ns of namespaces) {
    if (ns.type !== 'string' || ns.estimatedKeys < 1_000) continue;
    const len = Math.round(ns.avgValueBytes);
    if (len % 4 !== 0 || !DIMS.has(len / 4)) continue;
    const spread = ns.sampled.every((s) => Math.abs(s.size - len) < 4);
    if (!spread) continue;
    const dim = len / 4;
    const saving = (len - (dim + 4)) * ns.estimatedKeys;
    add({
      severity: OPP,
      saving,
      title: `${ns.pattern} - looks like raw float32 embeddings (${dim} dims, ${len} B each)`,
      detail:
        `Fixed-width ${len} B values at a common embedding dimension. Note ${len} B + header rounds to ` +
        `the ${sizeClass(len + 4)} B size class, so you are also losing ` +
        `${fmtBytes(sizeClass(len + 4) - len - 4)}/vector to rounding.`,
      fix: `int8 quantization takes each vector to ${dim + 4} B. Measure recall@10 on your corpus before ` +
           'and after - it should barely move for int8.',
      doc: 'docs/embeddings.md',
    });
  }

  // ── encoding regressions ───────────────────────────────────────────
  for (const ns of namespaces) {
    if (ns.estimatedKeys < 100) continue;
    const enc = Object.keys(ns.encodings);
    const avgSize = ns.sampled.reduce((a, s) => a + s.size, 0) / ns.n;

    if (ns.type === 'hash' && enc.includes('hashtable') && avgSize <= 512) {
      add({
        severity: OPP,
        saving: 0,
        title: `${ns.pattern} - hashes are "hashtable"-encoded at only ~${Math.round(avgSize)} fields`,
        detail: 'A listpack-encoded hash of this size uses substantially less memory. Measured on ' +
                '20-field records: hashtable 1,142 B vs listpack 729 B.',
        fix: `Raise hash-max-listpack-entries above ${Math.ceil(avgSize)} (and hash-max-listpack-value ` +
             'above your largest field value). Note the conversion is one-way: existing hashes must be ' +
             'rewritten to pick up the new encoding.',
        doc: 'docs/tuning.md',
      });
    }
    if (ns.type === 'set' && !enc.includes('intset') && ns.members) {
      const members = Array.isArray(ns.members) ? ns.members.map(str) : [];
      if (members.length && members.every((m) => NUM_RE.test(m))) {
        add({
          severity: OPP,
          saving: 0,
          title: `${ns.pattern} - integer-only sets not using the intset encoding`,
          detail: 'Sets whose members are all integers can be stored as a packed sorted int64 array. ' +
                  'Measured: 54.1 B/member as strings vs 9.0 B/member as sharded intsets.',
          fix: 'Keep each set under set-max-intset-entries (default 512) by sharding, so the packed ' +
               'encoding is retained.',
          doc: 'docs/records.md',
        });
      }
    }
  }

  // ── ZSETs used as a hand-rolled time series ────────────────────────
  for (const ns of namespaces) {
    if (ns.type !== 'zset') continue;
    const avgMembers = ns.sampled.reduce((a, s) => a + s.size, 0) / ns.n;
    if (avgMembers < 500) continue;
    // A ZSET holding a sample per member costs ~88 B/sample; RedisTimeSeries
    // COMPRESSED measured 2.1 B/sample for the same data, and stays exact.
    const perSample = ns.avgMemory / avgMembers;
    if (perSample < 20) continue;
    const samples = avgMembers * ns.estimatedKeys;
    const saving = (perSample - 2.6) * samples;
    if (!material(saving)) continue;
    add({
      severity: OPP,
      saving,
      title: `${ns.pattern} - large sorted sets (~${Math.round(avgMembers).toLocaleString()} members) at ~${Math.round(perSample)} B/member`,
      detail:
        'If the score is a timestamp, this is a hand-rolled time series. Measured on 1M samples: ' +
        'a ZSET cost 88.6 B/sample, RedisTimeSeries COMPRESSED with integer values cost 2.1 B - ' +
        'and TimeSeries is exact and range-queryable.',
      fix: 'TS.CREATE … ENCODING COMPRESSED, and round values to integers before storing ' +
           '(float values measured 4× worse: 8.6 vs 2.1 B/sample).',
      doc: 'docs/metrics.md',
    });
  }

  // ── stream consumer-group backlogs ─────────────────────────────────
  //
  // A stream with a 30,000-message backlog is a single key, so a random sample
  // of 2,000 keys out of 60,000 misses it ~97% of the time. Big keys are
  // exactly the ones you cannot afford to miss, so scan for them by type.
  const streamKeys = await scanByType(c, 'stream', dbsize);
  if (streamKeys.length && !streamKeys.complete) {
    add({
      severity: INFO,
      title: `stream scan was partial - only the first ${streamKeys.length} streams were checked`,
      detail: 'The keyspace is large enough that scanning every key by type would have made this ' +
              'slow. Consumer-group backlogs on unscanned streams are not reflected below.',
      fix: 'Re-run against a smaller keyspace, or check the remaining streams with XINFO GROUPS.',
    });
  }
  for (const key of streamKeys.slice(0, 25)) {
    {
      const groups = await c.cmd('XINFO', 'GROUPS', key);
      if (!Array.isArray(groups)) continue;
      for (const g of groups) {
        if (!Array.isArray(g)) continue;
        const map = {};
        for (let i = 0; i + 1 < g.length; i += 2) map[str(g[i])] = g[i + 1];
        const pending = num(map.pending);
        if (pending > 1_000) {
          add({
            severity: WARN,
            saving: pending * 238,
            title: `${key} group "${str(map.name)}" has ${pending.toLocaleString()} un-acked messages`,
            detail:
              'Every delivered-but-un-acked message sits in the Pending Entries List, holding an id, a ' +
              'consumer, a timestamp and a delivery counter - measured at ~238 B each. XTRIM does NOT ' +
              'shrink the PEL, so MAXLEN will not save you here.',
            fix: 'XACK on success, and XAUTOCLAIM + XACK to reap messages abandoned by dead consumers.',
            doc: 'docs/production.md',
          });
        }
      }
    }
  }

  // ── free wins available on this version ────────────────────────────
  const [maj, min] = version.split('.').map(Number);
  const v810 = maj > 8 || (maj === 8 && min >= 10);
  const unsharded = namespaces.filter(
    (n) => n.type === 'hash' && !n.pattern.includes('{*}') && n.estimatedKeys > 10_000,
  );
  if (v810 && config['hash-min-template-entries'] === '0' && unsharded.length) {
    add({
      severity: OPP,
      saving: 0,
      title: 'Redis 8.10 hash templates are available and disabled',
      detail:
        'Hashes that share a schema can store their field names ONCE in a shared template. Measured ' +
        '729 → 409 B/run on 20-field records - a config change with no application change.',
      fix: 'CONFIG SET hash-min-template-entries 4. IMPORTANT: this HURTS sharded hashes, whose field ' +
           'names are unique ids (measured 20% worse). Enable it only if you keep one hash per record.',
      doc: 'docs/records.md',
    });
  }
  if (maj < 8 || (maj === 7 && min < 4)) {
    add({
      severity: INFO,
      title: `Redis ${version} - per-field TTLs (HEXPIRE) need 7.4+`,
      detail: 'Without HEXPIRE you cannot shard records that need individual expiry, which removes the ' +
              'largest single optimization from the table.',
      fix: 'Upgrade to 7.4+ to unlock per-field expiry, or 8.10+ for hash templates as well.',
    });
  }

  return f;
}


// ────────────────────────────── fixing ─────────────────────────────────
// Three tiers: `config` (CONFIG SET, reversible), `rewrite` (lossless -
// DUMP+RESTORE re-encodes under current config, keeping the TTL), and `refused`
// (lossy or needs your data model; prints instructions instead of guessing).

const REENCODE_LUA = `
local d = redis.call('DUMP', KEYS[1])
if not d then return 0 end
local t = redis.call('PTTL', KEYS[1])
if t < 0 then t = 0 end
redis.call('RESTORE', KEYS[1], tostring(t), d, 'REPLACE')
return 1`;

function planFixes(ctx) {
  const { config, namespaces, dbsize, version } = ctx;
  const plan = [];
  const policy = config['maxmemory-policy'];
  const keysWithoutTtl = namespaces
    .filter((n) => n.ttlCoverage < 0.5 && n.estimatedKeys > 100)
    .reduce((a, n) => a + n.estimatedKeys, 0);

  if (policy?.startsWith('allkeys') && keysWithoutTtl > 0) {
    plan.push({
      tier: 'config',
      why: 'stops Redis silently deleting durable records when the instance fills',
      what: `maxmemory-policy: ${policy} → volatile-ttl`,
      cmds: [['CONFIG', 'SET', 'maxmemory-policy', 'volatile-ttl']],
      rollback: `CONFIG SET maxmemory-policy ${policy}`,
      note: 'keys WITHOUT a TTL stop being eviction candidates. Give cache entries a TTL.',
    });
  }
  if (config['maxmemory-clients'] === '0' && Number(config['maxmemory'] || 0) > 0) {
    plan.push({
      tier: 'config',
      why: 'one slow consumer can no longer push the whole instance into eviction',
      what: 'maxmemory-clients: 0 (unlimited) → 5%',
      cmds: [['CONFIG', 'SET', 'maxmemory-clients', '5%']],
      rollback: 'CONFIG SET maxmemory-clients 0',
    });
  }
  if (config['appendonly'] === 'yes' && config['no-appendfsync-on-rewrite'] === 'yes') {
    plan.push({
      tier: 'config',
      why: 'restores your AOF guarantee during rewrites (this flag silently defeats appendfsync always)',
      what: 'no-appendfsync-on-rewrite: yes → no',
      cmds: [['CONFIG', 'SET', 'no-appendfsync-on-rewrite', 'no']],
      rollback: 'CONFIG SET no-appendfsync-on-rewrite yes',
    });
  }

  // raise the listpack threshold, then actually re-encode what is already wrong
  const shrinkable = namespaces.filter((n) => {
    if (n.type !== 'hash' || !Object.keys(n.encodings).includes('hashtable')) return false;
    const avg = n.sampled.reduce((a, x) => a + x.size, 0) / n.n;
    return avg > 0 && avg <= 512 && n.estimatedKeys >= 50;
  });
  if (shrinkable.length) {
    const maxFields = Math.max(
      ...shrinkable.map((n) => Math.ceil(n.sampled.reduce((a, x) => a + x.size, 0) / n.n)),
    );
    const target = Math.max(128, Math.min(1024, maxFields * 2));
    const current = Number(config['hash-max-listpack-entries'] || 128);
    if (target > current) {
      plan.push({
        tier: 'config',
        why: 'lets hashes of this size use the compact listpack encoding',
        what: `hash-max-listpack-entries: ${current} → ${target}`,
        cmds: [['CONFIG', 'SET', 'hash-max-listpack-entries', String(target)]],
        rollback: `CONFIG SET hash-max-listpack-entries ${current}`,
        note: 'affects NEW hashes only - the rewrite below fixes the existing ones',
      });
    }
    for (const n of shrinkable) {
      plan.push({
        tier: 'rewrite',
        why: 'existing hashes keep their old encoding forever; the conversion is one-way',
        what: `re-encode ~${n.estimatedKeys.toLocaleString()} keys matching ${n.pattern} to listpack`,
        reencode: n.pattern,
        estimated: n.estimatedBytes * 0.5,
        note: 'DUMP + RESTORE REPLACE per key, atomic, TTL preserved, bytes identical',
      });
    }
  }

  const refused = [];
  for (const n of namespaces) {
    if (n.type === 'string' && n.estimatedKeys > 1000) {
      const len = Math.round(n.avgValueBytes);
      if (len % 4 === 0 && DIMS.has(len / 4)) {
        refused.push(`${n.pattern}: quantize ${len / 4}-d float32 embeddings to int8 (${len} → ${len / 4 + 4} B). Lossy - measure recall@10 first. See docs/embeddings.md`);
      }
    }
  }
  if (dbsize > 0) {
    refused.push('JSON records → a positional schema: needs your data model, so nibble will not guess. See docs/packing.md');
  }
  return { plan, refused };
}

async function applyFixes(c, plan, opts) {
  const before = Number(/used_memory:(\d+)/.exec(str(await c.cmd('INFO', 'memory')))?.[1] ?? 0);
  let sha = null;

  for (const step of plan) {
    if (step.tier === 'config') {
      for (const cmd of step.cmds) {
        const r = await c.cmd(...cmd);
        if (r?.err) {
          console.log(`  ${C.red}✘${C.r} ${step.what} - ${r.err}`);
          continue;
        }
      }
      console.log(`  ${C.grn}✔${C.r} ${step.what}`);
    } else if (step.tier === 'rewrite') {
      if (!sha) sha = str(await c.cmd('SCRIPT', 'LOAD', REENCODE_LUA));
      // re-scan for the concrete keys behind this pattern
      let cursor = '0', done = 0, iters = 0;
      const t0 = Date.now();
      do {
        const r = await c.cmd('SCAN', cursor, 'COUNT', 1000, 'TYPE', 'hash');
        if (!Array.isArray(r)) break;
        cursor = str(r[0]);
        const keys = r[1].map(str).filter((k) => patternOf(k) === step.reencode);
        for (let i = 0; i < keys.length; i += 200) {
          const batch = keys.slice(i, i + 200);
          await c.pipeline(batch.map((k) => ['EVALSHA', sha, '1', k]));
          done += batch.length;
        }
      } while (cursor !== '0' && ++iters < 20_000);
      console.log(`  ${C.grn}✔${C.r} re-encoded ${done.toLocaleString()} keys matching ${step.reencode} (${Date.now() - t0} ms)`);
    }
  }

  if (opts.persist) {
    const r = await c.raw('CONFIG', 'REWRITE');
    console.log(r?.err
      ? `  ${C.yel}!${C.r} CONFIG REWRITE failed (${r.err}) - changes are runtime-only and will not survive a restart`
      : `  ${C.grn}✔${C.r} CONFIG REWRITE - changes persisted to the config file`);
  }

  try { await c.cmd('MEMORY', 'PURGE'); } catch { /* not jemalloc */ }
  const after = Number(/used_memory:(\d+)/.exec(str(await c.cmd('INFO', 'memory')))?.[1] ?? 0);
  return { before, after, freed: before - after };
}

function renderPlan(plan, refused, applying) {
  console.log(`\n${C.b}${applying ? 'Applying' : 'Fix plan'}${C.r}  ${C.dim}${plan.length} change(s)${C.r}\n`);
  if (!plan.length) {
    console.log('  Nothing safe left to change automatically.\n');
  }
  for (const step of plan) {
    if (applying) continue;
    const tag = step.tier === 'config'
      ? `${C.cyn}  config${C.r}`
      : `${C.yel} rewrite${C.r}`;
    console.log(`  ${tag}  ${C.b}${step.what}${C.r}` +
      (step.estimated ? `  ${C.grn}~${fmtBytes(step.estimated)}${C.r}` : ''));
    console.log(wrap(step.why, 84, '            '));
    if (step.note) console.log(wrap(`note: ${step.note}`, 84, '            '));
    if (step.rollback) console.log(`            ${C.dim}rollback: ${step.rollback}${C.r}`);
    console.log('');
  }
  if (refused.length && !applying) {
    console.log(`${C.b}Not doing automatically${C.r} ${C.dim}(lossy or needs your data model)${C.r}\n`);
    for (const r of refused) console.log(wrap(`• ${r}`, 86, '  '));
    console.log('');
  }
}

// ──────────────────────────────── report ────────────────────────────────

const C = process.stdout.isTTY
  ? { dim: '\x1b[2m', b: '\x1b[1m', r: '\x1b[0m', red: '\x1b[31m', yel: '\x1b[33m', grn: '\x1b[32m', cyn: '\x1b[36m' }
  : { dim: '', b: '', r: '', red: '', yel: '', grn: '', cyn: '' };

const BADGE = {
  [CRIT]: `${C.red}CRITICAL${C.r}`,
  [WARN]: `${C.yel} WARNING${C.r}`,
  [OPP]: `${C.cyn}  SAVING${C.r}`,
  [INFO]: `${C.dim}    INFO${C.r}`,
};
const ORDER = { [CRIT]: 0, [WARN]: 1, [OPP]: 2, [INFO]: 3 };

function wrap(text, width, indent) {
  const words = text.split(/\s+/);
  const lines = [];
  let line = '';
  for (const w of words) {
    if ((line + ' ' + w).trim().length > width) { lines.push(line.trim()); line = w; }
    else line += ' ' + w;
  }
  if (line.trim()) lines.push(line.trim());
  return lines.map((l) => indent + l).join('\n');
}

function report(ctx, findings) {
  const { version, used, dbsize, namespaces, scanned, elapsed } = ctx;

  console.log(`\n${C.b}█ nibble doctor${C.r}  ${C.dim}Redis ${version} · ${fmtBytes(used)} used · ${dbsize.toLocaleString()} keys · sampled ${scanned.toLocaleString()} in ${elapsed} ms${C.r}\n`);

  if (dbsize === 0) {
    console.log('  This instance is empty. Nothing to look at.\n');
    return;
  }

  // top namespaces by estimated footprint
  console.log(`${C.b}Where the memory is${C.r}`);
  const top = namespaces.slice(0, 8);
  const w = Math.max(...top.map((n) => n.pattern.length), 20);
  for (const n of top) {
    const pctOfUsed = used ? (n.estimatedBytes / used) * 100 : 0;
    const bar = '█'.repeat(Math.max(0, Math.round(pctOfUsed / 4)));
    console.log(
      `  ${n.pattern.padEnd(w)}  ${String(n.type).padEnd(6)} ${fmtBytes(n.estimatedBytes).padStart(9)}` +
      `  ${(pctOfUsed.toFixed(0) + '%').padStart(4)} ${C.dim}${bar}${C.r}` +
      `  ${C.dim}${n.estimatedKeys.toLocaleString()} keys · ${Object.keys(n.encodings).join(',')}${C.r}`,
    );
  }

  const savings = findings.reduce((a, x) => a + (x.saving || 0), 0);
  const problems = findings.filter((x) => x.severity === CRIT || x.severity === WARN);

  console.log(`\n${C.b}Findings${C.r}  ${C.dim}${problems.length} problem(s), ` +
    `${findings.length - problems.length} opportunity(ies)${C.r}\n`);

  if (!findings.length) {
    console.log(`  ${C.grn}✔${C.r} Nothing worth changing. This instance is configured safely and laid out well.\n`);
    return;
  }

  for (const x of findings) {
    const tag = x.saving ? `  ${C.grn}~${fmtBytes(x.saving)}${C.r}` : '';
    console.log(`  ${BADGE[x.severity]}  ${C.b}${x.title}${C.r}${tag}`);
    console.log(wrap(x.detail, 84, '            '));
    console.log(wrap(`→ ${x.fix}`, 84, '            '));
    if (x.doc) console.log(`            ${C.dim}${x.doc}${C.r}`);
    console.log('');
  }

  if (savings > 0) {
    const usd = dollars(savings);
    const usdText = usd < 1 ? `<$1` : `~$${usd.toFixed(0)}`;
    console.log(`${C.b}Estimated recoverable${C.r}  ${C.grn}${fmtBytes(savings)}${C.r}` +
      ` ${C.dim}(${usdText}/month at $${DOLLARS_PER_GIB_MONTH}/usable GiB)${C.r}`);
    console.log(`${C.dim}Estimates are extrapolated from a ${scanned.toLocaleString()}-key sample and assume ` +
      `a 124-record shard width. Measure before you commit.${C.r}\n`);
  }

  // the honest "you don't need this" verdict
  if (used < 2 * 1024 ** 3 && savings < 512 * 1024 * 1024) {
    console.log(`${C.dim}Verdict: this instance is small enough that layout work is not worth the ` +
      `complexity. Fix anything marked CRITICAL, ignore the rest until you are RAM-bound.${C.r}\n`);
  }
}

// ───────────────────────────────── main ─────────────────────────────────

const args = process.argv.slice(2);
const asJson = args.includes('--json');
const wantFix = args.includes('--fix');
const wantApply = args.includes('--apply');
const wantPersist = args.includes('--persist');
const url = args.find((a) => a.startsWith('redis://') || a.startsWith('rediss://'))
  || process.env.REDIS_URL
  || 'redis://127.0.0.1:6379';
const sampleArg = args.find((a) => a.startsWith('--sample='));
const sampleSize = sampleArg ? Number(sampleArg.split('=')[1]) : 2000;

const t0 = Date.now();
let c;
try {
  c = await Client.connect(url);
} catch (e) {
  console.error(`nibble doctor: cannot connect to ${url.replace(/:[^:@/]*@/, ':***@')} - ${e.message}`);
  process.exit(2);
}

const serverInfo = str(await c.cmd('INFO', 'server'));
const memInfo = str(await c.cmd('INFO', 'memory'));
const version = /redis_version:(\S+)/.exec(serverInfo)?.[1] ?? '0.0.0';
const used = Number(/used_memory:(\d+)/.exec(memInfo)?.[1] ?? 0);

const CONFIG_KEYS = [
  'maxmemory', 'maxmemory-policy', 'maxmemory-clients', 'appendonly', 'appendfsync',
  'no-appendfsync-on-rewrite', 'save', 'hash-max-listpack-entries', 'hash-max-listpack-value',
  'hash-min-template-entries', 'set-max-intset-entries', 'set-max-listpack-entries',
  'zset-max-listpack-entries', 'list-max-listpack-size', 'list-compress-depth', 'activedefrag',
];
const config = {};
const cfgReplies = await c.pipeline(CONFIG_KEYS.map((k) => ['CONFIG', 'GET', k]));
cfgReplies.forEach((r, i) => {
  if (Array.isArray(r) && r.length >= 2) config[CONFIG_KEYS[i]] = str(r[1]);
});

const dbsize = num(await c.cmd('DBSIZE'));
const keys = dbsize ? await sampleKeyspace(c, sampleSize) : [];
const samples = keys.length ? await inspect(c, keys) : [];
const namespaces = groupNamespaces(samples, dbsize, samples.length || 1);

const ctx = { config, namespaces, dbsize, used, version, scanned: samples.length, elapsed: Date.now() - t0 };
const findings = dbsize ? await runChecks(c, ctx) : [];
findings.sort((a, b) => (ORDER[a.severity] - ORDER[b.severity]) || (b.saving - a.saving));
ctx.elapsed = Date.now() - t0;

if (wantFix) {
  const { plan, refused } = planFixes(ctx);
  if (!wantApply) {
    report(ctx, findings);
    renderPlan(plan, refused, false);
    console.log(`${C.dim}Nothing has been changed. Re-run with ${C.r}${C.b}--apply${C.r}${C.dim} to execute` +
      `, and ${C.r}${C.b}--persist${C.r}${C.dim} to also CONFIG REWRITE so it survives a restart.${C.r}\n`);
  } else {
    renderPlan(plan, refused, true);
    const res = await applyFixes(c, plan, { persist: wantPersist });
    console.log('');
    if (res.freed > 0) {
      console.log(`${C.b}Freed${C.r}  ${C.grn}${fmtBytes(res.freed)}${C.r} ${C.dim}` +
        `(${fmtBytes(res.before)} → ${fmtBytes(res.after)}, measured)${C.r}`);
    } else {
      console.log(`${C.dim}used_memory ${fmtBytes(res.before)} → ${fmtBytes(res.after)}. ` +
        `Config-only changes free nothing immediately; they change what NEW data costs.${C.r}`);
    }
    if (!wantPersist) {
      console.log(`${C.yel}!${C.r} Runtime-only. These revert on restart unless you re-run with --persist ` +
        `(or set them in your config file / parameter group).`);
    }
    console.log('');
  }
  c.close();
  process.exit(0);
}


c.close();

if (asJson) {
  console.log(JSON.stringify({
    redis: version,
    usedMemory: used,
    dbsize,
    scanned: ctx.scanned,
    elapsedMs: ctx.elapsed,
    config,
    namespaces: namespaces.map(({ sampled, members, ...n }) => n),
    findings: findings.map(({ saving, ...x }) => ({ ...x, estimatedSavingBytes: Math.round(saving) })),
    estimatedRecoverableBytes: Math.round(findings.reduce((a, x) => a + (x.saving || 0), 0)),
  }, null, 2));
} else {
  report(ctx, findings);
}

process.exit(findings.some((x) => x.severity === CRIT) ? 1 : 0);
