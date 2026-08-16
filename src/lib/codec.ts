/**
 * Encoders, by how much they know about your data: JSON knows nothing,
 * MessagePack knows types, schema-packing knows the shape, zstd knows the
 * entropy, dictionary-zstd knows the corpus.
 */
import zlib from 'node:zlib';
import type { AgentRun, ToolSpan } from './rng.ts';

// ─────────────────────────────── varints ────────────────────────────────

export class Writer {
  private buf: Buffer;
  private off = 0;
  constructor(cap = 512) {
    this.buf = Buffer.allocUnsafe(cap);
  }
  private need(n: number) {
    if (this.off + n <= this.buf.length) return;
    const next = Buffer.allocUnsafe(Math.max(this.buf.length * 2, this.off + n));
    this.buf.copy(next, 0, 0, this.off);
    this.buf = next;
  }
  u8(v: number) {
    this.need(1);
    this.buf[this.off++] = v & 0xff;
  }
  /** LEB128 - 1 byte for <128, 2 for <16384. Most agent counters are 1–3 bytes. */
  varint(v: number) {
    let n = v >>> 0;
    this.need(5);
    while (n >= 0x80) {
      this.buf[this.off++] = (n & 0x7f) | 0x80;
      n >>>= 7;
    }
    this.buf[this.off++] = n;
  }
  /** ZigZag so small negative deltas stay 1 byte. */
  svarint(v: number) {
    this.varint((v << 1) ^ (v >> 31));
  }
  bytes(b: Buffer) {
    this.need(b.length);
    b.copy(this.buf, this.off);
    this.off += b.length;
  }
  str(s: string) {
    const b = Buffer.from(s, 'utf8');
    this.varint(b.length);
    this.bytes(b);
  }
  done(): Buffer {
    return this.buf.subarray(0, this.off);
  }
}

export class Reader {
  private off = 0;
  private buf: Buffer;
  constructor(buf: Buffer) {
    this.buf = buf;
  }
  u8(): number {
    return this.buf[this.off++];
  }
  varint(): number {
    let shift = 0, res = 0, b: number;
    do {
      b = this.buf[this.off++];
      res |= (b & 0x7f) << shift;
      shift += 7;
    } while (b & 0x80);
    return res >>> 0;
  }
  svarint(): number {
    const v = this.varint();
    return (v >>> 1) ^ -(v & 1);
  }
  bytes(n: number): Buffer {
    const b = this.buf.subarray(this.off, this.off + n);
    this.off += n;
    return b;
  }
  str(): string {
    return this.bytes(this.varint()).toString('utf8');
  }
  /** True once every byte has been consumed. Lets a decoder stop early when
   *  reading a record written by an older, shorter schema version. */
  exhausted(): boolean {
    return this.off >= this.buf.length;
  }
}

/** 36-char UUID string ⇄ its actual 16 bytes. A 2.25× win before anything else. */
export function uuidToBytes(u: string): Buffer {
  return Buffer.from(u.replace(/-/g, ''), 'hex');
}
export function bytesToUuid(b: Buffer): string {
  const h = b.toString('hex');
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-${h.slice(12, 16)}-${h.slice(16, 20)}-${h.slice(20)}`;
}

// ───────────────────────── schema-packed records ─────────────────────────

const STATUSES = ['queued', 'running', 'awaiting_tool', 'succeeded', 'failed', 'cancelled'];
const MODELS = ['claude-opus-5', 'claude-sonnet-5', 'claude-haiku-4-5-20251001', 'claude-fable-5'];
const REGIONS = ['iad1', 'sfo1', 'fra1', 'hnd1', 'syd1'];
const TOOLS = [
  'web_search', 'read_file', 'write_file', 'bash', 'sql_query',
  'vector_search', 'http_request', 'send_email', 'code_interpreter', 'browser',
];

const idx = (arr: string[], v: string) => {
  const i = arr.indexOf(v);
  return i < 0 ? 255 : i;
};

/** AgentRun → ~110 bytes. Same record as JSON's ~470. */
export function packRun(run: AgentRun): Buffer {
  const w = new Writer(160);
  w.u8(1); // schema version - migrations stay possible
  w.bytes(uuidToBytes(run.run_id));
  w.bytes(uuidToBytes(run.tenant_id));
  w.bytes(uuidToBytes(run.agent_id));
  w.bytes(uuidToBytes(run.parent_run_id));
  w.bytes(uuidToBytes(run.thread_id));
  w.u8(idx(STATUSES, run.status));
  w.u8(idx(MODELS, run.model));
  w.u8(idx(REGIONS, run.region));
  w.varint(run.created_at);
  // timestamps stored as deltas off created_at: seconds, not epochs
  w.svarint(run.updated_at - run.created_at);
  w.svarint(run.started_at - run.created_at);
  w.svarint(run.deadline_at - run.created_at);
  w.varint(run.step_count);
  w.varint(run.tool_call_count);
  w.varint(run.prompt_tokens);
  w.varint(run.completion_tokens);
  w.varint(run.cached_tokens);
  w.varint(run.cost_micros);
  w.varint(run.retry_count);
  w.varint(run.priority);
  return w.done();
}

export function unpackRun(buf: Buffer): AgentRun {
  const r = new Reader(buf);
  r.u8();
  const run_id = bytesToUuid(r.bytes(16));
  const tenant_id = bytesToUuid(r.bytes(16));
  const agent_id = bytesToUuid(r.bytes(16));
  const parent_run_id = bytesToUuid(r.bytes(16));
  const thread_id = bytesToUuid(r.bytes(16));
  const status = STATUSES[r.u8()];
  const model = MODELS[r.u8()];
  const region = REGIONS[r.u8()];
  const created_at = r.varint();
  const updated_at = created_at + r.svarint();
  const started_at = created_at + r.svarint();
  const deadline_at = created_at + r.svarint();
  return {
    run_id, tenant_id, agent_id, parent_run_id, thread_id,
    status, model, region, created_at, updated_at, started_at, deadline_at,
    step_count: r.varint(),
    tool_call_count: r.varint(),
    prompt_tokens: r.varint(),
    completion_tokens: r.varint(),
    cached_tokens: r.varint(),
    cost_micros: r.varint(),
    retry_count: r.varint(),
    priority: r.varint(),
  };
}

/** ToolSpan → ~40 bytes vs ~250 as JSON. Multiply by 50M/day and it matters. */
export function packSpan(s: ToolSpan, baseTsMs: number): Buffer {
  const w = new Writer(64);
  w.bytes(uuidToBytes(s.span_id));
  w.varint(s.step);
  w.u8(idx(TOOLS, s.tool));
  w.varint(Math.max(0, s.ts_ms - baseTsMs));
  w.varint(s.dur_ms);
  // five booleans/small enums folded into one byte
  w.u8((s.ok << 0) | (s.cache_hit << 1) | ((s.retry & 0x7) << 2));
  w.varint(s.input_bytes);
  w.varint(s.output_bytes);
  w.varint(s.http_status);
  return w.done();
}

export function unpackSpan(buf: Buffer, baseTsMs: number, runId: string): ToolSpan {
  const r = new Reader(buf);
  const span_id = bytesToUuid(r.bytes(16));
  const step = r.varint();
  const tool = TOOLS[r.u8()];
  const ts_ms = baseTsMs + r.varint();
  const dur_ms = r.varint();
  const flags = r.u8();
  return {
    span_id, run_id: runId, step, tool, ts_ms, dur_ms,
    ok: flags & 1,
    cache_hit: (flags >> 1) & 1,
    retry: (flags >> 2) & 7,
    input_bytes: r.varint(),
    output_bytes: r.varint(),
    http_status: r.varint(),
  };
}

// ───────────────────────────── compression ──────────────────────────────

export const ZSTD_LEVEL = 6;

export function zstd(b: Buffer, level = ZSTD_LEVEL): Buffer {
  return zlib.zstdCompressSync(b, {
    params: { [zlib.constants.ZSTD_c_compressionLevel]: level },
  });
}
export function unzstd(b: Buffer): Buffer {
  return zlib.zstdDecompressSync(b);
}

/**
 * Deflate with a preset dictionary.
 *
 * Small records are where naive compression falls over: a 300-byte JSON blob
 * has no history to back-reference, so zstd/gzip often make it *bigger*. A
 * preset dictionary hands the compressor a prebuilt history - the field names,
 * the enum values, the URL prefixes your records all share - so the very first
 * byte can be a back-reference. This is the single highest-leverage trick for
 * agentic workloads, which are millions of small, near-identical records.
 *
 * zlib caps the dictionary at 32 KiB and uses the tail if you pass more.
 */
export function makeDictionary(samples: Buffer[], maxBytes = 32 * 1024): Buffer {
  const joined = Buffer.concat(samples);
  return joined.length <= maxBytes ? joined : joined.subarray(joined.length - maxBytes);
}

export function deflateDict(b: Buffer, dictionary: Buffer): Buffer {
  return zlib.deflateRawSync(b, { dictionary, level: 9, memLevel: 9 });
}
export function inflateDict(b: Buffer, dictionary: Buffer): Buffer {
  return zlib.inflateRawSync(b, { dictionary });
}

// ──────────────────────────────── json ─────────────────────────────────
//
// msgpack lives in ./msgpack.ts - it is a benchmark comparison point, and
// keeping it out of here is what makes the kit dependency-free.

export function json(v: unknown): Buffer {
  return Buffer.from(JSON.stringify(v), 'utf8');
}

/** Short field names for hashes. Field names are stored once *per record*. */
export const RUN_SHORT_FIELDS: Record<keyof AgentRun, string> = {
  run_id: 'i', tenant_id: 't', agent_id: 'a', parent_run_id: 'p', thread_id: 'h',
  status: 's', model: 'm', created_at: 'c', updated_at: 'u', started_at: 'b',
  deadline_at: 'd', step_count: 'n', tool_call_count: 'k', prompt_tokens: 'P',
  completion_tokens: 'C', cached_tokens: 'K', cost_micros: '$', retry_count: 'r',
  priority: 'y', region: 'g',
};
