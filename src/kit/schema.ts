/**
 * Schema DSL for packed binary records.
 *
 * JSON ships field names once per record; a schema ships them zero times.
 * Add fields at the END and bump `version` — see docs/packing.md.
 */
import { Writer, Reader, uuidToBytes, bytesToUuid } from '../lib/codec.ts';

export interface FieldType<T> {
  /** Human-readable name, for error messages and docs generation. */
  kind: string;
  write: (w: Writer, v: T) => void;
  read: (r: Reader) => T;
  /** Rough bytes for a typical value — used by `describe()`, never for encoding. */
  typical: number;
}

/** A 36-char UUID string stored as its actual 16 bytes. 2.25× off the top. */
export function uuid(): FieldType<string> {
  return {
    kind: 'uuid',
    write: (w, v) => w.bytes(uuidToBytes(v)),
    read: (r) => bytesToUuid(r.bytes(16)),
    typical: 16,
  };
}

/** One byte, as long as you have fewer than 255 members. */
export function enum_<T extends string>(members: readonly T[]): FieldType<T> {
  if (members.length > 255) throw new Error('enum_ supports at most 255 members');
  const index = new Map(members.map((m, i) => [m, i]));
  return {
    kind: `enum(${members.length})`,
    write: (w, v) => {
      const i = index.get(v);
      if (i === undefined) throw new Error(`enum_: unknown member ${JSON.stringify(v)}`);
      w.u8(i);
    },
    read: (r) => members[r.u8()],
    typical: 1,
  };
}

/** LEB128. 1 byte below 128, 2 below 16384. Most agent counters are 1–3 bytes. */
export function varint(): FieldType<number> {
  return { kind: 'varint', write: (w, v) => w.varint(v), read: (r) => r.varint(), typical: 2 };
}

/** ZigZag varint — use for deltas, where small negatives must stay 1 byte. */
export function svarint(): FieldType<number> {
  return { kind: 'svarint', write: (w, v) => w.svarint(v), read: (r) => r.svarint(), typical: 2 };
}

export function u8(): FieldType<number> {
  return { kind: 'u8', write: (w, v) => w.u8(v), read: (r) => r.u8(), typical: 1 };
}

/** Length-prefixed UTF-8. The only variable-cost field — keep them rare. */
export function str(): FieldType<string> {
  return { kind: 'str', write: (w, v) => w.str(v), read: (r) => r.str(), typical: 24 };
}

/** Raw bytes with a length prefix. */
export function blob(): FieldType<Buffer> {
  return {
    kind: 'blob',
    write: (w, v) => { w.varint(v.length); w.bytes(v); },
    read: (r) => Buffer.from(r.bytes(r.varint())),
    typical: 32,
  };
}

/** Up to 8 booleans in one byte. Eight JSON booleans cost ~90. */
export function flags<K extends string>(names: readonly K[]): FieldType<Record<K, boolean>> {
  if (names.length > 8) throw new Error('flags() packs at most 8 booleans per byte');
  return {
    kind: `flags(${names.length})`,
    write: (w, v) => {
      let b = 0;
      names.forEach((n, i) => { if (v[n]) b |= 1 << i; });
      w.u8(b);
    },
    read: (r) => {
      const b = r.u8();
      const out = {} as Record<K, boolean>;
      names.forEach((n, i) => { out[n] = (b & (1 << i)) !== 0; });
      return out;
    },
    typical: 1,
  };
}

export type Shape = Record<string, FieldType<any>>;
export type Infer<S extends Shape> = { [K in keyof S]: S[K] extends FieldType<infer T> ? T : never };

export interface Codec<S extends Shape> {
  encode: (v: Infer<S>) => Buffer;
  decode: (b: Buffer) => Infer<S>;
  /** Reads ONLY the version byte — cheap enough to call on every record. */
  versionOf: (b: Buffer) => number;
  /** Estimated bytes for a typical record. For sizing, not for billing. */
  typical: number;
  describe: () => string;
}

export interface SchemaOpts {
  /** First byte. Bump when adding fields; short records decode with trailing fields undefined. */
  version?: number;
}

export function schema<S extends Shape>(shape: S, opts: SchemaOpts = {}): Codec<S> {
  const version = opts.version ?? 1;
  const entries = Object.entries(shape) as [keyof S & string, FieldType<any>][];
  const typical = 1 + entries.reduce((a, [, f]) => a + f.typical, 0);

  return {
    typical,
    versionOf: (b) => b[0],
    encode(v) {
      const w = new Writer(Math.max(32, typical * 2));
      w.u8(version);
      for (const [name, f] of entries) f.write(w, v[name]);
      return w.done();
    },
    decode(b) {
      const r = new Reader(b);
      r.u8();
      const out = {} as Infer<S>;
      for (const [name, f] of entries) {
        if (r.exhausted()) break; // older record — trailing fields stay undefined

        (out as any)[name] = f.read(r);
      }
      return out;
    },
    describe() {
      const rows = entries.map(([n, f]) => `  ${n.padEnd(20)} ${f.kind.padEnd(12)} ~${f.typical} B`);
      return `schema v${version} (~${typical} B typical)\n${rows.join('\n')}`;
    },
  };
}
