/**
 * A Redis client, so nibble needs no npm install.
 *
 * RESP2 over TCP/TLS, AUTH, SELECT, pipelining. No cluster redirection, pub/sub
 * or reconnect — every primitive accepts anything exposing cmd/pipeline.
 * doctor.mjs carries its own copy on purpose: it must stay one file.
 */
import net from 'node:net';
import tls from 'node:tls';

export type Arg = string | number | Buffer;
export type Reply = Buffer | string | number | null | Reply[] | { err: string };

function findCRLF(buf: Buffer, from: number): number {
  for (let i = from; i + 1 < buf.length; i++) if (buf[i] === 13 && buf[i + 1] === 10) return i;
  return -1;
}

/** Returns [value, nextOffset], or null when the buffer holds a partial reply. */
function parse(buf: Buffer, off: number): [Reply, number] | null {
  if (off >= buf.length) return null;
  const type = buf[off];
  const crlf = findCRLF(buf, off + 1);
  if (crlf === -1) return null;
  const line = buf.subarray(off + 1, crlf).toString('latin1');
  const next = crlf + 2;

  switch (type) {
    case 0x2b: return [line, next];                        // +simple
    case 0x2d: return [{ err: line }, next];               // -error
    case 0x3a: return [Number(line), next];                // :integer
    case 0x24: {                                           // $bulk
      const len = Number(line);
      if (len === -1) return [null, next];
      if (next + len + 2 > buf.length) return null;
      return [buf.subarray(next, next + len), next + len + 2];
    }
    case 0x2a: {                                           // *array
      const n = Number(line);
      if (n === -1) return [null, next];
      const arr: Reply[] = [];
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

function encode(args: Arg[]): Buffer {
  const parts: Buffer[] = [Buffer.from(`*${args.length}\r\n`)];
  for (const a of args) {
    const b = Buffer.isBuffer(a) ? a : Buffer.from(String(a));
    parts.push(Buffer.from(`$${b.length}\r\n`), b, Buffer.from('\r\n'));
  }
  return Buffer.concat(parts);
}

export function isErr(v: Reply): v is { err: string } {
  return !!v && typeof v === 'object' && !Buffer.isBuffer(v) && !Array.isArray(v) && 'err' in v;
}

export function str(v: Reply): string {
  if (v == null) return '';
  if (Buffer.isBuffer(v)) return v.toString('utf8');
  if (isErr(v)) return '';
  return String(v);
}

export function num(v: Reply): number {
  if (typeof v === 'number') return v;
  const n = Number(str(v));
  return Number.isFinite(n) ? n : 0;
}

export class Client {
  private sock: net.Socket | tls.TLSSocket;
  private buf: Buffer = Buffer.alloc(0);
  private waiting: ((v: Reply) => void)[] = [];

  private constructor(sock: net.Socket | tls.TLSSocket) {
    this.sock = sock;
    sock.on('data', (chunk: Buffer) => {
      this.buf = this.buf.length ? Buffer.concat([this.buf, chunk]) : chunk;
      let off = 0;
      for (;;) {
        const r = parse(this.buf, off);
        if (!r) break;
        off = r[1];
        this.waiting.shift()?.(r[0]);
      }
      if (off) this.buf = this.buf.subarray(off);
    });
    sock.on('error', (e: Error) => {
      // fail the in-flight command rather than crashing the process
      this.waiting.shift()?.({ err: e.message });
    });
  }

  static async connect(url = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379'): Promise<Client> {
    const u = new URL(url);
    const port = Number(u.port || 6379);
    const host = u.hostname || '127.0.0.1';
    const sock = await new Promise<net.Socket | tls.TLSSocket>((res, rej) => {
      const s: net.Socket | tls.TLSSocket = u.protocol === 'rediss:'
        ? tls.connect({ host, port, servername: host }, () => res(s))
        : net.createConnection({ host, port }, () => res(s));
      s.setNoDelay(true);
      s.once('error', rej);
    });
    const c = new Client(sock);
    if (u.password) {
      const pass = decodeURIComponent(u.password);
      const reply = u.username
        ? await c.raw('AUTH', decodeURIComponent(u.username), pass)
        : await c.raw('AUTH', pass);
      if (isErr(reply)) throw new Error(`AUTH failed: ${reply.err}`);
    }
    const db = u.pathname?.slice(1);
    if (db) await c.cmd('SELECT', db);
    return c;
  }

  /** Send a command. Throws if Redis replies with an error. */
  async cmd(...args: Arg[]): Promise<Reply> {
    const v = await this.raw(...args);
    if (isErr(v)) throw new Error(`${args[0]}: ${v.err}`);
    return v;
  }

  /** Send a command, returning error replies instead of throwing. */
  raw(...args: Arg[]): Promise<Reply> {
    return new Promise((res) => {
      this.waiting.push(res);
      this.sock.write(encode(args));
    });
  }

  /** One round trip. Errors are returned in place, not thrown. */
  pipeline(cmds: Arg[][]): Promise<Reply[]> {
    return new Promise((res) => {
      const out: Reply[] = new Array(cmds.length);
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

  /** Bounded chunks — an unbounded batch balloons client buffers. */
  async pipelineChunked(cmds: Arg[][], size = 1000): Promise<void> {
    for (let i = 0; i < cmds.length; i += size) {
      const res = await this.pipeline(cmds.slice(i, i + size));
      for (const v of res) if (isErr(v)) throw new Error(v.err);
    }
  }

  close(): void {
    this.sock.destroy();
  }
}
