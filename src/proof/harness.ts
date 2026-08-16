/** Shared harness for the primary-database proofs. Each runs its own container. */
import { execFileSync } from 'node:child_process';
import Redis from 'ioredis';

export const IMAGE = 'redis:8-alpine';

export function sh(cmd: string, args: string[], allowFail = false): string {
  try {
    return execFileSync(cmd, args, { encoding: 'utf8', stdio: ['ignore', 'pipe', 'pipe'] });
  } catch (e: any) {
    if (allowFail) return String(e.stdout ?? '') + String(e.stderr ?? '');
    throw e;
  }
}

export function rmContainer(name: string): void {
  sh('docker', ['rm', '-f', name], true);
}

export interface StartOpts {
  name: string;
  port: number;
  /** Extra redis-server flags, e.g. ['--appendonly','yes','--appendfsync','everysec'] */
  args?: string[];
  /** Named docker volume for data that must survive `docker rm`. */
  volume?: string;
}

export function startRedis(o: StartOpts): void {
  rmContainer(o.name);
  const args = [
    'run', '-d', '--name', o.name,
    '-p', `${o.port}:6379`,
  ];
  if (o.volume) args.push('-v', `${o.volume}:/data`);
  args.push(IMAGE, 'redis-server', '--dir', '/data', ...(o.args ?? []));
  sh('docker', args);
}

/** SIGKILL — no shutdown hook, no final fsync, no chance to flush buffers. */
export function killRedis(name: string): void {
  sh('docker', ['kill', '-s', 'KILL', name], true);
}

export function startExisting(name: string): void {
  sh('docker', ['start', name]);
}

export async function waitReady(port: number, timeoutMs = 30_000): Promise<Redis> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const c = new Redis({
      host: '127.0.0.1', port, lazyConnect: true,
      maxRetriesPerRequest: 1, retryStrategy: () => null,
      enableOfflineQueue: false,
    });
    // the server is intentionally down while we poll; swallow the expected
    // ECONNREFUSED so it does not surface as an unhandled error event
    c.on('error', () => {});
    try {
      await c.connect();
      // PING succeeds during loading; use a real read to confirm the dataset is up
      for (;;) {
        try {
          await c.dbsize();
          return c;
        } catch (e: any) {
          if (!/LOADING/i.test(String(e?.message))) throw e;
          await sleep(100);
        }
      }
    } catch {
      c.disconnect();
      if (Date.now() > deadline) throw new Error(`redis on :${port} never became ready`);
      await sleep(200);
    }
  }
}

export const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

// ───────────────────────────── reporting ──────────────────────────────

const PASS = '\x1b[32m✔\x1b[0m';
const FAIL = '\x1b[31m✘\x1b[0m';
const INFO = '\x1b[36m·\x1b[0m';

export interface Claim {
  claim: string;
  ok: boolean;
  detail: string;
}

export class Report {
  readonly claims: Claim[] = [];
  readonly title: string;
  readonly objection: string;
  constructor(title: string, objection: string) {
    this.title = title;
    this.objection = objection;
    console.log(`\n\x1b[1m▸ ${title}\x1b[0m`);
    console.log(`  objection: "${objection}"\n`);
  }
  assert(claim: string, ok: boolean, detail = ''): void {
    this.claims.push({ claim, ok, detail });
    console.log(`  ${ok ? PASS : FAIL} ${claim}${detail ? `\n      ${detail}` : ''}`);
  }
  info(msg: string): void {
    console.log(`  ${INFO} ${msg}`);
  }
  get passed(): boolean {
    return this.claims.every((c) => c.ok);
  }
  summary(): { title: string; objection: string; passed: boolean; claims: Claim[] } {
    console.log(
      `\n  ${this.passed ? PASS : FAIL} ${this.claims.filter((c) => c.ok).length}/${this.claims.length} claims verified\n`,
    );
    return { title: this.title, objection: this.objection, passed: this.passed, claims: this.claims };
  }
}

export function pct(sorted: number[], p: number): number {
  if (!sorted.length) return 0;
  const i = Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length));
  return sorted[i];
}

export function stats(xs: number[]): Record<string, number> {
  const s = [...xs].sort((a, b) => a - b);
  return {
    p50: +pct(s, 50).toFixed(3),
    p95: +pct(s, 95).toFixed(3),
    p99: +pct(s, 99).toFixed(3),
    max: +(s[s.length - 1] ?? 0).toFixed(3),
  };
}
