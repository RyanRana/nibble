/** MessagePack - benchmark only. Kept out of src/kit so the kit stays dependency-free. */
import { encode as mpEncode, decode as mpDecode } from '@msgpack/msgpack';

export function msgpack(v: unknown): Buffer {
  return Buffer.from(mpEncode(v));
}

export function unmsgpack(b: Buffer): unknown {
  return mpDecode(b);
}
