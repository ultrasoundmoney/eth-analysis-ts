import { performance } from "perf_hooks";
import * as Log from "./log.js";

export const logPerf = (name: string, t0: number): void => {
  const t1 = performance.now();
  const took = ((t1 - t0) / 1000).toFixed(2);
  Log.debug(`${name} took ${took}s`);
};

export const logPerfT = (name: string, t0: number) => () => () => {
  logPerf(name, t0);
};

export function withPerfLogAsync<A, B>(
  msg: string,
  fn: (a: A) => Promise<B>,
): (a: A) => Promise<B>;
export function withPerfLogAsync<A, B, C>(
  msg: string,
  fn: (a: A, b: B) => Promise<C>,
): (a: A, b: B) => Promise<C> {
  const t0 = performance.now();

  return async (a: A, b: B) => {
    const result = await fn(a, b);
    logPerf(msg, t0);
    return result;
  };
}
