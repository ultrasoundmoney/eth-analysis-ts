import { performance } from "perf_hooks";
import * as Config from "./config.js";
import { pipe, T } from "./fp.js";
import * as Log from "./log.js";

export const logPerf = (
  name: string,
  t0: number,
  level: Log.Level = "DEBUG",
): void => {
  if (!Config.getLogPerformance()) {
    return;
  }

  const t1 = performance.now();
  const took = ((t1 - t0) / 1000).toFixed(2);
  Log.log(level, `${name} took ${took}s`);
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

/* eslint-disable @typescript-eslint/no-explicit-any */
export const withPerfLogT =
  <A extends any[], B>(msg: string, fn: (...args: A) => T.Task<B>) =>
  (...args: A) =>
    pipe(
      T.Do,
      T.bind("t0", () => T.of(performance.now())),
      T.bind("result", () => fn(...args)),
      T.chainFirstIOK(({ t0 }) => () => {
        logPerf(msg, t0);
      }),
      T.map(({ result }) => result),
    );
/* eslint-enable @typescript-eslint/no-explicit-any */

export const measureTaskPerf = <A>(msg: string, task: T.Task<A>) =>
  pipe(
    T.Do,
    T.bind("t0", () => T.of(performance.now())),
    T.bind("result", () => task),
    T.chainFirstIOK(({ t0 }) => () => {
      logPerf(msg, t0);
    }),
    T.map(({ result }) => {
      return result;
    }),
  );
