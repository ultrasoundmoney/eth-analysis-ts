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

export const measureTaskPerf =
  (msg: string) =>
  <A>(task: T.Task<A>) =>
    pipe(
      T.Do,
      T.bind("t0", () => T.of(performance.now())),
      T.bind("result", () => task),
      T.chainFirstIOK(({ t0 }) => () => {
        logPerf(msg, t0);
      }),
      T.map(({ result }) => result),
    );

export const measurePromisePerf = async <A>(
  msg: string,
  promise: Promise<A>,
): Promise<A> => {
  const t0 = performance.now();
  const result = await promise;
  logPerf(msg, t0);
  return result;
};
