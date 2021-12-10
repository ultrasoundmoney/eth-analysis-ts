import { performance } from "perf_hooks";
import * as Log from "./log.js";

export const logPerf = (name: string, t0: number): void => {
  const t1 = performance.now();
  const took = ((t1 - t0) / 1000).toFixed(2);
  Log.info(`${name} took ${took}s`);
};

export const logPerfT = (name: string, t0: number) => () => () => {
  logPerf(name, t0);
};
