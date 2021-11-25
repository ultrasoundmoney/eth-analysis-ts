import * as DateFns from "date-fns";
import { setInterval } from "timers/promises";
import * as Coingecko from "./coingecko.js";
import * as Duration from "./duration.js";
import * as Log from "./log.js";

process.on("unhandledRejection", (error) => {
  throw error;
});

const warnWatermark = 180;
const criticalWatermark = 360;

export const storeMarketCapsAbortController = new AbortController();
export const continuouslyStoreMarketCaps = async () => {
  const intervalIterator = setInterval(
    Duration.millisFromMinutes(1),
    Date.now(),
    { signal: storeMarketCapsAbortController.signal },
  );

  let lastRun = new Date();

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  for await (const _ of intervalIterator) {
    const secondsSinceLastRun = DateFns.differenceInSeconds(
      new Date(),
      lastRun,
    );

    if (secondsSinceLastRun >= warnWatermark) {
      Log.warn(
        `store market cap not keeping up, ${secondsSinceLastRun}s since last price fetch`,
      );
    }

    if (secondsSinceLastRun >= criticalWatermark) {
      Log.error(
        `store market cap not keeping up, ${secondsSinceLastRun}s since last price fetch`,
      );
    }

    lastRun = new Date();

    await Coingecko.storeMarketCaps();
  }
};
