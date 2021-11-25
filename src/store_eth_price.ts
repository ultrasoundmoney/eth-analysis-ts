import * as EthPrices from "./eth_prices.js";
import * as DateFns from "date-fns";
import { setInterval } from "timers/promises";
import * as Duration from "./duration.js";
import * as Log from "./log.js";

process.on("unhandledRejection", (error) => {
  throw error;
});

const warnWatermark = 30;
const criticalWatermark = 60;

export const storePriceAbortController = new AbortController();

export const continuouslyStorePrice = async () => {
  const intervalIterator = setInterval(
    Duration.millisFromSeconds(10),
    Date.now(),
    { signal: storePriceAbortController.signal },
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
        `store price not keeping up, ${secondsSinceLastRun}s since last price fetch`,
      );
    }

    if (secondsSinceLastRun >= criticalWatermark) {
      Log.error(
        `store price not keeping up, ${secondsSinceLastRun}s since last price fetch`,
      );
    }

    lastRun = new Date();

    await EthPrices.storePrice()();
  }
};
