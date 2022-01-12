import * as DateFns from "date-fns";
import { setInterval } from "timers/promises";
import * as Duration from "./duration.js";
import * as EthPrices from "./eth_prices.js";
import { pipe, TEAlt } from "./fp.js";
import * as Log from "./log.js";

process.on("unhandledRejection", (error) => {
  throw error;
});

const watermark = 30;

const intervalIterator = setInterval(
  Duration.millisFromSeconds(10),
  Date.now(),
);

let lastRun = new Date();

// eslint-disable-next-line @typescript-eslint/no-unused-vars
for await (const _ of intervalIterator) {
  const secondsSinceLastRun = DateFns.differenceInSeconds(new Date(), lastRun);

  if (secondsSinceLastRun >= watermark) {
    Log.error(
      `store price not keeping up, ${secondsSinceLastRun}s since last price fetch`,
    );
  }

  lastRun = new Date();

  await pipe(EthPrices.storeCurrentEthPrice(), TEAlt.getOrThrow)();
}
