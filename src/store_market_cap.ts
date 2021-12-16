import * as DateFns from "date-fns";
import { setInterval } from "timers/promises";
import * as MarketCaps from "./market_caps.js";
import * as Duration from "./duration.js";
import * as Log from "./log.js";
import { pipe, TE } from "./fp.js";

process.on("unhandledRejection", (error) => {
  throw error;
});

const warnWatermark = 180;
const criticalWatermark = 360;

const storeMarketCapsAbortController = new AbortController();

const intervalIterator = setInterval(
  Duration.millisFromMinutes(1),
  Date.now(),
  { signal: storeMarketCapsAbortController.signal },
);

let lastRun = new Date();

// eslint-disable-next-line @typescript-eslint/no-unused-vars
for await (const _ of intervalIterator) {
  const secondsSinceLastRun = DateFns.differenceInSeconds(new Date(), lastRun);

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

  await pipe(
    MarketCaps.storeCurrentMarketCaps(),
    TE.match(
      (e) => {
        if (typeof e === "string") {
          throw new Error(e);
        }

        if (e instanceof Error) {
          throw e;
        }

        if (e._tag === "timeout" || e._tag === "rate-limit") {
          Log.warn(e.error);
          return;
        }

        throw e.error;
      },
      () => undefined,
    ),
  )();
}
