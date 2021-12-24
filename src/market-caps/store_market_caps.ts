import { setInterval } from "timers/promises";
import * as Coingecko from "../coingecko.js";
import * as Duration from "../duration.js";
import { pipe, TE } from "../fp.js";
import * as Log from "../log.js";
import * as MarketCaps from "./market_caps.js";

process.on("unhandledRejection", (error) => {
  throw error;
});

const everyMinuteIterator = setInterval(
  Duration.millisFromMinutes(1),
  Date.now(),
);

// eslint-disable-next-line @typescript-eslint/no-unused-vars
for await (const _ of everyMinuteIterator) {
  try {
    await pipe(
      MarketCaps.storeCurrentMarketCaps(),
      TE.mapLeft((e) => {
        if (
          e instanceof Coingecko.Timeout ||
          e instanceof Coingecko.RateLimit
        ) {
          Log.warn("hit rate-limit storing market caps", e);
          return;
        }

        Log.error("error storing market caps", e);
      }),
    )();
  } catch (error) {
    Log.alert("unhandled error storing market caps", error);
  }
}
