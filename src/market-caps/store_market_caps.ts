import { setInterval } from "timers/promises";
import * as Duration from "../duration.js";
import { BadResponseError } from "../fetch_alt.js";
import { pipe, TE } from "../fp.js";
import * as Log from "../log.js";
import * as EthSupply from "../scarcity/eth_supply.js";
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
  await pipe(
    MarketCaps.storeCurrentMarketCaps(),
    TE.match(
      (e) => {
        if (e instanceof BadResponseError && e.status === 429) {
          Log.warn("hit rate-limit storing market caps", e);
          return;
        }

        Log.error("failed to store market caps", e);
      },
      () => undefined,
    ),
  )();
}

await EthSupply.init();
await MarketCaps.storeCurrentMarketCaps()();
