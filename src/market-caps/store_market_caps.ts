import * as Coingecko from "../coingecko.js";
import { pipe, TE } from "../fp.js";
import * as Log from "../log.js";
import * as MarketCaps from "./market_caps.js";

try {
  await pipe(
    MarketCaps.storeCurrentMarketCaps(),
    TE.mapLeft((e) => {
      if (e instanceof Coingecko.Timeout || e instanceof Coingecko.RateLimit) {
        Log.warn(e);
        return;
      }

      Log.error(e);
    }),
  )();
} catch (error) {
  Log.alert("unhandled exception when storing metrics");
}
