import * as Retry from "retry-ts";
import * as FetchAlt from "./fetch_alt.js";
import { E, pipe, T, TE } from "./fp.js";
import * as Log from "./log.js";

const marketDataApi = `https://data-api.defipulse.com/api/v1/defipulse/api/MarketData?api-key=${process.env.DEFI_PULSE_API_KEY}`;

type MarketData = {
  All: {
    value: {
      total: {
        ETH: {
          value: number;
        };
      };
    };
  };
};

// Uses 5 API credits per call, we have 2000 per month.
export const getEthLocked = () =>
  pipe(
    Log.debug("getting ETH locked from DefiPulse"),
    () =>
      // DefiPulse API is broken, to speed up our boot time we only retry the request once for the time being.
      FetchAlt.fetchWithRetry(marketDataApi, undefined, {
        retryPolicy: Retry.Monoid.concat(
          Retry.exponentialBackoff(2000),
          Retry.limitRetries(1),
        ),
      }),
    TE.chainW((res) =>
      pipe(() => res.json() as Promise<MarketData>, T.map(E.right)),
    ),
    TE.map((marketData) => {
      const ethLocked = marketData.All.value.total.ETH.value;
      Log.debug(`got eth locked from defi pulse: ${ethLocked} ETH`);
      return ethLocked;
    }),
  );
