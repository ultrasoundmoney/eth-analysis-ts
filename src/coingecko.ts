import PQueue from "p-queue";
import QuickLRU from "quick-lru";
import urlcatM from "urlcat";
import * as Duration from "./duration.js";
import { HistoricPrice } from "./eth-prices/eth_prices.js";
import * as FetchAlt from "./fetch_alt.js";
import { O, pipe, T, TE, TEAlt } from "./fp.js";
import * as UrlSub from "url-sub";
import * as Log from "./log.js";
import { decodeErrorFromUnknown } from "./errors.js";

// NOTE: import is broken somehow, "urlcat is not a function" without.
const urlcat = (urlcatM as unknown as { default: typeof urlcatM }).default;

class FetchError extends Error {}
export class Timeout extends Error {}
export class RateLimit extends Error {}
export type CoinGeckoApiError = FetchError | Timeout | RateLimit;

export type PriceResponse = {
  ethereum: {
    usd: number;
    usd_24h_change: number;
    usd_market_cap: number;
    btc: number;
    btc_24h_change: number;
  };
  bitcoin: {
    usd: number;
    usd_24h_change: number;
    usd_market_cap: number;
  };
  "tether-gold": {
    usd: number;
    usd_24h_change: number;
    usd_market_cap: number;
  };
};

const url = UrlSub.formatUrl(
  "https://api.coingecko.com",
  "/api/v3/simple/price",
  {
    ids: ["ethereum", "bitcoin", "tether-gold"].join(","),
    vs_currencies: ["usd", "btc"].join(","),
    include_market_cap: "true",
    include_24hr_change: "true",
  },
);

export const getSimpleCoins = (): TE.TaskEither<
  CoinGeckoApiError,
  PriceResponse
> =>
  pipe(
    FetchAlt.fetchWithRetry(url),
    TE.chain((res) =>
      TE.tryCatch(
        () => res.json() as Promise<PriceResponse>,
        decodeErrorFromUnknown,
      ),
    ),
    TE.chainFirstIOK((pr) => () => {
      Log.debug(
        `got CoinGecko simple prices for, btc: ${pr.bitcoin.usd}, eth: ${pr.ethereum.usd}, and gold: ${pr["tether-gold"].usd}`,
      );
    }),
  );
