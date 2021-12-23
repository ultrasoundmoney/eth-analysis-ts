import PQueue from "p-queue";
import QuickLRU from "quick-lru";
import urlcatM from "urlcat";
import * as Duration from "./duration.js";
import { HistoricPrice } from "./eth_prices.js";
import * as FetchAlt from "./fetch_alt.js";
import { O, pipe, T, TE } from "./fp.js";
import * as Log from "./log.js";

// NOTE: import is broken somehow, "urlcat is not a function" without.
const urlcat = (urlcatM as unknown as { default: typeof urlcatM }).default;

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

class BadResponseError extends Error {
  public status: number;

  constructor(message: string | undefined, status: number) {
    super(message);
    this.status = status;
  }
}

class FetchError extends Error {}
export class Timeout extends Error {}
export class RateLimit extends Error {}
export type CoinGeckoApiError =
  | BadResponseError
  | FetchError
  | Timeout
  | RateLimit;

// CoinGecko API has a 50 requests per minute rate-limit. Use up half the capacity as instances may run on the same machine and rates are limited by IP.
export const apiQueue = new PQueue({
  concurrency: 2,
  interval: Duration.millisFromSeconds(8),
  intervalCap: 3,
});

/* eslint-disable @typescript-eslint/no-explicit-any */
const queueApiFetch =
  <A>(task: T.Task<A>): T.Task<A> =>
  () =>
    apiQueue.add(task);
/* eslint-enable @typescript-eslint/no-explicit-any */

const fetchCoinGecko = <A>(url: string): TE.TaskEither<CoinGeckoApiError, A> =>
  pipe(
    FetchAlt.fetchWithRetry(url),
    queueApiFetch,
    TE.chain((res) => {
      if (res.status !== 200) {
        return TE.left(
          new BadResponseError(
            `fetch coingecko bad response status: ${res.status}, url: ${url}`,
            res.status,
          ),
        );
      }

      return TE.fromTask(() => res.json() as Promise<A>);
    }),
  );

const priceCache = new QuickLRU<string, PriceResponse>({
  maxSize: 100,
  maxAge: Duration.millisFromSeconds(16),
});

const fetchWithCache = <A>(
  cache: QuickLRU<string, A>,
  url: string,
): TE.TaskEither<CoinGeckoApiError, A> =>
  pipe(
    cache.get(url),
    O.fromNullable,
    O.match(
      () =>
        pipe(
          fetchCoinGecko<A>(url),
          queueApiFetch,
          TE.chainFirstIOK((value) => () => {
            Log.debug("coingecko fetch cache miss");
            cache.set(url, value);
          }),
        ),
      (cValue) =>
        pipe(
          TE.right(cValue),
          TE.chainFirstIOK(() => () => {
            Log.debug("coingecko fetch cache hit");
          }),
        ),
    ),
  );

const pricesQueue = new PQueue({
  concurrency: 1,
});

export const getSimpleCoins = (): TE.TaskEither<
  CoinGeckoApiError,
  PriceResponse
> => {
  const url = urlcat("https://api.coingecko.com/api/v3/simple/price", {
    ids: ["ethereum", "bitcoin", "tether-gold"].join(","),
    vs_currencies: ["usd", "btc"].join(","),
    include_market_cap: "true",
    include_24hr_change: "true",
  });

  return pipe(
    () => pricesQueue.add(fetchWithCache<PriceResponse>(priceCache, url)),
    TE.chainFirstIOK((pr) => () => {
      Log.debug(
        `getSimpleCoins, btc: ${pr.bitcoin.usd}, eth: ${pr.ethereum.usd}, gold: ${pr["tether-gold"].usd}`,
      );
    }),
  );
};

type HistoricPricesResponse = {
  prices: HistoricPrice[];
};

const pastDayEthPricesCache = new QuickLRU<string, HistoricPricesResponse>({
  maxSize: 1,
  maxAge: Duration.millisFromSeconds(60),
});

export const getPastDayEthPrices = (): TE.TaskEither<
  CoinGeckoApiError,
  HistoricPrice[]
> => {
  const url = urlcat(
    "https://api.coingecko.com/api/v3/coins/ethereum/market_chart",
    { vs_currency: "usd", days: 1 },
  );

  return pipe(
    fetchWithCache<HistoricPricesResponse>(pastDayEthPricesCache, url),
    TE.map((res) => res.prices),
  );
};
