import fetch from "node-fetch";
import PQueue from "p-queue";
import QuickLRU from "quick-lru";
import { exponentialBackoff, limitRetries, Monoid } from "retry-ts";
import { retrying } from "retry-ts/lib/Task.js";
import urlcatM from "urlcat";
import * as Duration from "./duration.js";
import { HistoricPrice } from "./eth_prices.js";
import { E, O, pipe, TE } from "./fp.js";
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

type BadResponse = { _tag: "bad-response"; error: Error; status: number };
type FetchError = { _tag: "fetch-error"; error: Error };
type UnknownError = { _tag: "unknown-error"; error: Error };
type Timeout = { _tag: "timeout"; error: Error };
type RateLimit = { _tag: "rate-limit"; error: Error };
type CoinGeckoApiError = BadResponse | FetchError | Timeout | RateLimit;

export type MarketDataError = CoinGeckoApiError | UnknownError;

// CoinGecko API has a 50 requests per minute rate-limit. Use up half the capacity as instances may run on the same machine and rates are limited by IP.
export const apiQueue = new PQueue({
  concurrency: 2,
  interval: Duration.millisFromSeconds(8),
  intervalCap: 3,
  timeout: Duration.millisFromSeconds(8),
});

const fetchCoinGecko = <A>(url: string): TE.TaskEither<CoinGeckoApiError, A> =>
  pipe(
    TE.tryCatch(
      () => apiQueue.add(() => fetch(url)),
      (error) =>
        ({ _tag: "fetch-error", error: error as Error } as CoinGeckoApiError),
    ),
    TE.chain((res) => {
      if (res === undefined) {
        return TE.left({
          _tag: "timeout",
          error: new Error("hit coingecko api request timeout"),
        });
      }

      if (res.status === 429) {
        return TE.left({
          _tag: "rate-limit",
          error: new Error("hit coingecko api rate-limit, slow down"),
        });
      }

      if (res.status !== 200) {
        return TE.left({
          _tag: "bad-response",
          error: new Error(
            `fetch coingecko bad response status: ${res.status}, url: ${url}`,
          ),
        } as CoinGeckoApiError);
      }

      return TE.fromTask(() => res.json() as Promise<A>);
    }),
  );

const fetchWithRetry = <A>(url: string): TE.TaskEither<CoinGeckoApiError, A> =>
  retrying(
    Monoid.concat(exponentialBackoff(1000), limitRetries(3)),
    () => fetchCoinGecko(url),
    E.isLeft,
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
          fetchWithRetry<A>(url),
          TE.chainFirstIOK((value) => () => {
            Log.debug("coingecko fetch cache miss");
            cache.set(url, value);
          }),
        ),
      (cValue) =>
        pipe(
          TE.of(cValue),
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
  MarketDataError,
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
