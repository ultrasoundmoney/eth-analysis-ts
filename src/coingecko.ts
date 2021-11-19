import * as DateFns from "date-fns";
import * as EthPrices from "./eth_prices.js";
import fetch from "node-fetch";
import PQueue from "p-queue";
import QuickLRU from "quick-lru";
import { exponentialBackoff, limitRetries, Monoid } from "retry-ts";
import { retrying } from "retry-ts/lib/Task.js";
import { setInterval } from "timers/promises";
import urlcatM from "urlcat";
import { sql } from "./db.js";
import * as Duration from "./duration.js";
import { HistoricPrice } from "./eth_prices.js";
import { E, O, pipe, seqTParT, seqTParTE, TE } from "./fp.js";
import * as Log from "./log.js";

// NOTE: import is broken somehow, "urlcat is not a function" without.
const urlcat = (urlcatM as unknown as { default: typeof urlcatM }).default;

type CoinResponse = {
  market_data: {
    circulating_supply: number;
  };
};

type PriceResponse = {
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

type MarketData = {
  eth: {
    usd: number;
    usd24hChange: number;
    btc: number;
    btc24hChange: number;
    circulatingSupply: number;
  };
  btc: {
    usd: number;
    usd24hChange: number;
    circulatingSupply: number;
  };
  gold: {
    usd: number;
    usd24hChange: number;
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

const circulatingSupplyCache = new QuickLRU<string, CoinResponse>({
  maxSize: 100,
  maxAge: Duration.millisFromSeconds(60),
});

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
            Log.debug("coingecko fetch cache miss", { url });
            cache.set(url, value);
          }),
        ),
      (cValue) =>
        pipe(
          TE.of(cValue),
          TE.chainFirstIOK(() => () => {
            Log.debug("coingecko fetch cache hit", url);
          }),
        ),
    ),
  );

const circulatingSupplyQueue = new PQueue({
  concurrency: 1,
});

const getCirculatingSupplyWithCache = (
  id: string,
): TE.TaskEither<MarketDataError, number> =>
  pipe(
    () =>
      circulatingSupplyQueue.add(
        fetchWithCache<CoinResponse>(
          circulatingSupplyCache,
          `https://api.coingecko.com/api/v3/coins/${id}`,
        ),
      ),
    TE.map((body) => body.market_data.circulating_supply),
  );

const pricesQueue = new PQueue({
  concurrency: 1,
});

const getPrices = (): TE.TaskEither<MarketDataError, PriceResponse> => {
  const url = urlcat("https://api.coingecko.com/api/v3/simple/price", {
    ids: ["ethereum", "bitcoin", "tether-gold"].join(","),
    vs_currencies: ["usd", "btc"].join(","),
    include_market_cap: "true",
    include_24hr_change: "true",
  });

  return () => pricesQueue.add(fetchWithCache<PriceResponse>(priceCache, url));
};

export const getMarketData = (): TE.TaskEither<MarketDataError, MarketData> =>
  pipe(
    seqTParTE(
      getPrices(),
      getCirculatingSupplyWithCache("ethereum"),
      getCirculatingSupplyWithCache("bitcoin"),
    ),
    TE.map(([prices, circulatingSupplyEth, circulatingSupplyBtc]) => {
      const eth = prices.ethereum;
      const btc = prices.bitcoin;
      const gold = prices["tether-gold"];

      return {
        eth: {
          usd: eth.usd,
          usd24hChange: eth.usd_24h_change,
          btc: eth.btc,
          btc24hChange: eth.btc_24h_change,
          circulatingSupply: circulatingSupplyEth,
        },
        btc: {
          usd: btc.usd,
          usd24hChange: btc.usd_24h_change,
          circulatingSupply: circulatingSupplyBtc,
        },
        gold: {
          usd: gold.usd,
          usd24hChange: gold.usd_24h_change,
          // In tonnes.
          circulatingSupply: 201296.1,
        },
      };
    }),
  );

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

const storeMarketCaps = async () => {
  const [coins, ethPrice] = await seqTParT(
    getPrices(),
    EthPrices.getEthPrice(new Date()),
  )();

  if (E.isLeft(coins)) {
    throw new Error(String(coins.left));
  }

  const btcMarketCap = coins.right.bitcoin.usd_market_cap;
  const coingeckoMarketCap = coins.right.ethereum.usd_market_cap;
  const coingeckoEthPrice = coins.right.ethereum.usd;
  const ethCirculatingSupply = coingeckoMarketCap / coingeckoEthPrice;
  const ethMarketCap = ethCirculatingSupply * ethPrice.ethusd;
  // See: https://www.gold.org/goldhub/data/above-ground-stocks which many appear to use.
  // In tonnes.
  const goldCirculatingSupply = 201296.1;
  const kgPerTonne = 1000;
  const troyOzPerKg = 1000 / 31.1034768;
  const goldPricePerTroyOz = coins.right["tether-gold"].usd;
  const goldMarketCap =
    goldCirculatingSupply * kgPerTonne * troyOzPerKg * goldPricePerTroyOz;
  // See: https://ycharts.com/indicators/us_m3_money_supply
  const usdM3MarketCap = 20_982_900_000_000;

  const marketCaps = {
    btc_market_cap: btcMarketCap,
    eth_market_cap: ethMarketCap,
    gold_market_cap: goldMarketCap,
    usd_m3_market_cap: usdM3MarketCap,
    timestamp: new Date(),
  };

  Log.debug("storing market caps", marketCaps);

  await sql`
    INSERT INTO market_caps
      ${sql(marketCaps)}
  `;

  // Don't let the table grow too much.
  await sql`
    DELETE FROM market_caps
    WHERE timestamp IN (
      SELECT timestamp FROM market_caps
      ORDER BY timestamp DESC
      OFFSET 1
    )
  `;
};

type MarketCaps = {
  btcMarketCap: number;
  ethMarketCap: number;
  goldMarketCap: number;
  usdM3MarketCap: number;
  timestamp: Date;
};

export const getMarketCaps = async (): Promise<MarketCaps> =>
  sql<MarketCaps[]>`
    SELECT
      btc_market_cap,
      eth_market_cap,
      gold_market_cap,
      usd_m3_market_cap,
      timestamp
    FROM market_caps
    ORDER BY timestamp DESC
    LIMIT 1
  `.then((rows) => rows[0]);

const warnWatermark = 180;
const criticalWatermark = 360;

export const storeMarketCapsAbortController = new AbortController();
export const continuouslyStoreMarketCaps = async () => {
  const intervalIterator = setInterval(
    Duration.millisFromMinutes(1),
    Date.now(),
    { signal: storeMarketCapsAbortController.signal },
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
        `store market cap not keeping up, ${secondsSinceLastRun}s since last price fetch`,
      );
    }

    if (secondsSinceLastRun >= criticalWatermark) {
      Log.error(
        `store market cap not keeping up, ${secondsSinceLastRun}s since last price fetch`,
      );
    }

    lastRun = new Date();

    await storeMarketCaps();
  }
};
