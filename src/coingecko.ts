import * as Duration from "./duration.js";
import PQueue from "p-queue";
import QuickLRU from "quick-lru";
import fetch from "node-fetch";
import { E, pipe, seqTParTE, TE } from "./fp.js";
import { exponentialBackoff, limitRetries, Monoid } from "retry-ts";
import { retrying } from "retry-ts/lib/Task.js";

type CoinCG = {
  market_data: {
    circulating_supply: number;
  };
};

type PriceCG = {
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

type BadResponse = { _tag: "bad-response"; status: number };
type FetchError = { _tag: "fetch-error"; error: Error };
type UnknownError = { _tag: "unknown-error"; error: Error };
type CoinGeckoApiError = BadResponse | FetchError;

export type MarketDataError = CoinGeckoApiError | UnknownError;

// CoinGecko API has a 50 requests per minute rate-limit. We run many instances so only use up 1/4 of the capacity.
export const coingeckoQueue = new PQueue({
  concurrency: 4,
  interval: Duration.milisFromSeconds(60),
  intervalCap: 50 / 4,
  throwOnTimeout: true,
  timeout: Duration.milisFromSeconds(16),
});

const fetchCoinGecko = <A>(url: string): TE.TaskEither<CoinGeckoApiError, A> =>
  pipe(
    TE.tryCatch(
      () => coingeckoQueue.add(() => fetch(url)),
      (error) =>
        ({ _tag: "fetch-error", error: error as Error } as CoinGeckoApiError),
    ),
    TE.chain((res) => {
      if (res.status !== 200) {
        return TE.left({
          _tag: "bad-response",
          status: res.status,
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

const getCirculatingSupply = (
  id: string,
): TE.TaskEither<MarketDataError, number> =>
  pipe(
    fetchWithRetry<CoinCG>(`https://api.coingecko.com/api/v3/coins/${id}`),
    TE.map((body) => body.market_data.circulating_supply),
  );

const getPrices = (): TE.TaskEither<MarketDataError, PriceCG> =>
  fetchWithRetry<PriceCG>(
    "https://api.coingecko.com/api/v3/simple/price?ids=ethereum,bitcoin,tether-gold&vs_currencies=usd%2Cbtc&include_24hr_change=true",
  );

const marketDataCache = new QuickLRU<string, MarketData>({
  maxSize: 1,
  maxAge: Duration.milisFromSeconds(10),
});
const marketDataKey = "prices";

export const getMarketData = (): TE.TaskEither<MarketDataError, MarketData> => {
  const cPrices = marketDataCache.get(marketDataKey);

  if (cPrices !== undefined) {
    return TE.right(cPrices);
  }

  return pipe(
    seqTParTE(
      getPrices(),
      getCirculatingSupply("ethereum"),
      getCirculatingSupply("bitcoin"),
    ),
    TE.map(([prices, circulatingSupplyEth, circulatingSupplyBtc]) => {
      const eth = prices.ethereum;
      const btc = prices.bitcoin;
      const gold = prices["tether-gold"];

      const marketData = {
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
        },
      };

      marketDataCache.set(marketDataKey, marketData);

      return marketData;
    }),
  );
};
