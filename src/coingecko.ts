import * as Duration from "./duration.js";
import QuickLRU from "quick-lru";
import fetch from "node-fetch";
import { E, pipe, seqTParTE, TE } from "./fp.js";
import { exponentialBackoff, limitRetries, Monoid } from "retry-ts";
import { retrying } from "retry-ts/lib/Task.js";

const marketDataCache = new QuickLRU<string, MarketData>({
  maxSize: 1,
  maxAge: Duration.milisFromSeconds(10),
});
const marketDataKey = "prices";

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

const fetchCoinGecko = <A>(url: string): TE.TaskEither<MarketDataError, A> => {
  return retrying(
    Monoid.concat(exponentialBackoff(1000), limitRetries(3)),
    () =>
      pipe(
        TE.tryCatch(
          () => fetch(url),
          (error) =>
            ({ _type: "fetch-error", error: String(error) } as MarketDataError),
        ),
        TE.chain((res) => {
          if (res.status !== 200) {
            return TE.left({
              _type: "bad-response",
              status: res.status,
            } as MarketDataError);
          }
          return TE.fromTask(() => res.json() as Promise<A>);
        }),
      ),
    E.isLeft,
  );
};

const getCirculatingSupply = (
  id: string,
): TE.TaskEither<MarketDataError, number> =>
  pipe(
    fetchCoinGecko<CoinCG>(`https://api.coingecko.com/api/v3/coins/${id}`),
    TE.map((body) => body.market_data.circulating_supply),
  );

const getPrices = (): TE.TaskEither<MarketDataError, PriceCG> =>
  fetchCoinGecko<PriceCG>(
    "https://api.coingecko.com/api/v3/simple/price?ids=ethereum,bitcoin,tether-gold&vs_currencies=usd%2Cbtc&include_24hr_change=true",
  );

type BadResponse = { _type: "bad-response"; status: number };
type FetchError = { _type: "fetch-error"; error: string };
export type MarketDataError = FetchError | BadResponse;

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
