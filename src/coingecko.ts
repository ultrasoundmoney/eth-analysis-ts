import * as Duration from "./duration.js";
import * as Log from "./log.js";
import QuickLRU from "quick-lru";
import fetch from "node-fetch";
import { E, pipe, seqTParTE, TE } from "./fp.js";
import { constantDelay, limitRetries, Monoid } from "retry-ts";
import { retrying } from "retry-ts/lib/Task";

const marketDataCache = new QuickLRU<string, MarketData>({
  maxSize: 1,
  maxAge: Duration.milisFromSeconds(5),
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

const getCirculatingSupply = (id: string): TE.TaskEither<Error, number> =>
  retrying(
    Monoid.concat(constantDelay(1000), limitRetries(3)),
    () =>
      pipe(
        TE.tryCatch(
          () => fetch(`https://api.coingecko.com/api/v3/coins/${id}`),
          (err) => err as Error,
        ),
        TE.chain((res) => {
          if (res.status !== 200) {
            return TE.left(new Error(`bad coingecko response: ${res.status}`));
          }
          return TE.fromTask(() => res.json() as Promise<CoinCG>);
        }),
        TE.map((body) => body.market_data.circulating_supply),
      ),
    E.isLeft,
  );

const getPrices = (): TE.TaskEither<Error, PriceCG> =>
  retrying(
    Monoid.concat(constantDelay(1000), limitRetries(3)),
    () =>
      pipe(
        TE.tryCatch(
          () =>
            fetch(
              "https://api.coingecko.com/api/v3/simple/price?ids=ethereum,bitcoin,tether-gold&vs_currencies=usd%2Cbtc&include_24hr_change=true",
            ),
          (err) => err as Error,
        ),
        TE.chain((res) => {
          if (res.status !== 200) {
            return TE.left(new Error(`bad coingecko response: ${res.status}`));
          }
          return TE.fromTask(() => res.json() as Promise<PriceCG>);
        }),
      ),
    E.isLeft,
  );

export const getMarketData = async (): Promise<MarketData | undefined> => {
  const cPrices = marketDataCache.get(marketDataKey);

  if (cPrices !== undefined) {
    return cPrices;
  }

  return pipe(
    seqTParTE(
      getPrices(),
      getCirculatingSupply("ethereum"),
      getCirculatingSupply("bitcoin"),
    ),
    TE.match(
      (error) => {
        Log.error("failed to fetch market data from coingekco", { error });
        return undefined;
      },
      ([prices, circulatingSupplyEth, circulatingSupplyBtc]) => {
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
      },
    ),
  )();
};
