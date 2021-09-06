import QuickLRU from "quick-lru";
import fetch from "node-fetch";
import * as Duration from "./duration.js";

const priceCache = new QuickLRU<string, Prices>({
  maxSize: 1,
  maxAge: Duration.milisFromSeconds(5),
});
const pricesKey = "prices";

type Prices = {
  eth: {
    usd: number;
    usd24hChange: number;
    usdMarketCap: number;
    btc: number;
    btc24hChange: number;
  };
  btc: {
    usd: number;
    usd24hChange: number;
    usdMarketCap: number;
  };
  gold: {
    usd: number;
    usd24hChange: number;
  };
};

type EthPriceCG = {
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

export const getPrices = async (): Promise<Prices> => {
  const cPrices = priceCache.get(pricesKey);

  if (cPrices !== undefined) {
    return cPrices;
  }

  const prices = await fetch(
    "https://api.coingecko.com/api/v3/simple/price?ids=ethereum,bitcoin,tether-gold&vs_currencies=usd%2Cbtc&include_24hr_change=true&include_market_cap=true",
  )
    .then((res) => res.json() as Promise<EthPriceCG>)
    .then((rawPrices) => {
      const eth = rawPrices.ethereum;
      const btc = rawPrices.bitcoin;
      const gold = rawPrices["tether-gold"];

      return {
        eth: {
          usd: eth.usd,
          usd24hChange: eth.usd_24h_change,
          usdMarketCap: eth.usd_market_cap,
          btc: eth.btc,
          btc24hChange: eth.btc_24h_change,
        },
        btc: {
          usd: btc.usd,
          usd24hChange: btc.usd_24h_change,
          usdMarketCap: btc.usd_market_cap,
        },
        gold: {
          usd: gold.usd,
          usd24hChange: gold.usd_24h_change,
        },
      };
    });

  priceCache.set(pricesKey, prices);

  return prices;
};
