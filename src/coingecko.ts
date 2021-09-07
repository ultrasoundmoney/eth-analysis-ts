import QuickLRU from "quick-lru";
import fetch from "node-fetch";
import * as Duration from "./duration.js";

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

const getCirculatingSupply = (id: string): Promise<number> =>
  fetch(`https://api.coingecko.com/api/v3/coins/${id}`)
    .then((res) => res.json() as Promise<CoinCG>)
    .then((body) => body.market_data.circulating_supply);

const getPrices = async (): Promise<PriceCG> => {
  const rawPrices = await fetch(
    "https://api.coingecko.com/api/v3/simple/price?ids=ethereum,bitcoin,tether-gold&vs_currencies=usd%2Cbtc&include_24hr_change=true",
  ).then((res) => res.json() as Promise<PriceCG>);

  return rawPrices;
};

export const getMarketData = async (): Promise<MarketData> => {
  const cPrices = marketDataCache.get(marketDataKey);

  if (cPrices !== undefined) {
    return cPrices;
  }

  const [prices, circulatingSupplyEth, circulatingSupplyBtc] =
    await Promise.all([
      getPrices(),
      getCirculatingSupply("ethereum"),
      getCirculatingSupply("bitcoin"),
    ]);

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
};
