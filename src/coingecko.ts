import QuickLRU from "quick-lru";
import fetch from "node-fetch";
import * as Duration from "./duration.js";

const priceCache = new QuickLRU<string, Prices>({
  maxSize: 1,
  maxAge: Duration.milisFromSeconds(5),
});
const pricesKey = "eth-price";

type PriceBreakdown = {
  usd: number;
  usd24hChange: number;
  btc: number;
  btc24hChange: number;
};
type Prices = {
  eth: PriceBreakdown;
  btc: PriceBreakdown;
};

type EthPriceCG = {
  ethereum: {
    usd: number;
    usd_24h_change: number;
    btc: number;
    btc_24h_change: number;
  };
  bitcoin: {
    usd: number;
    usd_24h_change: number;
    btc: number;
    btc_24h_change: number;
  };
};

export const getPrices = async (): Promise<Prices> => {
  const cPrices = priceCache.get(pricesKey);

  if (cPrices !== undefined) {
    return cPrices;
  }

  const prices = await fetch(
    "https://api.coingecko.com/api/v3/simple/price?ids=ethereum,bitcoin&vs_currencies=usd%2Cbtc&include_24hr_change=true",
  )
    .then((res) => res.json() as Promise<EthPriceCG>)
    .then(({ ethereum, bitcoin }) => ({
      eth: {
        usd: ethereum.usd,
        usd24hChange: ethereum.usd_24h_change,
        btc: ethereum.btc,
        btc24hChange: ethereum.btc_24h_change,
      },
      btc: {
        usd: bitcoin.usd,
        usd24hChange: bitcoin.usd_24h_change,
        btc: bitcoin.btc,
        btc24hChange: bitcoin.btc_24h_change,
      },
    }));

  priceCache.set(pricesKey, prices);

  return prices;
};
