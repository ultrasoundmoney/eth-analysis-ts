import QuickLRU from "quick-lru";
import fetch from "node-fetch";
import * as Duration from "./duration.js";

const ethPriceCache = new QuickLRU<string, EthPrice>({
  maxSize: 1,
  maxAge: Duration.milisFromSeconds(5),
});
const ethPriceKey = "eth-price";

type EthPrice = {
  usd: number;
  usd24hChange: number;
  btc: number;
  btc24hChange: number;
};

export const getEthPrice = async (): Promise<EthPrice> => {
  const cEthPrice = ethPriceCache.get(ethPriceKey);

  if (cEthPrice !== undefined) {
    return cEthPrice;
  }

  const ethPrice = await fetch(
    "https://api.coingecko.com/api/v3/simple/price?ids=ethereum&vs_currencies=usd%2Cbtc&include_24hr_change=true",
  )
    .then((res) => res.json())
    .then(({ ethereum }) => ({
      usd: ethereum.usd,
      usd24hChange: ethereum.usd_24h_change,
      btc: ethereum.btc,
      btc24hChange: ethereum.btc_24h_change,
    }));

  ethPriceCache.set(ethPriceKey, ethPrice);

  return ethPrice;
};
