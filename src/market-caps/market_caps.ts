import camelcaseKeys from "camelcase-keys";
import * as Coingecko from "../coingecko.js";
import { sql, sqlT, sqlTNotify, sqlTVoid } from "../db.js";
import * as Duration from "../duration.js";
import * as EthPrices from "../eth-prices/eth_prices.js";
import { E, flow, pipe, T, TE } from "../fp.js";
import * as Log from "../log.js";

export type MarketCaps = {
  btcMarketCap: number;
  ethMarketCap: number;
  goldMarketCap: number;
  usdM3MarketCap: number;
  timestamp: Date;
};

export const marketCapsCacheKey = "market-caps";

const storeMarketCaps = (marketCaps: MarketCapsInsertable) =>
  sqlTVoid`
    INSERT INTO key_value_store
      ${sql({ key: marketCapsCacheKey, value: JSON.stringify(marketCaps) })}
    ON CONFLICT (key) DO UPDATE SET
      value = excluded.value
  `;

type MarketCapsInsertable = {
  btc_market_cap: number;
  eth_market_cap: number;
  gold_market_cap: number;
  usd_m3_market_cap: number;
  timestamp: Date;
};

const insertableFromCoinData = (
  coins: Coingecko.PriceResponse,
  ethPrice: EthPrices.EthPrice,
): MarketCapsInsertable => {
  const btcMarketCap = coins.bitcoin.usd_market_cap;
  const coingeckoMarketCap = coins.ethereum.usd_market_cap;
  const coingeckoEthPrice = coins.ethereum.usd;
  const ethCirculatingSupply = coingeckoMarketCap / coingeckoEthPrice;
  const ethMarketCap = ethCirculatingSupply * ethPrice.ethusd;
  // See: https://www.gold.org/goldhub/data/above-ground-stocks which many appear to use.
  // In tonnes.
  const goldCirculatingSupply = 201296.1;
  const kgPerTonne = 1000;
  const troyOzPerKg = 1000 / 31.1034768;
  const goldPricePerTroyOz = coins["tether-gold"].usd;
  const goldMarketCap =
    goldCirculatingSupply * kgPerTonne * troyOzPerKg * goldPricePerTroyOz;
  // See: https://ycharts.com/indicators/us_m3_money_supply
  const usdM3MarketCap = 20_982_900_000_000;

  return {
    btc_market_cap: btcMarketCap,
    eth_market_cap: ethMarketCap,
    gold_market_cap: goldMarketCap,
    usd_m3_market_cap: usdM3MarketCap,
    timestamp: new Date(),
  };
};

export const storeCurrentMarketCaps = () =>
  pipe(
    TE.Do,
    TE.apS("coins", Coingecko.getSimpleCoins()),
    TE.apSW(
      "ethPrice",
      EthPrices.getEthPrice(new Date(), Duration.millisFromMinutes(5)),
    ),
    TE.map(
      ({ coins, ethPrice }): MarketCapsInsertable =>
        insertableFromCoinData(coins, ethPrice),
    ),
    TE.chainW((marketCaps) =>
      pipe(storeMarketCaps(marketCaps), T.map(E.right)),
    ),
    TE.chainFirstTaskK(() => sqlTNotify("cache-update", marketCapsCacheKey)),
    TE.chainFirstIOK(() => () => {
      Log.debug(`stored market caps at ${new Date().toISOString()}`);
    }),
  );

type MarketCapsRow = {
  btc_market_cap: number;
  eth_market_cap: number;
  gold_market_cap: number;
  usd_m3_market_cap: number;
  timestamp: string;
};

export const getStoredMarketCaps = () =>
  pipe(
    sqlT<{ value: MarketCapsRow }[]>`
      SELECT
        value
      FROM key_value_store
      WHERE key = ${marketCapsCacheKey}
    `,
    T.map(
      flow(
        (rows) => rows[0].value,
        (obj) => camelcaseKeys(obj),
        (obj) => ({
          ...obj,
          timestamp: new Date(obj.timestamp),
        }),
      ),
    ),
  );
