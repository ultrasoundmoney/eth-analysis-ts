import * as Coingecko from "../coingecko.js";
import { sql, sqlT, sqlTNotify, sqlTVoid } from "../db.js";
import * as Duration from "../duration.js";
import * as EthPrices from "../eth-prices/eth_prices.js";
import { E, flow, O, pipe, T, TE } from "../fp.js";
import * as Log from "../log.js";

export type MarketCaps = {
  btcMarketCap: number;
  ethMarketCap: number;
  goldMarketCap: number;
  timestamp: Date;
  usdM3MarketCap: number;
};

export const marketCapsCacheKey = "market-caps";

const storeMarketCaps = (marketCaps: MarketCaps) =>
  sqlTVoid`
    INSERT INTO key_value_store
      ${sql({ key: marketCapsCacheKey, value: JSON.stringify(marketCaps) })}
    ON CONFLICT (key) DO UPDATE SET
      value = excluded.value
  `;

const insertableFromCoinData = (
  coins: Coingecko.PriceResponse,
  ethPrice: EthPrices.EthPrice,
) => {
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
    btcMarketCap: btcMarketCap,
    ethMarketCap: ethMarketCap,
    goldMarketCap: goldMarketCap,
    timestamp: new Date(),
    usdM3MarketCap: usdM3MarketCap,
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
    TE.map(({ coins, ethPrice }) => insertableFromCoinData(coins, ethPrice)),
    TE.chainW((marketCaps) =>
      pipe(storeMarketCaps(marketCaps), T.map(E.right)),
    ),
    TE.chainFirstIOK(() => () => {
      Log.debug(`stored market caps at ${new Date().toISOString()}`);
    }),
    TE.chainFirstTaskK(() => sqlTNotify("cache-update", marketCapsCacheKey)),
  );

type MarketCapsRow = {
  btcMarketCap: number;
  ethMarketCap: number;
  goldMarketCap: number;
  timestamp: string;
  usdM3MarketCap: number;
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
        O.fromNullableK((rows) => rows[0]?.value),
        O.map((obj) => ({
          ...obj,
          timestamp: new Date(obj.timestamp),
        })),
      ),
    ),
  );
