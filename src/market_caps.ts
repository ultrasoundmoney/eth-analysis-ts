import * as Coingecko from "./coingecko.js";
import { sql } from "./db.js";
import * as Duration from "./duration.js";
import * as EthPrices from "./eth_prices.js";
import { E, pipe, T, TE, TEAlt } from "./fp.js";
import * as Log from "./log.js";

export type MarketCaps = {
  btcMarketCap: number;
  ethMarketCap: number;
  goldMarketCap: number;
  usdM3MarketCap: number;
  timestamp: Date;
};

const storeMarketCaps = (marketCaps: MarketCapRow): T.Task<void> =>
  pipe(
    () => sql`
        INSERT INTO market_caps
        ${sql(marketCaps)}
      `,
    T.map(() => undefined),
  );

const trimMarketCapsTable = (): T.Task<void> =>
  pipe(
    () => sql`
        DELETE FROM market_caps
        WHERE timestamp IN (
          SELECT timestamp FROM market_caps
          ORDER BY timestamp DESC
          OFFSET 1
        )
      `,
    T.map(() => undefined),
  );

type MarketCapRow = {
  btc_market_cap: number;
  eth_market_cap: number;
  gold_market_cap: number;
  usd_m3_market_cap: number;
  timestamp: Date;
};

const insertableFromCoinData = (
  coins: Coingecko.PriceResponse,
  ethPrice: EthPrices.EthPrice,
): MarketCapRow => {
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

export const storeCurrentMarketCaps = (): TE.TaskEither<
  Coingecko.CoinGeckoApiError,
  void
> =>
  pipe(
    TEAlt.seqTParTE(
      Coingecko.getSimpleCoins(),
      EthPrices.getEthPrice(new Date(), Duration.millisFromMinutes(5)),
    ),
    TE.map(
      ([coins, ethPrice]): MarketCapRow =>
        insertableFromCoinData(coins, ethPrice),
    ),
    TE.chainW((marketCaps) =>
      pipe(storeMarketCaps(marketCaps), T.map(E.right)),
    ),
    // // Don't let the table grow too much.
    TE.chainW(() => pipe(trimMarketCapsTable(), T.map(E.right))),
    TE.chainFirstIOK(() => () => {
      Log.debug(`stored market caps at ${new Date().toISOString()}`);
    }),
  );

export const getStoredMarketCaps = async (): Promise<MarketCaps> =>
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
