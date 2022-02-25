import * as Blocks from "./blocks/blocks.js";
import * as BurnRecordsCache from "./burn-records/cache.js";
import * as BurnRates from "./burn_rates.js";
import { sql, sqlT, sqlTNotify, sqlTVoid } from "./db.js";
import * as DeflationaryStreak from "./deflationary_streaks.js";
import * as EthPrices from "./eth-prices/eth_prices.js";
import * as FeeBurn from "./fee_burn.js";
import { A, flow, O, OAlt, pipe, T, TAlt, TE } from "./fp.js";
import { serializeBigInt } from "./json.js";
import * as LatestBlockFees from "./latest_block_fees.js";
import * as Leaderboards from "./leaderboards.js";
import { LeaderboardEntries } from "./leaderboards.js";
import * as LeaderboardsAll from "./leaderboards_all.js";
import * as LeaderboardsLimitedTimeframe from "./leaderboards_limited_timeframe.js";
import * as Log from "./log.js";
import * as Performance from "./performance.js";

export const groupedAnalysis1CacheKey = "grouped-analysis-1";

export type GroupedAnalysis1 = {
  baseFeePerGas: number;
  burnRates: BurnRates.BurnRatesT;
  burnRecords: BurnRecordsCache.BurnRecordsCache["records"];
  deflationaryStreak: DeflationaryStreak.DeflationaryStreakForSite;
  ethPrice: EthPrices.EthStats | null;
  feeBurns: FeeBurn.FeesBurnedT;
  latestBlockFees: LatestBlockFees.LatestBlockFees;
  leaderboards: Leaderboards.LeaderboardEntries;
  number: number;
};

export const getLatestAnalysis = () =>
  pipe(
    sqlT<{ value: GroupedAnalysis1 }[]>`
      SELECT value FROM key_value_store
      WHERE key = ${groupedAnalysis1CacheKey}
    `,
    T.map((rows) => rows[0].value),
  );

const getLeaderboards = () =>
  pipe(
    TAlt.seqTParT(
      Performance.measureTaskPerf(
        "calc leaderboard all",
        LeaderboardsAll.calcLeaderboardAll(),
      ),
      Performance.measureTaskPerf(
        "calc leaderboard limited timeframes",
        LeaderboardsLimitedTimeframe.calcLeaderboardForLimitedTimeframes(),
      ),
    ),
    T.map(([leaderboardAll, leaderboardLimitedTimeframes]) => ({
      leaderboard5m: leaderboardLimitedTimeframes["5m"],
      leaderboard1h: leaderboardLimitedTimeframes["1h"],
      leaderboard24h: leaderboardLimitedTimeframes["24h"],
      leaderboard7d: leaderboardLimitedTimeframes["7d"],
      leaderboard30d: leaderboardLimitedTimeframes["30d"],
      leaderboardAll: leaderboardAll,
    })),
  );

export const updateAnalysis = (block: Blocks.BlockDb) =>
  pipe(
    Log.debug("computing grouped analysis 1"),
    () => T.Do,
    T.apS(
      "burnRates",
      Performance.measureTaskPerf(
        "calc burn rates",
        BurnRates.calcBurnRates(block),
      ),
    ),
    T.apS("leaderboards", getLeaderboards()),
    T.apS(
      "burnRecords",
      Performance.measureTaskPerf(
        "calc burn records",
        pipe(
          BurnRecordsCache.updateRecordsCache(block.number),
          T.chain(() =>
            pipe(
              BurnRecordsCache.getRecordsCache(),
              T.map((cache) => cache.records),
            ),
          ),
        ),
      ),
    ),
    T.apS("latestBlockFees", LatestBlockFees.getLatestBlockFees(block.number)),
    T.apS(
      "ethPrice",
      pipe(
        EthPrices.getEthStats(),
        TE.match(
          (e) => {
            Log.error("failed to compute eth stats", e);
            return null;
          },
          (v) => v,
        ),
      ),
    ),
    T.apS(
      "feeBurns",
      Performance.measureTaskPerf("get fee burns", FeeBurn.getFeeBurnsOld()),
    ),
    T.apS(
      "deflationaryStreak",
      Performance.measureTaskPerf(
        "get deflationary streak",
        DeflationaryStreak.getStreakStateForSite(),
      ),
    ),
    T.map(
      ({
        burnRates,
        burnRecords,
        deflationaryStreak,
        ethPrice,
        latestBlockFees,
        leaderboards,
        feeBurns,
      }): GroupedAnalysis1 => ({
        baseFeePerGas: Number(block.baseFeePerGas),
        number: block.number,
        burnRates: burnRates,
        burnRecords,
        deflationaryStreak,
        ethPrice,
        latestBlockFees,
        leaderboards: leaderboards,
        feeBurns,
      }),
    ),
    T.map((groupedAnalysis1) => ({
      key: groupedAnalysis1CacheKey,
      value: JSON.stringify(groupedAnalysis1, serializeBigInt),
    })),
    T.chain(
      (insertable) => sqlTVoid`
        INSERT INTO key_value_store
          ${sql(insertable)}
        ON CONFLICT (key) DO UPDATE SET
          value = excluded.value
      `,
    ),
    T.chain(() => sqlTNotify("cache-update", groupedAnalysis1CacheKey)),
  );

export const getLatestLeaderboards = (): T.Task<{
  number: number;
  leaderboards: LeaderboardEntries;
}> =>
  pipe(
    sqlT<{ value: { number: number; leaderboards: LeaderboardEntries } }[]>`
      SELECT value FROM key_value_store
      WHERE key = ${groupedAnalysis1CacheKey}
    `,
    T.map(
      flow(
        A.head,
        O.map((row) => ({
          number: row.value.number,
          leaderboards: row.value.leaderboards,
        })),
        OAlt.getOrThrow(
          "empty derived block stats, can't return latest leaderboards",
        ),
      ),
    ),
  );
