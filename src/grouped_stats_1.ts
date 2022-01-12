import * as FeeBurn from "./fee_burns.js";
import * as Blocks from "./blocks/blocks.js";
import * as BurnRecordsCache from "./burn-records/cache.js";
import * as BurnRates from "./burn_rates.js";
import { sql, sqlT, sqlTNotify, sqlTVoid } from "./db.js";
import { EthPrice } from "./eth_prices.js";
import { A, flow, O, OAlt, pipe, T, TAlt } from "./fp.js";
import * as LatestBlockFees from "./latest_block_fees.js";
import * as Leaderboards from "./leaderboards.js";
import * as LeaderboardsAll from "./leaderboards_all.js";
import * as LeaderboardsLimitedTimeframe from "./leaderboards_limited_timeframe.js";
import * as Log from "./log.js";
import * as Performance from "./performance.js";
import * as ScarcityCache from "./scarcity/cache.js";
import { serializeBigInt } from "./json.js";
import { LeaderboardEntries } from "./leaderboards.js";

export const groupedStats1Key = "grouped-stats-1";

export type GroupedStats1 = {
  baseFeePerGas: number;
  number: number;
  burnRates: BurnRates.BurnRatesT;
  burnRecords: BurnRecordsCache.BurnRecordsCache;
  ethPrice: EthPrice;
  feesBurned: FeeBurn.FeesBurnedT;
  latestBlockFees: LatestBlockFees.LatestBlockFees;
  leaderboards: Leaderboards.LeaderboardEntries;
};

export const getLatestStats = () =>
  pipe(
    sqlT<{ value: GroupedStats1 }[]>`
      SELECT value FROM key_value_store
      WHERE key = ${groupedStats1Key}
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

export const updateGroupedStats1 = (
  block: Blocks.BlockDb,
  ethPrice: EthPrice,
) =>
  pipe(
    Log.debug("computing grouped stats 1"),
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
          T.chain(() => BurnRecordsCache.getRecordsCache()),
        ),
      ),
    ),
    T.apS("scarcity", ScarcityCache.updateScarcityCache(block)),
    T.apS("latestBlockFees", LatestBlockFees.getLatestBlockFees(block.number)),
    T.map(
      ({
        burnRates,
        burnRecords,
        latestBlockFees,
        leaderboards,
      }): GroupedStats1 => ({
        baseFeePerGas: Number(block.baseFeePerGas),
        number: block.number,
        burnRates: burnRates,
        burnRecords,
        ethPrice,
        latestBlockFees,
        leaderboards: leaderboards,
        feesBurned: FeeBurn.getFeeBurnsOld(),
      }),
    ),
    T.map((groupedStats) => ({
      key: "grouped-stats-1",
      value: JSON.stringify(groupedStats, serializeBigInt),
    })),
    T.chain(
      (insertable) => sqlTVoid`
        INSERT INTO key_value_store
          ${sql(insertable)}
        ON CONFLICT (key) DO UPDATE SET
          value = excluded.value
      `,
    ),
    T.chain(() => sqlTNotify("cache-update", "grouped-stats-1")),
  );

export const getLatestLeaderboards = (): T.Task<{
  blockNumber: number;
  leaderboards: LeaderboardEntries;
}> =>
  pipe(
    sqlT<
      { value: { blockNumber: number; leaderboards: LeaderboardEntries } }[]
    >`
      SELECT value FROM key_value_store
      WHERE key = 'grouped-stats-1'
    `,
    T.map(
      flow(
        A.head,
        O.map((row) => ({
          blockNumber: row.value.blockNumber,
          leaderboards: row.value.leaderboards,
        })),
        OAlt.getOrThrow(
          "empty derived block stats, can't return latest leaderboards",
        ),
      ),
    ),
  );
