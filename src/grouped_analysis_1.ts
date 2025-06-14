import * as Blocks from "./blocks/blocks.js";
import * as BurnRecordsCache from "./burn-records/cache.js";
import * as BurnRates from "./burn_rates.js";
import { sql, sqlT, sqlTNotify, sqlTVoid } from "./db.js";
import * as DeflationaryStreak from "./deflationary_streaks.js";
import * as DeflationaryBlobStreak from "./deflationary_blob_streaks.js";
import * as EthPrices from "./eth-prices/index.js";
import * as FeeBurn from "./fee_burn.js";
import { A, flow, O, OAlt, pipe, T, TAlt, TE } from "./fp.js";
import { serializeBigInt } from "./json.js";
import * as LatestBlockFees from "./latest_block_fees.js";
import * as Leaderboards from "./leaderboards.js";
import { LeaderboardEntries } from "./leaderboards.js";
import * as LeaderboardsUnlimitedTimeframe from "./leaderboards_unlimited_time_frames.js";
import * as LeaderboardsLimitedTimeframe from "./leaderboards_limited_timeframe.js";
import * as Log from "./log.js";
import * as Performance from "./performance.js";

export const groupedAnalysis1CacheKey = "grouped-analysis-1";

export type GroupedAnalysis1 = {
  baseFeePerGas: number;
  blobBaseFee: number | null;
  burnRates: BurnRates.BurnRatesT;
  blobBurnRates: BurnRates.BurnRatesT;
  burnRecords: BurnRecordsCache.BurnRecordsCache["records"];
  deflationaryStreak: DeflationaryStreak.StreakForSite;
  deflationaryBlobStreak: DeflationaryStreak.StreakForSite;
  ethPrice: EthPrices.EthStats | undefined;
  feeBurns: FeeBurn.FeesBurnedT;
  blobFeeBurns: FeeBurn.FeesBurnedT;
  latestBlockFees: LatestBlockFees.LatestBlockFees;
  latestBlockFeesFlipped: LatestBlockFees.LatestBlockFees;
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
    TAlt.seqTSeq(
      pipe(
        LeaderboardsUnlimitedTimeframe.calcLeaderboard("all"),
        Performance.measureTaskPerf("  per-refresh leaderboard all"),
      ),
      pipe(
        LeaderboardsUnlimitedTimeframe.calcLeaderboard("since_merge"),
        Performance.measureTaskPerf("  per-refresh leaderboard since_merge"),
      ),
      pipe(
        LeaderboardsLimitedTimeframe.calcLeaderboardForLimitedTimeframes(),
        Performance.measureTaskPerf(
          "  per-refresh leaderboard limited timeframes",
        ),
      ),
    ),
    T.map(
      ([
        leaderboardAll,
        leaderboardSinceMerge,
        leaderboardLimitedTimeframes,
      ]) => ({
        leaderboard5m: leaderboardLimitedTimeframes["5m"],
        leaderboard1h: leaderboardLimitedTimeframes["1h"],
        leaderboard24h: leaderboardLimitedTimeframes["24h"],
        leaderboard7d: leaderboardLimitedTimeframes["7d"],
        leaderboard30d: leaderboardLimitedTimeframes["30d"],
        leaderboardAll: leaderboardAll,
        leaderboardSinceBurn: leaderboardAll,
        leaderboardSinceMerge,
      }),
    ),
  );

export const updateAnalysis = (block: Blocks.BlockV1) =>
  pipe(
    Log.debug("computing grouped analysis 1"),
    () => T.Do,
    T.bind("feeBurns", () =>
      pipe(
        FeeBurn.getFeeBurnsOld(),
        Performance.measureTaskPerf("  per-refresh fee burns"),
      ),
    ),
    T.bind("burnRates", ({ feeBurns }) =>
      pipe(
        BurnRates.calcBurnRates(feeBurns),
        T.of,
        Performance.measureTaskPerf("  per-refresh burn rates"),
      ),
    ),
    T.bind("blobFeeBurns", () =>
      pipe(
        FeeBurn.getBlobFeeBurnsOld(),
        Performance.measureTaskPerf("  per-refresh blob fee burns"),
      ),
    ),
    T.bind("blobBurnRates", ({ blobFeeBurns }) =>
      pipe(
        BurnRates.calcBurnRates(blobFeeBurns),
        T.of,
        Performance.measureTaskPerf("  per-refresh blob burn rates"),
      ),
    ),
    T.bind("leaderboards", () => getLeaderboards()),
    T.bind("burnRecords", () =>
      pipe(
        pipe(
          BurnRecordsCache.updateRecordsCache(block.number),
          T.chain(() =>
            pipe(
              BurnRecordsCache.getRecordsCache(),
              T.map((cache) => cache.records),
            ),
          ),
        ),
        Performance.measureTaskPerf("  per-refresh burn records"),
      ),
    ),
    T.bind("latestBlockFees", () =>
      pipe(
        LatestBlockFees.getLatestBlockFees(block.number),
        Performance.measureTaskPerf("  per-refresh latest blocks"),
      ),
    ),
    T.bind("ethPrice", () =>
      pipe(
        pipe(
          EthPrices.getEthStats(),
          TE.match(
            (e) => {
              Log.error("failed to compute eth stats", e);
              return undefined;
            },
            (v) => v,
          ),
        ),
        Performance.measureTaskPerf("  per-refresh eth price + 24h change"),
      ),
    ),
    T.bind("deflationaryStreak", () =>
      pipe(
        DeflationaryStreak.getStreakForSite(block),
        Performance.measureTaskPerf("  per-refresh deflationary streak"),
      ),
    ),
    T.bind("deflationaryBlobStreak", () =>
      pipe(
        DeflationaryBlobStreak.getStreakForSite(block),
        Performance.measureTaskPerf("  per-refresh deflationary blob streak"),
      ),
    ),
    T.map(
      ({
        burnRates,
        blobBurnRates,
        burnRecords,
        deflationaryStreak,
        deflationaryBlobStreak,
        ethPrice,
        feeBurns,
        blobFeeBurns,
        latestBlockFees,
        leaderboards,
      }): GroupedAnalysis1 => ({
        baseFeePerGas: Number(block.baseFeePerGas),
        blobBaseFee: block.blobBaseFee ? Number(block.blobBaseFee) : null,
        burnRates: burnRates,
        blobBurnRates,
        burnRecords,
        deflationaryStreak,
        deflationaryBlobStreak,
        ethPrice,
        feeBurns,
        blobFeeBurns,
        latestBlockFees,
        latestBlockFeesFlipped: [...latestBlockFees].reverse(),
        leaderboards: leaderboards,
        number: block.number,
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

export const getLatestGroupedAnalysisNumber = () =>
  pipe(
    sqlT<{ number: number }[]>`
      SELECT value::json->'number' AS number
      FROM key_value_store
      WHERE key = ${groupedAnalysis1CacheKey}
    `,
    T.map(O.fromNullableK((rows) => rows[0]?.number)),
  );
