import { pipe } from "fp-ts/lib/function.js";
import * as T from "fp-ts/lib/Task.js";
import { FeesBurnedT } from "./base_fee_sums.js";
// import { BurnRecordsT } from "./burn-records/burn_records.js";
import { BurnRatesT } from "./burn_rates.js";
import { sql, sqlT } from "./db.js";
import { A, TE } from "./fp.js";
import { serializeBigInt } from "./json.js";
import { LeaderboardEntries } from "./leaderboards.js";
import { ScarcityT } from "./scarcity/scarcity.js";

export type DerivedBlockStats = {
  // burnRecords: BurnRecordsT;
  blockNumber: number;
  burnRates: BurnRatesT;
  feesBurned: FeesBurnedT;
  leaderboards: LeaderboardEntries;
  scarcity: ScarcityT;
};

export type DerivedBlockStatsSerialized = {
  blockNumber: number;
  burnRates: BurnRatesT;
  // burnRecords: BurnRecordsT;
  feesBurned: FeesBurnedT;
  leaderboards: LeaderboardEntries;
};

export const getLatestDerivedBlockStats = (): T.Task<DerivedBlockStats> => () =>
  sql<DerivedBlockStats[]>`
    SELECT * FROM derived_block_stats
    WHERE block_number = (
      SELECT MAX(block_number) FROM derived_block_stats
    )
`.then((rows) => rows[0]);

export const getLatestStatsWithLeaderboards =
  async (): Promise<DerivedBlockStats> => {
    const rows = await sql<DerivedBlockStats[]>`
      SELECT * FROM derived_block_stats
      WHERE block_number = (
        SELECT MAX(block_number) FROM derived_block_stats
        WHERE leaderboards IS NOT NULL
      )
    `;
    return rows[0];
  };

export const getDerivedBlockStats = async (
  blockNumber: number,
): Promise<DerivedBlockStats | undefined> => {
  const rows = await sql<DerivedBlockStats[]>`
    SELECT * FROM derived_block_stats
    WHERE block_number = ${blockNumber}
  `;

  return rows[0];
};

type InsertableDerivedStats = {
  block_number: number;
  burn_rates: string;
  fees_burned: string;
  leaderboards: string;
  scarcity: string;
};

const insertableFromDerivedStats = (
  stats: DerivedBlockStats,
): InsertableDerivedStats => ({
  block_number: stats.blockNumber,
  burn_rates: JSON.stringify(stats.burnRates),
  fees_burned: JSON.stringify(stats.feesBurned),
  leaderboards: JSON.stringify(stats.leaderboards),
  scarcity: JSON.stringify(stats.scarcity, serializeBigInt),
});

export const storeDerivedBlockStats = (stats: DerivedBlockStats) =>
  pipe(
    stats,
    insertableFromDerivedStats,
    (insertable) => sqlT`
      INSERT INTO derived_block_stats
        ${sql(insertable)}
      ON CONFLICT (block_number) DO UPDATE SET
      burn_rates = excluded.burn_rates,
      fees_burned = excluded.fees_burned,
      leaderboards = excluded.leaderboards,
      scarcity = excluded.scarcity
    `,
  );

export const deleteOldDerivedStats = (): T.Task<void> =>
  pipe(
    () => sql`
      DELETE FROM derived_block_stats
      WHERE block_number IN (
        SELECT block_number FROM derived_block_stats
        ORDER BY block_number DESC
        OFFSET 100
      )
    `,
    T.map(() => undefined),
  );

class NoDerivedStatsError extends Error {}

export const getLatestLeaderboards = (): TE.TaskEither<
  NoDerivedStatsError,
  { blockNumber: number; leaderboards: LeaderboardEntries }
> =>
  pipe(
    sqlT<{ blockNumber: number; leaderboards: LeaderboardEntries }[]>`
      SELECT block_number, leaderboards FROM derived_block_stats
      WHERE block_number = (
        SELECT MAX(block_number) FROM derived_block_stats
      )
    `,
    T.map(A.head),
    TE.fromTaskOption(() => new NoDerivedStatsError()),
  );
