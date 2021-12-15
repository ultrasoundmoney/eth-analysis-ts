import { pipe } from "fp-ts/lib/function.js";
import * as T from "fp-ts/lib/Task.js";
import { FeesBurnedT } from "./base_fee_sums.js";
// import { BurnRecordsT } from "./burn-records/burn_records.js";
import { BurnRatesT } from "./burn_rates.js";
import { sql } from "./db.js";
import { LeaderboardEntries } from "./leaderboards.js";

export type DerivedBlockStats = {
  blockNumber: number;
  burnRates: BurnRatesT;
  // burnRecords: BurnRecordsT;
  feesBurned: FeesBurnedT;
  leaderboards: LeaderboardEntries;
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

export const getDerivedBlockStats = (
  blockNumber: number,
): T.Task<DerivedBlockStats | undefined> =>
  pipe(
    () =>
      sql<DerivedBlockStats[]>`
        SELECT * FROM derived_block_stats
        WHERE block_number = ${blockNumber}
    `,
    T.map((rows) => rows[0]),
  );

export const storeDerivedBlockStats = ({
  blockNumber,
  burnRates,
  // burnRecords,
  feesBurned,
  leaderboards,
}: DerivedBlockStats): T.Task<void> => {
  return pipe(
    () => sql`
      INSERT INTO derived_block_stats (
        block_number,
        burn_rates,
        fees_burned,
        leaderboards
      )
      VALUES (
        ${blockNumber},
        ${sql.json(burnRates)},
        ${sql.json(feesBurned)},
        ${sql.json(leaderboards)}
      )
      ON CONFLICT (block_number) DO UPDATE SET
        burn_rates = ${sql.json(burnRates)},
        fees_burned = ${sql.json(feesBurned)},
        leaderboards = ${sql.json(leaderboards)}
    `,
    T.map(() => undefined),
  );
};

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
