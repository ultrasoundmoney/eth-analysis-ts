import * as T from "fp-ts/lib/Task.js";
import { BlockLondon } from "./eth_node.js";
import { BurnRatesT } from "./burn_rates.js";
import { FeesBurnedT } from "./fees_burned.js";
import { LeaderboardEntries } from "./leaderboards.js";
import { pipe } from "fp-ts/lib/function.js";
import { sql } from "./db.js";

export type DerivedBlockStats = {
  burnRates: BurnRatesT;
  feesBurned: FeesBurnedT;
  leaderboards: LeaderboardEntries;
};

export const getLatestDerivedBlockStats = (): T.Task<DerivedBlockStats> => () =>
  sql<DerivedBlockStats[]>`
  SELECT * FROM derived_block_stats
  WHERE block_number = (SELECT MAX(block_number) FROM derived_block_stats)
`.then((rows) => rows[0]);

export const getDerivedBlockStats =
  (block: BlockLondon): T.Task<DerivedBlockStats> =>
  () =>
    sql<DerivedBlockStats[]>`
      SELECT * FROM derived_block_stats
      WHERE block_number = ${block.number}
    `.then((rows) => rows[0]);

export const storeDerivedBlockStats = (
  block: BlockLondon,
  { burnRates, feesBurned, leaderboards }: DerivedBlockStats,
): T.Task<void> => {
  return pipe(
    () => sql`
      INSERT INTO derived_block_stats (
        block_number,
        burn_rates,
        fees_burned,
        leaderboards
      )
      VALUES (
        ${block.number},
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
