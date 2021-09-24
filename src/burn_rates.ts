import * as T from "fp-ts/lib/Task.js";
import { pipe } from "fp-ts/lib/function.js";
import { seqSPar } from "./sequence.js";
import { sql } from "./db.js";
import { BlockLondon } from "./eth_node.js";

export type BurnRatesT = {
  burnRate5m: number;
  burnRate1h: number;
  burnRate24h: number;
  burnRate7d: number;
  burnRate30d: number;
  burnRateAll: number;
};

export const calcBurnRates = (block: BlockLondon): T.Task<BurnRatesT> => {
  const burnRate5m = () =>
    sql<{ burnPerMinute: number }[]>`
      SELECT SUM(base_fee_sum) / 5 AS burn_per_minute FROM blocks
      WHERE mined_at >= now() - interval '5 minutes'
      AND number <= ${block.number}
  `.then((rows) => rows[0]?.burnPerMinute ?? 0);

  const burnRate1h = () =>
    sql<{ burnPerMinute: number }[]>`
      SELECT SUM(base_fee_sum) / 60 AS burn_per_minute FROM blocks
      WHERE mined_at >= now() - interval '1 hours'
      AND number <= ${block.number}
  `.then((rows) => rows[0]?.burnPerMinute ?? 0);

  const burnRate24h = () =>
    sql<{ burnPerMinute: number }[]>`
      SELECT SUM(base_fee_sum) / (24 * 60) AS burn_per_minute FROM blocks
      WHERE mined_at >= now() - interval '24 hours'
      AND number <= ${block.number}
  `.then((rows) => rows[0]?.burnPerMinute ?? 0);

  // The more complex queries account for the fact we don't have all blocks in the queried period yet and can't assume the amount of minutes is the length of the period in days times the number of minutes in a day. Once we do we can simplify to the above.
  const burnRate7d = () =>
    sql<{ burnPerMinute: number }[]>`
      SELECT
        SUM(base_fee_sum) / (7 * 24 * 60) AS burn_per_minute
      FROM blocks
      WHERE mined_at >= now() - interval '7 days'
      AND number <= ${block.number}
  `.then((rows) => rows[0]?.burnPerMinute ?? 0);

  const burnRate30d = () =>
    sql<{ burnPerMinute: number }[]>`
      SELECT
        SUM(base_fee_sum) / (
          EXTRACT(epoch FROM now() - min(mined_at)) / 60
        ) AS burn_per_minute
      FROM blocks
      WHERE mined_at >= now() - interval '30 days'
      AND number <= ${block.number}
  `.then((rows) => rows[0]?.burnPerMinute ?? 0);

  const burnRateAll = () =>
    sql<{ burnPerMinute: number }[]>`
      SELECT
        SUM(base_fee_sum) / (
          EXTRACT(epoch FROM now() - '2021-08-05 12:33:42+00') / 60
        ) AS burn_per_minute
      FROM blocks
      WHERE number <= ${block.number}
  `.then((rows) => rows[0]?.burnPerMinute ?? 0);

  return seqSPar({
    burnRate5m,
    burnRate1h,
    burnRate24h,
    burnRate7d,
    burnRate30d,
    burnRateAll,
  });
};

export const getBurnRates = (blockNumber: number): T.Task<BurnRatesT> => {
  return pipe(
    () => sql<{ burnRates: BurnRatesT }[]>`
      SELECT burn_rates FROM derived_block_stats
      WHERE number = ${blockNumber}
    `,
    T.map((rows) => rows[0].burnRates),
  );
};
