import * as T from "fp-ts/lib/Task.js";
import { pipe } from "fp-ts/lib/function.js";
import { seqSPar } from "./sequence.js";
import { sql } from "./db.js";
import { BlockLondon } from "./eth_node.js";

export type FeesBurnedT = {
  feesBurned5m: number;
  feesBurned1h: number;
  feesBurned24h: number;
  feesBurned7d: number;
  feesBurned30d: number;
  feesBurnedAll: number;
};

export const calcFeesBurned = (block: BlockLondon): T.Task<FeesBurnedT> => {
  const feesBurned5m = () =>
    sql<{ baseFeeSum: number }[]>`
      SELECT SUM(base_fee_sum) AS base_fee_sum FROM blocks
      WHERE mined_at >= now() - interval '5 minutes'
      AND number <= ${block.number}
  `.then((rows) => rows[0]?.baseFeeSum ?? 0);

  const feesBurned1h = () =>
    sql<{ baseFeeSum: number }[]>`
      SELECT SUM(base_fee_sum) AS base_fee_sum FROM blocks
      WHERE mined_at >= now() - interval '1 hours'
      AND number <= ${block.number}
  `.then((rows) => rows[0]?.baseFeeSum ?? 0);

  const feesBurned24h = () =>
    sql<{ baseFeeSum: number; minedAt: Date }[]>`
      SELECT SUM(base_fee_sum) AS base_fee_sum FROM blocks
      WHERE mined_at >= now() - interval '24 hours'
      AND number <= ${block.number}
  `.then((rows) => rows[0]?.baseFeeSum ?? 0);

  const feesBurned7d = () =>
    sql<{ baseFeeSum: number; minedAt: Date }[]>`
      SELECT SUM(base_fee_sum) AS base_fee_sum FROM blocks
      WHERE mined_at >= now() - interval '7 days'
      AND number <= ${block.number}
  `.then((rows) => rows[0]?.baseFeeSum ?? 0);

  const feesBurned30d = () =>
    sql<{ baseFeeSum: number; minedAt: Date }[]>`
      SELECT SUM(base_fee_sum) AS base_fee_sum FROM blocks
      WHERE mined_at >= now() - interval '30 days'
      AND number <= ${block.number}
  `.then((rows) => rows[0]?.baseFeeSum ?? 0);

  const feesBurnedAll = () =>
    sql<{ baseFeeSum: number }[]>`
      SELECT SUM(base_fee_sum) as base_fee_sum FROM blocks
      WHERE number <= ${block.number}
  `.then((rows) => rows[0]?.baseFeeSum ?? 0);

  return seqSPar({
    feesBurned5m,
    feesBurned1h,
    feesBurned24h,
    feesBurned7d,
    feesBurned30d,
    feesBurnedAll,
  });
};

export const getFeesBurned = (blockNumber: number): T.Task<FeesBurnedT> => {
  return pipe(
    () => sql<{ feesBurned: FeesBurnedT }[]>`
      SELECT fees_burned FROM derived_block_stats
      WHERE number = ${blockNumber}
    `,
    T.map((rows) => rows[0].feesBurned),
  );
};
