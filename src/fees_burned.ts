import * as T from "fp-ts/lib/Task.js";
import { pipe } from "fp-ts/lib/function.js";
import { sql } from "./db.js";
import { BlockLondon } from "./eth_node.js";
import { seqSParT } from "./fp.js";
import { LimitedTimeframe } from "./leaderboards.js";

export type FeesBurnedT = {
  feesBurned5m: number;
  feesBurned5mUsd: number;
  feesBurned1h: number;
  feesBurned1hUsd: number;
  feesBurned24h: number;
  feesBurned24hUsd: number;
  feesBurned7d: number;
  feesBurned7dUsd: number;
  feesBurned30d: number;
  feesBurned30dUsd: number;
  feesBurnedAll: number;
  feesBurnedAllUsd: number;
};

const timeframeIntervalMap: Record<LimitedTimeframe, string> = {
  "5m": "5 minutes",
  "1h": "1 hours",
  "24h": "24 hours",
  "7d": "7 days",
  "30d": "30 days",
};

const getTimeframeSum = (
  limitedTimeframe: LimitedTimeframe,
  upToAndIncluding: number,
) => {
  const intervalSql = timeframeIntervalMap[limitedTimeframe];

  return pipe(
    () => sql<{ baseFeeSum: number; baseFeeSumUsd: number }[]>`
      SELECT
        SUM(base_fee_sum) AS base_fee_sum,
        SUM(base_fee_sum * eth_price) AS base_fee_sum_usd
      FROM blocks
      WHERE mined_at >= now() - interval '${sql(intervalSql)}'
      AND number <= ${upToAndIncluding}
    `,
    T.map((rows) => ({
      eth: rows[0]?.baseFeeSum ?? 0,
      usd: rows[0]?.baseFeeSumUsd ?? 0,
    })),
  );
};

const getSum = (upToAndIncluding: number) =>
  pipe(
    () =>
      sql<{ baseFeeSum: number; baseFeeSumUsd: number }[]>`
        SELECT
          SUM(base_fee_sum) as base_fee_sum,
          SUM(base_fee_sum * eth_price) AS base_fee_sum_usd
        FROM blocks
        WHERE number <= ${upToAndIncluding}
    `,
    T.map((rows) => ({
      eth: rows[0]?.baseFeeSum ?? 0,
      usd: rows[0]?.baseFeeSumUsd ?? 0,
    })),
  );

export const calcFeesBurned = (block: BlockLondon): T.Task<FeesBurnedT> => {
  return pipe(
    seqSParT({
      feesBurned5m: getTimeframeSum("5m", block.number),
      feesBurned1h: getTimeframeSum("1h", block.number),
      feesBurned24h: getTimeframeSum("24h", block.number),
      feesBurned7d: getTimeframeSum("7d", block.number),
      feesBurned30d: getTimeframeSum("30d", block.number),
      feesBurnedAll: getSum(block.number),
    }),
    T.map((fees) => ({
      feesBurned5m: fees.feesBurned5m.eth,
      feesBurned5mUsd: fees.feesBurned5m.usd,
      feesBurned1h: fees.feesBurned1h.eth,
      feesBurned1hUsd: fees.feesBurned1h.usd,
      feesBurned24h: fees.feesBurned24h.eth,
      feesBurned24hUsd: fees.feesBurned24h.usd,
      feesBurned7d: fees.feesBurned7d.eth,
      feesBurned7dUsd: fees.feesBurned7d.usd,
      feesBurned30d: fees.feesBurned30d.eth,
      feesBurned30dUsd: fees.feesBurned30d.usd,
      feesBurnedAll: fees.feesBurnedAll.eth,
      feesBurnedAllUsd: fees.feesBurnedAll.usd,
    })),
  );
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
