import { pipe } from "fp-ts/lib/function.js";
import { sql } from "./db.js";
import { BlockLondon } from "./eth_node.js";
import { seqSParT, T } from "./fp.js";
import * as Timeframe from "./timeframe.js";
import { LimitedTimeframe } from "./timeframe.js";

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

type BaseFeeSum = {
  eth: number;
  usd: number;
};

const getTimeframeBaseFeeSum = (
  block: BlockLondon,
  timeframe: LimitedTimeframe,
): T.Task<BaseFeeSum> =>
  pipe(
    () => sql<BaseFeeSum[]>`
      SELECT
        SUM(base_fee_sum) AS eth,
        SUM(base_fee_sum * eth_price / 1e18) AS usd
      FROM blocks
      WHERE mined_at >= now() - ${Timeframe.intervalSqlMap[timeframe]}::interval
      AND number <= ${block.number}
    `,
    T.map((rows) => ({
      eth: rows[0]?.eth ?? 0,
      usd: rows[0]?.usd ?? 0,
    })),
  );

const getBaseFeeSum = (block: BlockLondon): T.Task<BaseFeeSum> =>
  pipe(
    () => sql<BaseFeeSum[]>`
      SELECT
        SUM(base_fee_sum) AS eth,
        SUM(base_fee_sum * eth_price / 1e18) AS usd
      FROM blocks
      WHERE number <= ${block.number}
    `,
    T.map((rows) => ({
      eth: rows[0]?.eth ?? 0,
      usd: rows[0]?.usd ?? 0,
    })),
  );

export const calcBaseFeeSums = (block: BlockLondon): T.Task<FeesBurnedT> =>
  pipe(
    seqSParT({
      feesBurned5m: getTimeframeBaseFeeSum(block, "5m"),
      feesBurned1h: getTimeframeBaseFeeSum(block, "1h"),
      feesBurned24h: getTimeframeBaseFeeSum(block, "24h"),
      feesBurned7d: getTimeframeBaseFeeSum(block, "7d"),
      feesBurned30d: getTimeframeBaseFeeSum(block, "30d"),
      feesBurnedAll: getBaseFeeSum(block),
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
