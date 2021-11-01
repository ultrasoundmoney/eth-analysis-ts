import * as T from "fp-ts/lib/Task.js";
import { pipe } from "fp-ts/lib/function.js";
import { sql } from "./db.js";
import { BlockLondon } from "./eth_node.js";
import { seqSParT } from "./fp.js";
import { LimitedTimeframe } from "./leaderboards.js";

export type BurnRatesT = {
  burnRate5m: number;
  burnRate1h: number;
  burnRate24h: number;
  burnRate7d: number;
  burnRate30d: number;
  burnRateAll: number;
};

type BurnRate = {
  eth: number;
  usd: number;
};

const timeframeIntervalMap: Record<LimitedTimeframe, string> = {
  "5m": "5 minutes",
  "1h": "1 hours",
  "24h": "24 hours",
  "7d": "7 days",
  "30d": "30 days",
};

const timeframeMinutesMap: Record<LimitedTimeframe, number> = {
  "5m": 5,
  "1h": 60,
  "24h": 24 * 60,
  "7d": 7 * 24 * 60,
  "30d": 30 * 24 * 60,
};

const getTimeframeBurnRate = (
  block: BlockLondon,
  timeframe: LimitedTimeframe,
) =>
  pipe(
    () => sql<BurnRate[]>`
      SELECT
        SUM(base_fee_sum) / ${timeframeMinutesMap[timeframe]}::int AS eth,
        SUM(base_fee_sum * eth_price) / ${timeframeMinutesMap[timeframe]}::int AS usd
      FROM blocks
      WHERE mined_at >= now() - interval '${timeframeIntervalMap[timeframe]}'
      AND number <= ${block.number}
    `,
    T.map((rows) => ({ eth: rows[0]?.eth ?? 0, usd: rows[0]?.usd ?? 0 })),
  );

const getBurnRate = (block: BlockLondon) =>
  pipe(
    () => sql`
      SELECT
        SUM(base_fee_sum) / (
          EXTRACT(epoch FROM now() - '2021-08-05 12:33:42+00') / 60
        ) AS eth,
        SUM(base_fee_sum * eth_price) / (
          EXTRACT(epoch FROM now() - '2021-08-05 12:33:42+00') / 60
        ) AS usd
      FROM blocks
      WHERE number <= ${block.number}
`,
    T.map((rows) => ({ eth: rows[0]?.eth ?? 0, usd: rows[0]?.usd ?? 0 })),
  );

export const calcBurnRates = (block: BlockLondon): T.Task<BurnRatesT> => {
  return pipe(
    seqSParT({
      burnRate5m: getTimeframeBurnRate(block, "5m"),
      burnRate1h: getTimeframeBurnRate(block, "1h"),
      burnRate24h: getTimeframeBurnRate(block, "24h"),
      burnRate7d: getTimeframeBurnRate(block, "7d"),
      burnRate30d: getTimeframeBurnRate(block, "30d"),
      burnRateAll: getBurnRate(block),
    }),
    T.map((burnRates) => ({
      burnRate5m: burnRates.burnRate5m.eth,
      burnRate5mUsd: burnRates.burnRate5m.usd,
      burnRate1h: burnRates.burnRate1h.eth,
      burnRate1hUsd: burnRates.burnRate1h.usd,
      burnRate24h: burnRates.burnRate24h.eth,
      burnRate24hUsd: burnRates.burnRate24h.usd,
      burnRate7d: burnRates.burnRate7d.eth,
      burnRate7dUsd: burnRates.burnRate7d.usd,
      burnRate30d: burnRates.burnRate30d.eth,
      burnRate30dUsd: burnRates.burnRate30d.usd,
      burnRateAll: burnRates.burnRateAll.eth,
      burnRateAllUsd: burnRates.burnRateAll.usd,
    })),
  );
};
