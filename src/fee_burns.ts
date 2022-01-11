import * as Blocks from "./blocks/blocks.js";
import { BlockDb } from "./blocks/blocks.js";
import { sqlT } from "./db.js";
import { WeiBI } from "./eth_units.js";
import { B, IO, pipe, RA, T, TAlt } from "./fp.js";
import * as Log from "./log.js";
import * as TimeFrames from "./time_frames.js";
import { LimitedTimeFrameNext, TimeFrameNext } from "./time_frames.js";
import { Usd } from "./usd_scaling.js";

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

type PreciseBaseFeeSum = {
  eth: WeiBI;
  usd: Usd;
};

const getSumForAll = () =>
  pipe(
    sqlT<{ eth: string; usd: number }[]>`
      SELECT
        SUM(gas_used::numeric(78) * base_fee_per_gas::numeric(78)) AS eth,
        SUM(gas_used::float8 * base_fee_per_gas::float8 * eth_price / 10e18) AS usd
      FROM blocks
    `,
  );

const getSumForInterval = (timeFrame: LimitedTimeFrameNext) =>
  pipe(
    TimeFrames.intervalSqlMapNext[timeFrame],
    (interval) => sqlT<{ eth: string; usd: number }[]>`
      SELECT
        SUM(gas_used::numeric(78) * base_fee_per_gas::numeric(78)) AS eth,
        SUM(gas_used::float8 * base_fee_per_gas::float8 * eth_price / 10e18) AS usd
      FROM blocks
      WHERE mined_at >= NOW() - ${interval}::interval
    `,
  );

export const getInitSumForTimeFrame = (
  timeFrame: TimeFrameNext,
): T.Task<PreciseBaseFeeSum> =>
  pipe(
    timeFrame === "all",
    B.match(
      () => getSumForInterval(timeFrame as LimitedTimeFrameNext),
      () => getSumForAll(),
    ),
    T.map((rows) => ({ eth: BigInt(rows[0].eth), usd: rows[0].usd })),
  );

type BaseFeeSums = Record<TimeFrameNext, PreciseBaseFeeSum>;
const currentBurnedMap = pipe(
  TimeFrames.timeFramesNext,
  RA.reduce({} as BaseFeeSums, (map, timeFrame) => {
    map[timeFrame] = {
      eth: 0n,
      usd: 0,
    };
    return map;
  }),
);

const addToCurrent = (
  timeFrame: TimeFrameNext,
  sum: PreciseBaseFeeSum,
): IO.IO<void> =>
  pipe(
    currentBurnedMap[timeFrame],
    (currentBurned) => ({
      eth: currentBurned.eth + sum.eth,
      usd: currentBurned.usd + sum.usd,
    }),
    (newCurrentBurned) => () => {
      currentBurnedMap[timeFrame] = newCurrentBurned;
    },
  );

export const init = (): T.Task<void> =>
  pipe(
    TimeFrames.timeFramesNext,
    T.traverseArray((timeFrame) =>
      pipe(
        getInitSumForTimeFrame(timeFrame),
        T.chain((sum) => T.fromIO(addToCurrent(timeFrame, sum))),
      ),
    ),
    TAlt.concatAllVoid,
  );

export const onNewBlock = (block: BlockDb): void => {
  for (const timeFrame of TimeFrames.timeFramesNext) {
    addToCurrent(timeFrame, {
      eth: block.baseFeePerGas * block.gasUsed,
      usd:
        (Number(block.baseFeePerGas * block.gasUsed) * block.ethPrice) /
        10 ** 18,
    });
  }
};

export const onRollback = (block: BlockDb): void => {
  for (const timeFrame of TimeFrames.timeFramesNext) {
    const isBlockWithinTimeFrame = Blocks.getIsBlockWithinTimeFrame(
      block.number,
      timeFrame,
    );

    if (!isBlockWithinTimeFrame) {
      return;
    }

    addToCurrent(timeFrame, {
      eth: block.baseFeePerGas * block.gasUsed * -1n,
      usd:
        ((Number(block.baseFeePerGas * block.gasUsed) * block.ethPrice) /
          10 ** 18) *
        -1,
    });
  }
};

export const getFeeBurns = (): BaseFeeSums => currentBurnedMap;

export const getAllFeesBurned = (): PreciseBaseFeeSum =>
  currentBurnedMap["all"];

export const getFeeBurnsOld = (): FeesBurnedT => ({
  feesBurned5m: Number(currentBurnedMap.m5.eth),
  feesBurned5mUsd: currentBurnedMap.m5.usd,
  feesBurned1h: Number(currentBurnedMap.h1.eth),
  feesBurned1hUsd: currentBurnedMap.h1.usd,
  feesBurned24h: Number(currentBurnedMap.d1.eth),
  feesBurned24hUsd: currentBurnedMap.d1.usd,
  feesBurned7d: Number(currentBurnedMap.d7.eth),
  feesBurned7dUsd: currentBurnedMap.d7.usd,
  feesBurned30d: Number(currentBurnedMap.d30.eth),
  feesBurned30dUsd: currentBurnedMap.d30.usd,
  feesBurnedAll: Number(currentBurnedMap.all.eth),
  feesBurnedAllUsd: currentBurnedMap.all.usd,
});
