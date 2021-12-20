import { BlockDb } from "./blocks/blocks.js";
import { sqlT } from "./db.js";
import { WeiBI } from "./eth_units.js";
import { B, pipe, T, TAlt } from "./fp.js";
import * as Log from "./log.js";
import * as TimeFrames from "./time_frames.js";
import { LimitedTimeFrame, TimeFrame } from "./time_frames.js";
import { Usd } from "./usd_scaling.js";

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

const getSumForInterval = (timeFrame: LimitedTimeFrame) =>
  pipe(
    TimeFrames.intervalSqlMap[timeFrame],
    (interval) => sqlT<{ eth: string; usd: number }[]>`
      SELECT
      SUM(gas_used::numeric(78) * base_fee_per_gas::numeric(78)) AS eth,
      SUM(gas_used::float8 * base_fee_per_gas::float8 * eth_price / 10e18) AS usd
      FROM blocks
      WHERE mined_at >= (SELECT MAX(mined_at) FROM blocks) - ${interval}::interval
    `,
  );

export const getInitSumForTimeFrame = (
  timeFrame: TimeFrame,
): T.Task<PreciseBaseFeeSum> =>
  pipe(
    timeFrame === "all",
    B.match(
      () => getSumForInterval(timeFrame as LimitedTimeFrame),
      () => getSumForAll(),
    ),
    T.map((rows) => ({ eth: BigInt(rows[0].eth), usd: rows[0].usd })),
    T.chainFirstIOK((baseFeeSum) => () => {
      Log.debug(
        `got precise fee burn for ${timeFrame}, eth: ${
          Number(baseFeeSum.eth) / 10 ** 18
        }`,
      );
    }),
  );

type BaseFeeSums = Record<TimeFrame, PreciseBaseFeeSum>;
const currentBurned: Record<TimeFrame, PreciseBaseFeeSum | undefined> = {
  "1h": undefined,
  "24h": undefined,
  "30d": undefined,
  "5m": undefined,
  "7d": undefined,
  all: undefined,
};

const addToCurrent = (timeFrame: TimeFrame, sum: PreciseBaseFeeSum) => {
  const eth = currentBurned[timeFrame]?.eth ?? 0n;
  const usd = currentBurned[timeFrame]?.usd ?? 0;

  currentBurned[timeFrame] = {
    eth: eth + sum.eth,
    usd: usd + sum.usd,
  };
};

export const init = (): T.Task<void> =>
  pipe(
    Log.debug("init precise fee burn"),
    () => TimeFrames.timeFrames,
    T.traverseArray((timeFrame) =>
      pipe(
        getInitSumForTimeFrame(timeFrame),
        T.chain((sum) => T.fromIO(() => addToCurrent(timeFrame, sum))),
      ),
    ),
    TAlt.concatAllVoid,
  );

export const onNewBlock = (block: BlockDb): void => {
  for (const timeFrame of TimeFrames.timeFrames) {
    addToCurrent(timeFrame, {
      eth: block.baseFeePerGas * block.gasUsed,
      usd:
        (Number(block.baseFeePerGas * block.gasUsed) * block.ethPrice) /
        10 ** 18,
    });
  }
};

export const onRollback = (block: BlockDb): void => {
  for (const timeFrame of TimeFrames.timeFrames) {
    addToCurrent(timeFrame, {
      eth: block.baseFeePerGas * block.gasUsed * -1n,
      usd:
        ((Number(block.baseFeePerGas * block.gasUsed) * block.ethPrice) /
          10 ** 18) *
        -1,
    });
  }
};

export const getFeeBurns = (): BaseFeeSums => {
  if (Object.values(currentBurned).some((value) => value === undefined)) {
    throw new Error("tried to get precise fee burns before init");
  }

  return currentBurned as BaseFeeSums;
};

export const getAllFeesBurned = (): PreciseBaseFeeSum => {
  if (currentBurned["all"] === undefined) {
    throw new Error("tried to get all precise fee burn before init");
  }

  return currentBurned["all"];
};
