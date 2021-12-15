import * as Blocks from "./blocks/blocks.js";
import { BlockDb } from "./blocks/blocks.js";
import { sql } from "./db.js";
import { WeiBI } from "./eth_units.js";
import * as TimeFrames from "./time_frames.js";
import { LimitedTimeFrame, TimeFrame } from "./time_frames.js";
import { Usd } from "./usd_scaling.js";

type PreciseBaseFeeSum = {
  eth: WeiBI;
  usd: Usd;
};

export const getInitSumForTimeFrame = async (
  timeFrame: TimeFrame,
): Promise<PreciseBaseFeeSum> => {
  const lastStoredBlock = await Blocks.getLastStoredBlock();

  const getFromForTimeFrame = async (timeFrame: LimitedTimeFrame) => {
    const fromBlock = await Blocks.getPastBlock(
      lastStoredBlock,
      // Postgres gives an error when using milliseconds.
      `${TimeFrames.timeFrameMillisMap[timeFrame] / 1000} seconds`,
    );
    return fromBlock.number;
  };

  const from =
    timeFrame === "all"
      ? Blocks.londonHardForkBlockNumber
      : await getFromForTimeFrame(timeFrame);

  const rows = await sql<{ eth: string; usd: number }[]>`
    SELECT
      SUM(gas_used::numeric(78) * base_fee_per_gas::numeric(78)) AS eth,
      SUM(gas_used::float8 * base_fee_per_gas::float8 * eth_price / 10e18) AS usd
    FROM blocks
    WHERE number >= ${from}
  `;

  return {
    eth: BigInt(rows[0].eth),
    usd: rows[0].usd,
  };
};

type BaseFeeSums = Record<TimeFrame, PreciseBaseFeeSum>;
const current = {} as BaseFeeSums;

const addToCurrent = (timeFrame: TimeFrame, sum: PreciseBaseFeeSum) => {
  const eth = current[timeFrame]?.eth ?? 0n;
  const usd = current[timeFrame]?.usd ?? 0;

  current[timeFrame] = {
    eth: eth + sum.eth,
    usd: usd + sum.usd,
  };
};

export const init = async (): Promise<void> => {
  const tasks = TimeFrames.timeFrames.map(async (timeFrame) => {
    const sum = await getInitSumForTimeFrame(timeFrame);
    addToCurrent(timeFrame, sum);
  });
  await Promise.all(tasks);
};

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

export const getFeeBurns = (): BaseFeeSums => current;

export const getAllFeesBurned = (): PreciseBaseFeeSum => current["all"];
