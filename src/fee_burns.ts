import * as Blocks from "./blocks/blocks.js";
import { BlockDb } from "./blocks/blocks.js";
import { sql } from "./db.js";
import { WeiBI } from "./eth_units.js";
import * as Log from "./log.js";
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

  Log.debug(
    `got precise fee burn for ${timeFrame}, eth: ${
      Number(rows[0]?.eth) / 10 ** 18
    }`,
  );

  return {
    eth: BigInt(rows[0].eth),
    usd: rows[0].usd,
  };
};

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

export const init = async (): Promise<void> => {
  Log.debug("init precise fee burn");
  const tasks = TimeFrames.timeFrames.map(async (timeFrame) => {
    const sum = await getInitSumForTimeFrame(timeFrame);
    addToCurrent(timeFrame, sum);
  });
  await Promise.all(tasks);
  console.log(currentBurned);
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
