import * as DateFns from "date-fns";
import Denque from "denque";
import _ from "lodash";
import { BlockDb, FeeBlockRow } from "../blocks/blocks.js";
import * as Cartesian from "../cartesian.js";
import * as DateFnsAlt from "../date_fns_alt.js";
import { millisecondsBetweenAbs } from "../date_fns_alt.js";
import { Denomination, denominations } from "../denominations.js";
import * as Duration from "../duration.js";
import { millisFromHours, millisFromMinutes } from "../duration.js";
import { Ord, OrdM } from "../fp.js";
import * as __ from "../lodash_alt.js";
import * as Log from "../log.js";
import * as TimeFrames from "../time_frames.js";
import { TimeFrame } from "../time_frames.js";

// TODO: rename 'block' to 'b1'
export const blockGranularity = "block" as const;
export const timeGranularities = ["m5", "h1", "d1", "d7"] as const;
export const granularities = [blockGranularity, ...timeGranularities];

export type TimeGranularities = typeof timeGranularities[number];
export type Granularity = typeof granularities[number];

export const granularityMillisMap: Record<TimeGranularities, number> = {
  m5: millisFromMinutes(5),
  h1: millisFromHours(1),
  d1: millisFromHours(24),
  d7: millisFromHours(24 * 7),
};

export const sortings = ["min", "max"] as const;
export type Sorting = typeof sortings[number];

// Range of blocks and their fee sum.
export type FeeRecord = {
  firstBlock: number;
  lastBlock: number;
  feeSum: bigint;
};

export type FeeBlock = {
  number: number;
  minedAt: Date;
  feesEth: bigint;
  feesUsd: bigint;
};

export const makeRecordState = (
  granularity: Granularity,
  timeFrame: TimeFrame,
): RecordState => ({
  granularity,
  timeFrame,
  sums: new Denque(),
  topSumsMap: { eth: { max: [], min: [] }, usd: { max: [], min: [] } },
  feeBlocks: new Denque(),
  sumsRollbackBuffer: new Denque(),
  feeBlockRollbackBuffer: new Denque(),
});

export const granularitySqlMap: Record<Granularity, string> = {
  block: "0 seconds",
  m5: "5 minutes",
  h1: "1 hours",
  d1: "1 days",
  d7: "7 days",
};

export const getIsGranularityEnabledForTimeFrame = (
  granularity: Granularity,
  timeFrame: TimeFrame,
) => {
  if (granularity === "block") {
    return true;
  }

  if (timeFrame === "all") {
    return true;
  }

  const granularityMillis = granularityMillisMap[granularity];
  const timeFrameMillis = TimeFrames.timeFrameMillisMap[timeFrame];

  if (timeFrameMillis > granularityMillis) {
    return true;
  }

  return false;
};

export const recordStates = Cartesian.make2(
  granularities,
  TimeFrames.timeFrames,
)
  .map(([granularity, timeFrame]) =>
    getIsGranularityEnabledForTimeFrame(granularity, timeFrame)
      ? makeRecordState(granularity, timeFrame)
      : undefined,
  )
  .filter((v): v is RecordState => v !== undefined);

export const getIsBlockWithinReferenceMaxAge =
  (maxAge: number, referenceBlock: { minedAt: Date }) =>
  (targetBlock: { minedAt: Date }) =>
    millisecondsBetweenAbs(referenceBlock.minedAt, targetBlock.minedAt) <=
    maxAge;

export const sumFeeBlocks = (
  denomination: Denomination,
  blocks: FeeBlock[],
): bigint =>
  blocks.reduce(
    (sum, block) =>
      sum + (denomination === "eth" ? block.feesEth : block.feesUsd),
    0n,
  );

export const feeBlockFromBlock = (block: FeeBlockRow): FeeBlock => {
  const feesWei = block.gasUsed * block.baseFeePerGas;
  return {
    number: block.number,
    minedAt: block.minedAt,
    feesEth: feesWei,
    feesUsd: (feesWei * block.ethPriceCents) / 10n ** 18n,
  };
};

export const topSumOrderingMap: Record<
  Denomination,
  Record<Sorting, Ord<Sum>>
> = {
  eth: {
    min: OrdM.fromCompare((first, second) =>
      first.sumEth > second.sumEth ? -1 : first.sumEth < second.sumEth ? 1 : 0,
    ),
    max: OrdM.fromCompare((first, second) =>
      first.sumEth < second.sumEth ? -1 : first.sumEth > second.sumEth ? 1 : 0,
    ),
  },
  usd: {
    min: OrdM.fromCompare((first, second) =>
      first.sumUsd > second.sumUsd ? -1 : first.sumUsd < second.sumUsd ? 1 : 0,
    ),
    max: OrdM.fromCompare((first, second) =>
      first.sumUsd < second.sumUsd ? -1 : first.sumUsd > second.sumUsd ? 1 : 0,
    ),
  },
};

export type Sum = {
  end: number;
  endMinedAt: Date;
  start: number;
  startMinedAt: Date;
  sumEth: bigint;
  sumUsd: bigint;
};

export type RecordState = {
  granularity: Granularity;
  feeBlockRollbackBuffer: Denque<FeeBlock>;
  // Used to calculate the next sum without refeteching all blocks.
  feeBlocks: Denque<FeeBlock>;
  // Used to rollback to the previous sum we were calculating with.
  // Used to drop top sums that fall outside the time frame?
  sums: Denque<Sum>;
  sumsRollbackBuffer: Denque<Sum>;
  timeFrame: TimeFrame;
  topSumsMap: Record<Denomination, Record<Sorting, Sum[]>>;
};

const getBlockFees = (
  denomination: Denomination,
  block: FeeBlockRow,
): bigint => {
  const feesWei = block.gasUsed * block.baseFeePerGas;
  return denomination === "eth"
    ? feesWei
    : (feesWei * block.ethPriceCents) / 10n ** 18n;
};

export const makeNewSum = (
  lastSum: Sum | undefined,
  block: FeeBlockRow,
): Sum => {
  if (lastSum === undefined) {
    return {
      end: block.number,
      endMinedAt: block.minedAt,
      start: block.number,
      startMinedAt: block.minedAt,
      sumEth: getBlockFees("eth", block),
      sumUsd: getBlockFees("usd", block),
    };
  }

  return {
    ...lastSum,
    end: lastSum.end + 1,
    endMinedAt: block.minedAt,
    sumEth: lastSum.sumEth + getBlockFees("eth", block),
    sumUsd: lastSum.sumUsd + getBlockFees("usd", block),
  };
};

const getIsBlockWithinMaxAgeWithMaxAge =
  (maxAge: number, referenceBlock: { minedAt: Date }) =>
  (block: FeeBlock): boolean =>
    DateFnsAlt.millisecondsBetweenAbs(referenceBlock.minedAt, block.minedAt) <=
    maxAge;

const getIsSumWithinMaxAgeWithMaxAge =
  (maxAge: number, referenceBlock: { minedAt: Date }) =>
  (sum: Sum): boolean =>
    DateFnsAlt.millisecondsBetweenAbs(referenceBlock.minedAt, sum.endMinedAt) <=
    maxAge;

export const recordsCount = 10;
export const rollbackBufferMillis = Duration.millisFromMinutes(10);

export const getTopSumsMaxCount = (granularity: Granularity): number => {
  const minBlockDuration = Duration.millisFromSeconds(12);
  const granularityMultiplier =
    granularity === "block"
      ? minBlockDuration
      : granularityMillisMap[granularity];

  return (
    (recordsCount * granularityMultiplier + rollbackBufferMillis) /
    minBlockDuration
  );
};

export const mergeCandidate2 = (
  denomination: Denomination,
  sorting: Sorting,
  topSums: Sum[],
  sum: Sum,
): Sum[] => {
  const sumGreaterThan = OrdM.gt(topSumOrderingMap[denomination][sorting]);
  const sumEqualTo = topSumOrderingMap[denomination][sorting].equals;

  // Find the index the candidate would rank at.
  let i = topSums.length;
  for (; i >= 1; i--) {
    const incumbent = topSums[i - 1];
    if (sumGreaterThan(incumbent, sum)) {
      break;
    }
    if (sumEqualTo(incumbent, sum)) {
      break;
    }
  }

  return __.insertAt(i, sum, topSums);
};

export const getIsOverlapping = (records: Sum[], sum: Sum): boolean => {
  for (const record of records) {
    // Sum overlaps with record on right hand side.
    if (sum.start <= record.start && sum.end >= record.start) {
      return true;
    }

    // Sum overlaps with record on left hand side.
    if (sum.start <= record.end && sum.end >= record.end) {
      return true;
    }
  }

  return false;
};

export const getRecordsFromTopSums = (topSums: Sum[]): Sum[] => {
  const records: Sum[] = [];

  for (const topSum of topSums) {
    if (getIsOverlapping(records, topSum)) {
      continue;
    }

    records.push(topSum);

    if (records.length === recordsCount) {
      return records;
    }
  }

  throw new Error(
    "hit end of top sums without finding desired number of records",
  );
};

// NOTE: to review
// Figures out what blocks to add back from the fee block rollback buffer.
const getBlocksToRestoreCount = (
  recordState: RecordState,
  granularity: Granularity,
): number => {
  // For the `block` granularity we don't rollback time-wise, we simply roll back one block.
  if (granularity === "block") {
    return 1;
  }

  // To rollback all fee blocks we dropped for the last added block, we need to add back all fee blocks that were still within our granularity one block ago.
  const newTip = recordState.feeBlocks.peekBack();
  if (newTip === undefined) {
    throw new Error(
      `tried to rollback burn records with ${granularity} granularity, but no fee block left to reference to determine which blocks in the rollback buffer are inside the current granularity`,
    );
  }
  const getIsBlockWithinGranularity = getIsBlockWithinMaxAgeWithMaxAge(
    granularityMillisMap[granularity],
    newTip!,
  );
  let i = 0;
  let newestFeeBlock = recordState.feeBlockRollbackBuffer.peekBack();
  while (
    newestFeeBlock !== undefined &&
    getIsBlockWithinGranularity(newestFeeBlock)
  ) {
    i++;
    newestFeeBlock = recordState.feeBlockRollbackBuffer.peekAt(i);
  }
  return i;
};

export const getMatchingSumIndexFromRight = (
  denomination: Denomination,
  sorting: Sorting,
  topSums: Sum[],
  sum: Sum,
): number | undefined => {
  if (topSums.length === 0) {
    return undefined;
  }

  const lt = OrdM.lt(topSumOrderingMap[denomination][sorting]);
  for (let i = topSums.length - 1; i >= 0; i--) {
    const candidate = topSums[i];

    // Found it!
    if (candidate.end === sum.end) {
      return i;
    }

    // Not going to find it anymore.
    if (lt(sum, candidate)) {
      return undefined;
    }
  }

  throw new Error(
    "no matching top sum found but sum is greater than all top sums",
  );
};

export const addBlockToState = (
  recordState: RecordState,
  block: FeeBlockRow,
): RecordState => {
  const { timeFrame, granularity } = recordState;

  const feeBlockToAdd = feeBlockFromBlock(block);

  const lastSum = recordState.sums.peekBack();

  const newSum = makeNewSum(lastSum, block);

  recordState.feeBlocks.push(feeBlockToAdd);

  const getIsBlockWithinGranularity =
    granularity === "block"
      ? (feeBlock: FeeBlock) => feeBlock.number === feeBlockToAdd.number
      : getIsBlockWithinMaxAgeWithMaxAge(
          granularityMillisMap[granularity],
          block,
        );

  // Remove expired fees from the current sum, and remember removed blocks.
  // Keep the live blocks for the current sum.
  let oldestFeeBlock = recordState.feeBlocks.peekFront();
  while (
    oldestFeeBlock !== undefined &&
    !getIsBlockWithinGranularity(oldestFeeBlock)
  ) {
    const expiredBlock = recordState.feeBlocks.shift()!;
    recordState.feeBlockRollbackBuffer.push(expiredBlock);
    newSum.start = newSum.start + 1;
    newSum.startMinedAt = recordState.feeBlocks.peekFront()!.minedAt;
    newSum.sumEth = newSum.sumEth - expiredBlock.feesEth;
    newSum.sumUsd = newSum.sumUsd - expiredBlock.feesUsd;
    oldestFeeBlock = recordState.feeBlocks.peekFront();
  }

  // Update sums.
  recordState.sums.push(newSum);

  // Merge the sum into top sums.
  const dimensions = Cartesian.make2(denominations, sortings);
  for (const [denomination, sorting] of dimensions) {
    const mergeResult = mergeCandidate2(
      denomination,
      sorting,
      // granularity,
      recordState.topSumsMap[denomination][sorting],
      newSum,
    );

    recordState.topSumsMap[denomination][sorting] = mergeResult;
  }

  // Drop sums outside of time frame.
  const nowSubRollback = DateFns.subMilliseconds(
    new Date(),
    rollbackBufferMillis,
  );
  const getIsSumWithinTimeFrame =
    timeFrame === "all"
      ? (sum: Sum) => DateFns.isAfter(sum.startMinedAt, nowSubRollback)
      : getIsSumWithinMaxAgeWithMaxAge(
          TimeFrames.timeFrameMillisMap[timeFrame],
          feeBlockToAdd,
        );

  let oldestSum = recordState.sums.peekFront();
  const expiredSumSet = new Set();
  // Drop expired sums from sums.
  while (oldestSum !== undefined && !getIsSumWithinTimeFrame(oldestSum)) {
    const expiredSum = recordState.sums.shift()!;
    expiredSumSet.add(expiredSum.end);
    recordState.sumsRollbackBuffer.push(expiredSum);
    oldestSum = recordState.sums.peekFront();
  }

  // Drop expired sums from top sums.
  for (const [denomination, sorting] of dimensions) {
    const topSums = recordState.topSumsMap[denomination][sorting];
    recordState.topSumsMap[denomination][sorting] = topSums.filter(
      (sum) => !expiredSumSet.has(sum.end),
    );
  }

  const getIsBlockWithinRollbackBuffer = getIsBlockWithinMaxAgeWithMaxAge(
    rollbackBufferMillis,
    block,
  );

  let oldestBufferFeeBlock = recordState.feeBlockRollbackBuffer.peekFront();
  while (
    oldestBufferFeeBlock !== undefined &&
    !getIsBlockWithinRollbackBuffer(oldestBufferFeeBlock)
  ) {
    recordState.feeBlockRollbackBuffer.shift();
    oldestBufferFeeBlock = recordState.feeBlockRollbackBuffer.peekFront();
  }

  const getIsSumWithinRollbackBuffer = getIsSumWithinMaxAgeWithMaxAge(
    rollbackBufferMillis,
    feeBlockToAdd,
  );

  let oldestBufferSum = recordState.sumsRollbackBuffer.peekFront();
  while (
    oldestBufferSum !== undefined &&
    !getIsSumWithinRollbackBuffer(oldestBufferSum)
  ) {
    recordState.sumsRollbackBuffer.shift();
    oldestBufferSum = recordState.sumsRollbackBuffer.peekFront();
  }

  return recordState;
};

export const rollbackBlock = (
  recordState: RecordState,
  block?: BlockDb,
): RecordState => {
  const { granularity, timeFrame } = recordState;
  const lastBlock = recordState.feeBlocks.peekBack();

  Log.debug(
    `burn records ${timeFrame} rollback, ${lastBlock?.number}, ${granularity}`,
  );

  if (block && lastBlock?.number !== block.number) {
    throw new Error(
      `tried to roll back block ${block.number} but tip is: ${lastBlock}`,
    );
  }

  // Drop the most recently added fee block.
  recordState.feeBlocks.pop();

  // Restore fee blocks from rollback buffer that are within the interval `lastFeeBlock - granularity`.
  // Drop blocks we use from the rollback buffer.
  const blocksToRestoreCount = getBlocksToRestoreCount(
    recordState,
    granularity,
  );
  for (let i = 0; i < blocksToRestoreCount; i++) {
    const feeBlockToRestore = recordState.feeBlockRollbackBuffer.pop()!;
    // Add them back onto fee blocks.
    recordState.feeBlocks.unshift(feeBlockToRestore);
  }

  // Remove the last sum we calculated from top sums.
  const lastSum = recordState.sums.peekBack();

  if (lastSum === undefined) {
    throw new Error(
      "tried to rollback burn records, found no last sum to drop from topSums",
    );
  }

  const dimensions = Cartesian.make2(denominations, sortings);
  for (const [denomination, sorting] of dimensions) {
    const lastSumTopIndex = getMatchingSumIndexFromRight(
      denomination,
      sorting,
      recordState.topSumsMap[denomination][sorting],
      lastSum,
    );

    if (lastSumTopIndex !== undefined) {
      _.pullAt(recordState.topSumsMap[denomination][sorting], lastSumTopIndex);
    }
  }

  // Drop the last sum we calculated
  recordState.sums.pop();

  const getIsSumWithinTimeFrame =
    timeFrame === "all"
      ? () => true
      : getIsSumWithinMaxAgeWithMaxAge(
          TimeFrames.timeFrameMillisMap[timeFrame],
          recordState.feeBlocks.peekBack()!,
        );

  let newestBufferSum = recordState.sumsRollbackBuffer.peekBack();
  // const sumsToRestore = _.takeRightWhile(
  //   recordState.sumsRollbackBuffer,
  //   getIsSumWithinTimeFrame,
  // );

  while (
    newestBufferSum !== undefined &&
    getIsSumWithinTimeFrame(newestBufferSum)
  ) {
    // Drop sums we restore
    const sumToRestore = recordState.sumsRollbackBuffer.pop()!;
    // Restore sums
    recordState.sums.unshift(sumToRestore);
    // Restore top sums
    for (const [denomination, sorting] of dimensions) {
      recordState.topSumsMap[denomination][sorting] = mergeCandidate2(
        denomination,
        sorting,
        recordState.topSumsMap[denomination][sorting],
        sumToRestore,
      );
    }
    newestBufferSum = recordState.sumsRollbackBuffer.peekBack();
  }

  return recordState;
};

export const getRecordStatesByTimeFrame = (
  recordStates: RecordState[],
  timeFrame: TimeFrame,
): RecordState[] =>
  recordStates.filter((recordState) => recordState.timeFrame === timeFrame)!;

export type BurnRecordsT = {
  denomination: Denomination;
  granularity: Granularity;
  sorting: Sorting;
  records: Sum[];
}[];

export const getRecords = (): BurnRecordsT => {
  return recordStates.flatMap((recordState) =>
    Cartesian.make2(denominations, sortings).map(([denomination, sorting]) => ({
      denomination,
      sorting,
      granularity: recordState.granularity,
      records: getRecordsFromTopSums(
        recordState.topSumsMap[denomination][sorting],
      ),
    })),
  );
};
