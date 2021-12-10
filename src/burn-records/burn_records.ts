import * as DateFns from "date-fns";
import _ from "lodash";
import { BlockDb, FeeBlockRow } from "../blocks/blocks.js";
import * as Cartesian from "../cartesian.js";
import * as DateFnsAlt from "../date_fns_alt.js";
import { millisecondsBetweenAbs } from "../date_fns_alt.js";
import { Denomination, denominations } from "../denominations.js";
import * as Duration from "../duration.js";
import { millisFromHours, millisFromMinutes } from "../duration.js";
import { A, Ord, OrdM, pipe } from "../fp.js";
import * as __ from "../lodash_alt.js";
import * as Log from "../log.js";
import * as TimeFrame from "../time_frame.js";

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
  fees: bigint;
};

export type FeeSetSum = {
  sum: bigint;
  blocks: FeeBlock[];
};

export type FeeRecordMap = Record<
  Granularity,
  Record<Sorting, Record<Denomination, FeeRecord[]>>
>;

export type FeeSetMap = Record<Granularity, Record<Denomination, FeeSetSum>>;

export const makeRecordMap = (): FeeRecordMap => {
  return pipe(
    Cartesian.make3(denominations, granularities, sortings),
    A.reduce(
      {} as FeeRecordMap,
      (map, [denomination, granularity, sorting]) => {
        map[granularity] = map[granularity] ?? {};
        map[granularity][sorting] = map[granularity][sorting] ?? {};
        map[granularity][sorting][denomination] = [];
        return map;
      },
    ),
  );
};

export const makeFeeSetMap = (): FeeSetMap => {
  const map = {} as FeeSetMap;
  for (const granularity of granularities) {
    map[granularity] = {} as Record<Denomination, FeeSetSum>;
    for (const denomination of denominations) {
      map[granularity][denomination] = { sum: 0n, blocks: [] };
    }
  }
  return map;
};

export const getIsBlockWithinReferenceMaxAge =
  (maxAge: number, referenceBlock: { minedAt: Date }) =>
  (targetBlock: { minedAt: Date }) =>
    millisecondsBetweenAbs(referenceBlock.minedAt, targetBlock.minedAt) <=
    maxAge;

export const sumFeeBlocks = (blocks: FeeBlock[]): bigint =>
  blocks.reduce((sum, block) => sum + block.fees, 0n);

export const feeBlockFromBlock = (
  denomination: Denomination,
  block: FeeBlockRow,
): FeeBlock => {
  const feesWei = block.gasUsed * block.baseFeePerGas;
  return {
    number: block.number,
    minedAt: block.minedAt,
    fees:
      denomination === "eth"
        ? feesWei
        : (feesWei * block.ethPriceCents) / 10n ** 18n,
  };
};

const addBlockToFeeSet = (
  feeSetSum: FeeSetSum,
  denomination: Denomination,
  granularity: Granularity,
  blockToAdd: FeeBlockRow,
): FeeSetSum => {
  if (granularity === "block") {
    throw new Error("cant handle block");
  }

  const feeBlockToAdd = feeBlockFromBlock(denomination, blockToAdd);

  const getIsBlockWithinMaxAge = getIsBlockWithinReferenceMaxAge(
    granularityMillisMap[granularity],
    feeBlockToAdd,
  );

  const { left: blocksToRemove, right: blocksToKeep } = pipe(
    feeSetSum.blocks,
    // To keep things fast we remember the blocks included for a given denomination and granularity, and their fee sum. Depending on the time that passed since the last block, a number of blocks now fall outside the interval of the block's timestamp minus the duration of the granularity. We subtract the fees from those blocks from the running total and drop them from the included block set. We add the newly received block.
    A.partition(getIsBlockWithinMaxAge),
  );

  const newFeeSetTotal = {
    sum: feeSetSum.sum - sumFeeBlocks(blocksToRemove) + feeBlockToAdd.fees,
    blocks: [...blocksToKeep, feeBlockToAdd],
  };

  return newFeeSetTotal;
};

export const orderingMap: Record<Sorting, Ord<FeeRecord>> = {
  min: OrdM.fromCompare((first, second) =>
    first.feeSum < second.feeSum ? -1 : first.feeSum === second.feeSum ? 0 : 1,
  ),
  max: OrdM.fromCompare((first, second) =>
    first.feeSum > second.feeSum ? -1 : first.feeSum === second.feeSum ? 0 : 1,
  ),
};

export const topSumOrderingMap: Record<Sorting, Ord<Sum>> = {
  min: OrdM.fromCompare((first, second) =>
    first.sum > second.sum ? -1 : first.sum < second.sum ? 1 : 0,
  ),
  max: OrdM.fromCompare((first, second) =>
    first.sum < second.sum ? -1 : first.sum > second.sum ? 1 : 0,
  ),
};

const getIsNewRecord = (
  ordering: Ord<FeeRecord>,
  candidate: FeeRecord,
  incumbent: FeeRecord,
) => OrdM.gt(ordering)(candidate, incumbent);

type MergeResult = { isNewRecordSet: boolean; feeRecords: FeeRecord[] };

// TODO: filter out disjoint records, taking the best one.
export const mergeCandidate = (
  ordering: Ord<FeeRecord>,
  feeRecords: FeeRecord[],
  candidateRecord: FeeRecord,
): MergeResult => {
  const worstRecord = feeRecords[104];

  if (worstRecord === undefined) {
    // We have less than 105 records, any candidate is a record.
    return {
      isNewRecordSet: true,
      feeRecords: pipe(feeRecords, A.append(candidateRecord), A.sort(ordering)),
    };
  }

  if (getIsNewRecord(ordering, candidateRecord, worstRecord)) {
    return {
      isNewRecordSet: true,
      feeRecords: pipe(
        feeRecords,
        A.append(candidateRecord),
        A.sort(ordering),
        A.takeLeft(105),
      ),
    };
  }

  return { isNewRecordSet: false, feeRecords };
};

const addBlockToRecords = (
  feeRecords: FeeRecord[],
  sorting: Sorting,
  feeSetSum: FeeSetSum,
): { isNewRecordSet: boolean; feeRecords: FeeRecord[] } => {
  const ordering = orderingMap[sorting];

  const candidate: FeeRecord = {
    feeSum: feeSetSum.sum,
    firstBlock: feeSetSum.blocks[0].number,
    lastBlock: feeSetSum.blocks[feeSetSum.blocks.length - 1].number,
  };

  return mergeCandidate(ordering, feeRecords, candidate);
};

export type OnNewRecordSet = (
  denomination: Denomination,
  granularity: Granularity,
  sorting: Sorting,
  feeRecords: FeeRecord[],
) => Promise<void>;

export const addBlock = async (
  onNewRecordSet: OnNewRecordSet,
  feeSetMap: FeeSetMap,
  feeRecordMap: FeeRecordMap,
  blockToAdd: FeeBlockRow,
): Promise<void> => {
  const tasks = Cartesian.make2(denominations, granularities).map(
    async ([denomination, granularity]) => {
      const feeSetSum = feeSetMap[granularity][denomination];
      const newFeeSetSum = addBlockToFeeSet(
        feeSetSum,
        denomination,
        granularity,
        blockToAdd,
      );
      feeSetMap[granularity][denomination] = newFeeSetSum;

      for (const sorting of sortings) {
        const feeRecords = feeRecordMap[granularity][sorting][denomination];
        const { isNewRecordSet, feeRecords: newFeeRecords } = addBlockToRecords(
          feeRecords,
          sorting,
          newFeeSetSum,
        );

        // As storing fee records is expensive (DB write), and infrequent, we only do so when a new record is set.
        if (isNewRecordSet) {
          feeRecordMap[granularity][sorting][denomination] = newFeeRecords;
          await onNewRecordSet(
            denomination,
            granularity,
            sorting,
            newFeeRecords,
          );
        }
      }
    },
  );

  await Promise.all(tasks);
};

export const rollbackFeeSet = (feeSetSum: FeeSetSum): FeeSetSum => {
  // When the active rollback includes more blocks than the length of the fee set, e.g. rolling back more than one block, with 'block' granularity containing only a single block, the fee set will be empty at this point.
  const blockToRemove = _.last(feeSetSum.blocks);

  return blockToRemove === undefined
    ? { blocks: [], sum: 0n }
    : {
        blocks: feeSetSum.blocks.slice(0, -1),
        sum: feeSetSum.sum - blockToRemove.fees,
      };
};

export const rollbackFeeRecords = (
  feeRecords: FeeRecord[],
  block: BlockDb,
): MergeResult => {
  const newFeeRecords = feeRecords.filter(
    (feeRecord) => feeRecord.lastBlock !== block.number,
  );

  return feeRecords.length === newFeeRecords.length
    ? { isNewRecordSet: false, feeRecords }
    : { isNewRecordSet: true, feeRecords: newFeeRecords };
};

export const rollbackLastBlock = async (
  onNewRecordSet: OnNewRecordSet,
  feeSetMap: FeeSetMap,
  feeRecordMap: FeeRecordMap,
  block: BlockDb,
): Promise<void> => {
  for (const [denomination, granularity] of Cartesian.make2(
    denominations,
    granularities,
  )) {
    const feeSetSum = feeSetMap[granularity][denomination];
    const newFeeSetSum = rollbackFeeSet(feeSetSum);
    feeSetMap[granularity][denomination] = newFeeSetSum;

    for (const sorting of sortings) {
      const feeRecords = feeRecordMap[granularity][sorting][denomination];
      const { isNewRecordSet, feeRecords: newFeeRecords } = rollbackFeeRecords(
        feeRecords,
        block,
      );

      if (isNewRecordSet) {
        await onNewRecordSet(denomination, granularity, sorting, newFeeRecords);
      }
    }
  }
};

export type Sum = {
  end: number;
  endMinedAt: Date;
  start: number;
  startMinedAt: Date;
  sum: bigint;
};

export type RecordState = {
  denomination: Denomination;
  // Used to calculate the next sum without refeteching all blocks.
  feeBlocks: FeeBlock[];
  feeBlockRollbackBuffer: FeeBlock[];
  sumsRollbackBuffer: Sum[];
  // Used to drop top sums that fall outside the time frame.
  sums: Sum[];
  topSums: Sum[];
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
  denomination: Denomination,
  lastSum: Sum | undefined,
  block: FeeBlockRow,
): Sum => {
  if (lastSum === undefined) {
    return {
      end: block.number,
      endMinedAt: block.minedAt,
      start: block.number,
      startMinedAt: block.minedAt,
      sum: getBlockFees("eth", block),
    };
  }

  return {
    ...lastSum,
    end: lastSum.end + 1,
    endMinedAt: block.minedAt,
    sum: lastSum.sum + getBlockFees(denomination, block),
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
  sorting: Sorting,
  granularity: Granularity,
  topSums: Sum[],
  sum: Sum,
): {
  topSums: Sum[];
  isNewRecordSet: boolean;
} => {
  const sumGreaterThan = OrdM.gt(topSumOrderingMap[sorting]);
  const topSumsMaxCount = getTopSumsMaxCount(granularity);

  // Find the index the candidate would rank at.
  let i = topSums.length;
  for (; i >= 1; i--) {
    const incumbent = topSums[i - 1];
    if (sumGreaterThan(incumbent, sum)) {
      break;
    }
    if (incumbent.sum === sum.sum) {
      break;
    }
  }

  // Don't add candidates worse than our worst when topSums is at limit.
  if (i === topSums.length && topSums.length >= topSumsMaxCount) {
    return {
      topSums,
      isNewRecordSet: false,
    };
  }

  const mergedSums = __.insertAt(i, sum, topSums);
  const newTopSums = _.dropRight(
    mergedSums,
    mergedSums.length - topSumsMaxCount,
  );

  // Otherwise, insert at the correct index.
  return {
    topSums: newTopSums,
    isNewRecordSet: true,
  };
};

export const addBlockToState = (
  recordState: RecordState,
  block: FeeBlockRow,
  granularity: Granularity,
  denomination: Denomination,
  sorting: Sorting,
  timeFrame: TimeFrame.TimeFrame,
): RecordState => {
  Log.debug(
    `burn records, ${timeFrame}, new tip: ${block.number}, ${granularity}, ${denomination}, ${sorting}`,
  );

  const feeBlockToAdd = feeBlockFromBlock(denomination, block);

  const lastSum = _.last(recordState.sums);

  const newSum = makeNewSum(denomination, lastSum, block);

  recordState.feeBlocks.push(feeBlockToAdd);

  const getIsBlockWithinGranularity =
    granularity === "block"
      ? (feeBlock: FeeBlock) => feeBlock.number === feeBlockToAdd.number
      : getIsBlockWithinMaxAgeWithMaxAge(
          granularityMillisMap[granularity],
          block,
        );

  // Remove expired fees from the current sum, and remember removed blocks.
  const expiredBlocks = _.takeWhile(
    recordState.feeBlocks,
    (block) => !getIsBlockWithinGranularity(block),
  );

  for (const expiredBlock of expiredBlocks) {
    recordState.feeBlockRollbackBuffer.push(expiredBlock);
    newSum.start = newSum.start + 1;
    newSum.startMinedAt = _.head(recordState.feeBlocks)!.minedAt;
    newSum.sum = newSum.sum - expiredBlock.fees;
  }

  // Keep the live blocks for the current sum.
  const liveBlocks = recordState.feeBlocks.slice(expiredBlocks.length);
  recordState.feeBlocks = liveBlocks;

  // Update sums.
  recordState.sums.push(newSum);

  // Merge the sum into top sums.
  const mergeResult = mergeCandidate2(
    sorting,
    granularity,
    recordState.topSums,
    newSum,
  );

  if (mergeResult.isNewRecordSet) {
    // TODO: call hook to write to db?
    recordState.topSums = mergeResult.topSums;
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
          TimeFrame.timeFrameMillisMap[timeFrame],
          feeBlockToAdd,
        );

  const expiredSums = _.takeWhile(
    recordState.sums,
    (sum) => !getIsSumWithinTimeFrame(sum),
  );

  for (const expiredSum of expiredSums) {
    recordState.sumsRollbackBuffer.push(expiredSum);
  }

  // Store sums that are within the time frame.
  const liveSums = recordState.sums.slice(expiredSums.length);
  recordState.sums = liveSums;

  const getIsBlockWithinRollbackBuffer = getIsBlockWithinMaxAgeWithMaxAge(
    rollbackBufferMillis,
    block,
  );

  recordState.feeBlockRollbackBuffer = _.dropWhile(
    recordState.feeBlockRollbackBuffer,
    (block) => !getIsBlockWithinRollbackBuffer(block),
  );

  const getIsSumWithinRollbackBuffer = getIsSumWithinMaxAgeWithMaxAge(
    rollbackBufferMillis,
    feeBlockToAdd,
  );

  recordState.sumsRollbackBuffer = _.dropWhile(
    recordState.sumsRollbackBuffer,
    (sum) => !getIsSumWithinRollbackBuffer(sum),
  );

  return recordState;
};

// NOTE: to review
// Figures out what blocks to add back from the fee block rollback buffer.
const getFeeBlocksForRollback = (
  recordState: RecordState,
  granularity: Granularity,
): FeeBlock[] => {
  // For the `block` granularity we don't rollback time-wise, we simply roll back one block.
  if (granularity === "block") {
    const lastExpiredFeeBlock = _.last(recordState.feeBlockRollbackBuffer);
    if (lastExpiredFeeBlock === undefined) {
      Log.warn(
        "tried to rollback burn records with block granularity but exhausted the rollback buffer",
      );
      return [];
    }
    return [lastExpiredFeeBlock];
  }

  // To rollback all fee blocks we dropped for the last added block, we need to add back all fee blocks that were still within our granularity one block ago.
  const tipBeforeRestore = _.last(recordState.feeBlocks);
  if (tipBeforeRestore === undefined) {
    throw new Error(
      `tried to rollback burn records with ${granularity} granularity, but no fee block left to reference to determine which blocks in the rollback buffer are inside the current granularity`,
    );
  }
  const getIsBlockWithinGranularity = getIsBlockWithinMaxAgeWithMaxAge(
    granularityMillisMap[granularity],
    tipBeforeRestore!,
  );
  const feeBlocksToRestore = _.takeRightWhile(
    recordState.feeBlockRollbackBuffer,
    getIsBlockWithinGranularity,
  );

  return feeBlocksToRestore;
};

export const getMatchingSumIndexFromRight = (
  sorting: Sorting,
  topSums: Sum[],
  sum: Sum,
): number | undefined => {
  if (topSums.length === 0) {
    return undefined;
  }

  const lt = OrdM.lt(topSumOrderingMap[sorting]);
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

export const rollbackBlock = (
  recordState: RecordState,
  granularity: Granularity,
  timeFrame: TimeFrame.TimeFrame,
  sorting: Sorting,
): RecordState => {
  const lastBlock = _.last(recordState.sums)?.end;
  Log.debug(`burn records ${timeFrame} rollback, ${lastBlock}, ${granularity}`);

  // Drop the most recently added fee block.
  recordState.feeBlocks = recordState.feeBlocks.slice(0, -1);

  // Restore fee blocks from rollback buffer that are within the interval `lastFeeBlock - granularity`.
  const feeBlocksToRestore = getFeeBlocksForRollback(recordState, granularity);
  // Drop blocks we use from the rollback buffer.
  recordState.feeBlockRollbackBuffer = _.dropRight(
    recordState.feeBlockRollbackBuffer,
    feeBlocksToRestore.length,
  );
  // Add them back onto fee blocks.
  recordState.feeBlocks = [...feeBlocksToRestore, ...recordState.feeBlocks];

  // Remove the last sum we calculated from top sums.
  const lastSum = _.last(recordState.sums);

  if (lastSum === undefined) {
    throw new Error(
      "tried to rollback burn records, found no last sum to drop from topSums",
    );
  }

  const lastSumTopIndex = getMatchingSumIndexFromRight(
    sorting,
    recordState.topSums,
    lastSum,
  );

  if (lastSumTopIndex !== undefined) {
    _.pullAt(recordState.topSums, lastSumTopIndex);
  }

  // Drop the last sum we calculated
  recordState.sums = recordState.sums.slice(0, -1);

  const getIsSumWithinTimeFrame =
    timeFrame === "all"
      ? () => true
      : getIsSumWithinMaxAgeWithMaxAge(
          TimeFrame.timeFrameMillisMap[timeFrame],
          _.last(recordState.feeBlocks)!,
        );
  const sumsToRestore = _.takeRightWhile(
    recordState.sumsRollbackBuffer,
    getIsSumWithinTimeFrame,
  );

  // Drop sums we restore
  recordState.sumsRollbackBuffer = _.dropRight(
    recordState.sumsRollbackBuffer,
    sumsToRestore.length,
  );

  recordState.sums = [...sumsToRestore, ...recordState.sums];

  return recordState;
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

export const getRecords = (recordState: RecordState): Sum[] => {
  const records: Sum[] = [];

  for (const topSum of recordState.topSums) {
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
