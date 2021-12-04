import { Denomination, denominations } from "../denominations.js";
import { A, O, Ord, OrdM, pipe } from "../fp.js";
import * as Cartesian from "../cartesian.js";
import { millisecondsBetweenAbs } from "../date_fns_alt.js";
import { millisFromHours, millisFromMinutes } from "../duration.js";
import { BlockDb, FeeBlockRow } from "../blocks/blocks.js";
import _ from "lodash";

export const granularities = ["block", "m5", "h1", "d1", "d7"] as const;
export type Granularity = typeof granularities[number];

export const granularityMillisMap: Record<Granularity, number> = {
  block: 0,
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
