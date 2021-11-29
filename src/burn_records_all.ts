import * as BaseFees from "./base_fees.js";
import * as Blocks from "./blocks.js";
import { BlockDb } from "./blocks.js";
import * as DateFnsAlt from "./date_fns_alt.js";
import * as Denominations from "./denominations.js";
import { Denomination } from "./denominations.js";
import * as Duration from "./duration.js";
import { A, NEA, O, Ord, pipe, T, TEAlt } from "./fp.js";

const granularities = ["block", "m5", "h1", "d1", "d7"] as const;
export type Granularity = typeof granularities[number];

export type FeeRecord = { number: number; feeSum: bigint };

export type FeeBlock = {
  number: number;
  minedAt: Date;
  fees: bigint;
};

const sortings = ["min", "max"] as const;
export type Sorting = typeof sortings[number];

export type RecordMap = Record<
  Granularity,
  Record<Sorting, Record<Denomination, FeeRecord[]>>
>;
export type InScopeBlockMap = Record<
  Granularity,
  Record<Denomination, FeeBlock[]>
>;

// Used to speed up calculating fee records for blocks by remember what blocks were in scope.
export const inScopeBlockMap: InScopeBlockMap = {
  block: { eth: [], usd: [] },
  h1: { eth: [], usd: [] },
  m5: { eth: [], usd: [] },
  d1: { eth: [], usd: [] },
  d7: { eth: [], usd: [] },
};

// Tracks fee records.
export const recordMap: RecordMap = {
  block: { min: { eth: [], usd: [] }, max: { eth: [], usd: [] } },
  h1: { min: { eth: [], usd: [] }, max: { eth: [], usd: [] } },
  m5: { min: { eth: [], usd: [] }, max: { eth: [], usd: [] } },
  d1: { min: { eth: [], usd: [] }, max: { eth: [], usd: [] } },
  d7: { min: { eth: [], usd: [] }, max: { eth: [], usd: [] } },
};

const sumFeeBlocks = (blocks: FeeBlock[]): bigint =>
  pipe(
    blocks,
    A.reduce(0n, (sum, block) => sum + block.fees),
  );

export const getIsBlockWithinReferenceMaxAge =
  (maxAge: number, referenceBlock: FeeBlock) => (targetBlock: FeeBlock) =>
    DateFnsAlt.millisecondsBetweenAbs(
      referenceBlock.minedAt,
      targetBlock.minedAt,
    ) <= maxAge;

export const mergeCandidate = (
  ordering: Ord<FeeRecord>,
  feeRecords: FeeRecord[],
  candidateRecord: FeeRecord,
): FeeRecord[] =>
  pipe(
    feeRecords,
    A.last,
    O.match(
      // We have no records yet, any candidate is a record.
      () => [candidateRecord],
      // Take the worst record and our candidate, sort them according to the desired order, take the first.
      (worstRecord) =>
        pipe(
          [worstRecord, candidateRecord] as NEA.NonEmptyArray<FeeRecord>,
          NEA.sort(ordering),
          NEA.head,
          (betterRecord) => [...feeRecords.slice(0, -1), betterRecord],
          A.sort(ordering),
        ),
    ),
  );

export const orderingMap: Record<Sorting, Ord<FeeRecord>> = {
  min: {
    equals: (x, y) => x.feeSum === y.feeSum,
    compare: (first, second) =>
      first.feeSum < second.feeSum
        ? -1
        : first.feeSum === second.feeSum
        ? 0
        : 1,
  },
  max: {
    equals: (x, y) => x.feeSum === y.feeSum,
    compare: (first, second) =>
      first.feeSum > second.feeSum
        ? -1
        : first.feeSum === second.feeSum
        ? 0
        : 1,
  },
};

export const expireOldBlocks = (
  maxAge: number,
  referenceDate: Date,
  inScopeBlocks: FeeBlock[],
): FeeBlock[] =>
  pipe(
    inScopeBlocks,
    A.filter(
      (block) =>
        DateFnsAlt.millisecondsBetweenAbs(referenceDate, block.minedAt) <=
        maxAge,
    ),
  );

const granularityMillisMap = {
  block: 0,
  m5: Duration.millisFromMinutes(5),
  h1: Duration.millisFromHours(1),
  d1: Duration.millisFromHours(24),
  d7: Duration.millisFromHours(24 * 7),
};

const feeBlockFromBlockEth = (block: BlockDb): FeeBlock => ({
  number: block.number,
  minedAt: block.minedAt,
  fees: BaseFees.calcBlockBaseFeeSumDb(block),
});

const feeBlockFromBlockUsd = (block: BlockDb): FeeBlock => ({
  number: block.number,
  minedAt: block.minedAt,
  // TODO: reconsider how to stay polymorphic, precise, and work with USD amounts.
  fees: pipe(
    BaseFees.calcBlockBaseFeeSumDb(block),
    // Eth price is float, we'd lose precision converting to bigint without scaling so use float8 from here on.
    (feesWei) => (Number(feesWei) * block.ethPrice) / 10 ** 18,
    // To stay polymorphic we convert to bigint, losing a lot of precision
    (feesUsd) => Math.round(feesUsd),
    BigInt,
  ),
});

// TODO: read from DB.
// Check analysis_progress
// Queue missing blocks
export const init = () => {
  return pipe(TEAlt.seqTParTE(Blocks.getLatestKnownBlockNumber()));
};

// TODO: read from DB.
const readFeeRecords = (
  denomination: Denomination,
  granularity: Granularity,
  sorting: Sorting,
): T.Task<FeeRecord[]> => T.of(recordMap[granularity][sorting][denomination]);

// TODO: write to DB.
const storeFeeRecords = (
  denomination: Denomination,
  granularity: Granularity,
  sorting: Sorting,
  feeRecords: FeeRecord[],
): T.Task<void> => {
  recordMap[granularity][sorting][denomination] = feeRecords;
  return T.of(undefined);
};

const storeInScopeBlocks = (
  denomination: Denomination,
  granularity: Granularity,
  inScopeBlocks: FeeBlock[],
) => {
  inScopeBlockMap[granularity][denomination] = inScopeBlocks;
  return undefined;
};

export const onNewBlock = async (blockToAdd: BlockDb) => {
  for (const denomination of Denominations.denominations) {
    for (const granularity of granularities) {
      for (const sorting of sortings) {
        const feeBlockToAdd =
          denomination === "eth"
            ? feeBlockFromBlockEth(blockToAdd)
            : feeBlockFromBlockUsd(blockToAdd);

        const getIsBlockWithinMaxAge = getIsBlockWithinReferenceMaxAge(
          granularityMillisMap[granularity],
          feeBlockToAdd,
        );

        const inScopeBlocks = pipe(
          inScopeBlockMap[granularity][denomination],
          // To keep things fast we remember what blocks were within a given granularity for the last block whoms fee sum we calculated. Depending on the time that passed since the last block, a number of blocks now fall outside the interval of the block's timestamp minus the duration of the granularity. We filter those blocks.
          A.filter(getIsBlockWithinMaxAge),
          // Finally we add the current block to the scope.
          A.append(feeBlockToAdd),
        );

        const feeRecords = await readFeeRecords(
          denomination,
          granularity,
          sorting,
        )();

        const ordering = orderingMap[sorting];

        const candidate = {
          feeSum: sumFeeBlocks(inScopeBlocks),
          number: feeBlockToAdd.number,
        };

        const newFeeRecords = mergeCandidate(ordering, feeRecords, candidate);

        await storeFeeRecords(
          denomination,
          granularity,
          sorting,
          newFeeRecords,
        )();

        storeInScopeBlocks(denomination, granularity, inScopeBlocks);
      }
    }
  }
};
