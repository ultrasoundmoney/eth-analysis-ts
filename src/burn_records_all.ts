import * as DateFnsAlt from "./date_fns_alt.js";
import { A, O, Ord, pipe } from "./fp.js";

export type Granularity = "block" | "m5" | "h1" | "d1" | "d7";

export type FeeRecord = { number: number; feeSum: bigint };

export type FeeBlock = {
  number: number;
  minedAt: Date;
  fees: bigint;
};

export type Sorting = "min" | "max";

export type FeeRecordMap = Record<Granularity, FeeRecord[]>;
export type BlockMap = Record<Granularity, FeeBlock[]>;

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
  ord: Ord<FeeRecord>,
  feeRecords: FeeRecord[],
  candidateRecord: FeeRecord,
): FeeRecord[] => {
  return pipe(
    feeRecords,
    A.last,
    O.match(
      // We have no records yet, any candidate is a record.
      () => [candidateRecord],
      // Take the worst record, compare against the candidate, merge in the winner.
      (worstRecord) =>
        pipe(
          [worstRecord, candidateRecord],
          A.sort(ord),
          A.head,
          O.getOrElseW(() => {
            throw new Error(
              "merge candidate, missing worst record and candidate record",
            );
          }),
          (betterRecord) => [...feeRecords.slice(0, -1), betterRecord],
          A.sort(ord),
        ),
    ),
  );
};

export const sortingOrdMap: Record<Sorting, Ord<FeeRecord>> = {
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

export const addBlock = (
  feeRecords: FeeRecord[],
  inScopeBlocks: FeeBlock[],
  ord: Ord<FeeRecord>,
  feeBlock: FeeBlock,
) => {
  const nextInScopeBlocks = pipe(inScopeBlocks, A.append(feeBlock));

  const candidate = {
    feeSum: sumFeeBlocks(nextInScopeBlocks),
    number: feeBlock.number,
  };

  const nextFeeRecords = mergeCandidate(ord, feeRecords, candidate);

  return {
    feeRecords: nextFeeRecords,
    inScopeBlocks: nextInScopeBlocks,
  };
};
