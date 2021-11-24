import BigNumber from "bignumber.js";
import * as DateFnsAlt from "./date_fns_alt.js";
import { A, O, Ord, pipe } from "./fp.js";

export type Granularity = "block" | "m5" | "h1" | "d1" | "d7";

export type FeeRecord = { number: number; feeSum: BigNumber };

export type FeeBlock = {
  number: number;
  minedAt: Date;
  fees: BigNumber;
};

export type Sorting = "min" | "max";

export type FeeRecordMap = Record<Granularity, FeeRecord[]>;
export type BlockMap = Record<Granularity, FeeBlock[]>;

const sumFeeBlocks = (blocks: FeeBlock[]): BigNumber =>
  pipe(
    blocks,
    A.reduce(new BigNumber(0), (bn, block) => bn.plus(block.fees)),
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
    equals: (x, y) => x.feeSum.eq(y.feeSum),
    compare: (first, second) =>
      first.feeSum.lt(second.feeSum)
        ? -1
        : first.feeSum.eq(second.feeSum)
        ? 0
        : 1,
  },
  max: {
    equals: (x, y) => x.feeSum.eq(y.feeSum),
    compare: (first, second) =>
      first.feeSum.gt(second.feeSum)
        ? -1
        : first.feeSum.eq(second.feeSum)
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
