import { Denomination, denominations } from "../denominations.js";
import { A, pipe } from "../fp.js";
import * as Cartesian from "../cartesian.js";

export const granularities = ["block", "m5", "h1", "d1", "d7"] as const;
export type Granularity = typeof granularities[number];

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
