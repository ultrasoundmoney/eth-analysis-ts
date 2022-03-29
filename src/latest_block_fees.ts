import { sqlT } from "./db.js";
import { A, pipe, T } from "./fp.js";

export type LatestBlock = {
  baseFeePerGas: number;
  fees: number;
  feesUsd: number;
  number: number;
};
export type LatestBlockFees = LatestBlock[];

type LatestBlockFeesRow = {
  number: number;
  baseFeeSum: number;
  baseFeeSumUsd: number;
  baseFeePerGas: number;
  minedAt: Date;
};

export const getLatestBlockFees = (
  blockNumber: number,
): T.Task<LatestBlockFees> =>
  pipe(
    sqlT<LatestBlockFeesRow[]>`
      SELECT
        number,
        base_fee_sum,
        (base_fee_sum * eth_price / 1e18) AS base_fee_sum_usd,
        base_fee_per_gas,
        mined_at
      FROM blocks
      WHERE number <= ${blockNumber}
      ORDER BY (number) DESC
      LIMIT 5
    `,
    T.map(
      A.map((row) => ({
        number: row.number,
        fees: row.baseFeeSum,
        feesUsd: row.baseFeeSumUsd,
        baseFeePerGas: Number(row.baseFeePerGas),
        minedAt: row.minedAt,
      })),
    ),
  );
