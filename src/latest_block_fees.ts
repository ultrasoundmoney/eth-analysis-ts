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
  blobFeeSum: number;
  blobFeeSumUsd: number;
  blobBaseFee: number;
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
        blob_fee_sum,
        (blob_fee_sum * eth_price / 1e18) AS blob_fee_sum_usd,
        base_fee_per_gas,
        blob_base_fee,
        mined_at
      FROM blocks
      WHERE number <= ${blockNumber}
      ORDER BY (number) DESC
      LIMIT 20
    `,
    T.map(
      A.map((row) => {
        console.log("LatestBlockFeesRow", row);
        const parsed =  {
          number: row.number,
          fees: Number(row.baseFeeSum) + Number(row.blobFeeSum),
          feesUsd: row.baseFeeSumUsd + row.blobFeeSumUsd,
          blobFees: row.blobFeeSum != null ? Number(row.blobFeeSum) : null,
          blobFeesUsd: row.blobFeeSumUsd,
          baseFeePerGas: Number(row.baseFeePerGas),
          blobBaseFee: row.blobBaseFee != null ? Number(row.blobBaseFee) : null,
          minedAt: row.minedAt,
        };
          console.log("Parsed", parsed);
          return parsed;
      }),
    ),
  );
