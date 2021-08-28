import { sql } from "./db.js";
import * as T from "fp-ts/lib/Task.js";
import { BlockLondon } from "./eth_node.js";

export type LatestBlock = { fees: number; number: number };
export type LatestBlockFees = LatestBlock[];

export const getLatestBlockFees = (
  block: BlockLondon,
): T.Task<LatestBlockFees> => {
  return () =>
    sql<{ number: number; baseFeeSum: number }[]>`
    SELECT number, base_fee_sum FROM blocks
    WHERE number <= ${block.number}
    ORDER BY (number) DESC
    LIMIT 7
  `.then((rows) =>
      rows.map((row) => ({ number: row.number, fees: row.baseFeeSum })),
    );
};
