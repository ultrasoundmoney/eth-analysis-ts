import { Row, RowList } from "postgres";
import { sql } from "./db";
import { eth } from "./web3";
import type { TransactionReceipt as TransactionReceiptWeb3 } from "web3-eth/types/index";
import * as Log from "./log";

// TODO: don't count failed transactions

const storeReceipt = (
  transactionReceipt: TransactionReceiptWeb3,
): Promise<RowList<Row[]>> =>
  sql`
    INSERT INTO transaction_receipts (hash, json)
    VALUES (
      ${transactionReceipt.transactionHash},
      ${sql.json(transactionReceipt)}
    )
    ON CONFLICT DO NOTHING`;

export const storeTransactionReceipts = (
  transactionReceipts: TransactionReceiptWeb3[],
): Promise<RowList<Row[]>> =>
  sql`
    INSERT INTO transaction_receipts (hash, json)
    VALUES ${sql(
      transactionReceipts.map(
        (transactionReceipt) =>
          `${transactionReceipt.transactionHash}, ${sql.json(
            transactionReceipt,
          )}`,
      ),
    )}
    ON CONFLICT DO NOTHING
`;

export const syncTransactionReceipts = async (
  transactionHashes: string[],
): Promise<void> => {
  for (const hash of transactionHashes) {
    const receiptWeb3 = await eth.getTransactionReceipt(hash);
    await storeReceipt(receiptWeb3);
  }
  Log.debug(`> fetched ${transactionHashes.length} transaction receipts`);
};

export type TransactionReceipt = {
  from: string;
  to: string | null;
  gasUsed: number;
  effectiveGasPrice: number;
};

export const getTransactionReceipts = (
  transactionHashes: string[],
): Promise<TransactionReceipt[]> =>
  sql<
    { effectiveGasPrice: string; from: string; to: string; gasUsed: number }[]
  >`
    SELECT
      hash,
      json -> 'from' AS from,
      json -> 'to' AS to,
      json -> 'gasUsed' AS gas_used,
      json -> 'effectiveGasPrice' as effective_gas_price
    FROM transaction_receipts
    WHERE hash = ANY (${sql.array(transactionHashes)})
  `.then((result) =>
    result.map((row) => ({
      ...row,
      effectiveGasPrice: Number.parseInt(row.effectiveGasPrice, 16),
    })),
  );
