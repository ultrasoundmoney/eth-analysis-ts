import * as Blocks from "../blocks/blocks.js";
import * as Contracts from "../contracts/contracts.js";
import { sql, sqlTVoid } from "../db.js";
import { A, NEA, O, pipe, T, TEAlt } from "../fp.js";
import * as Log from "../log.js";
import * as Transactions from "../transactions.js";

const blockNumbers = [
  13490115, 13677569, 13767694, 13833009, 13856316, 13877679, 13888674,
  13955805, 13958561, 13965374, 13978866, 13980473, 13987749, 13995811,
  14017264, 14025193, 14031938,
];

for (const blockNumber of blockNumbers) {
  const block = await Blocks.getBlockSafe(blockNumber)();
  if (O.isNone(block)) {
    throw new Error(`failed to get block ${blockNumber}`);
  }
  const transactionReceipts = await pipe(
    Transactions.transactionReceiptsFromBlock(block.value),
    TEAlt.getOrThrow,
  )();
  const { other } = Transactions.segmentTransactions(transactionReceipts);
  const transactionCounts = Blocks.countTransactionsPerContract(other);

  await pipe(
    Object.keys(transactionCounts),
    NEA.fromArray,
    O.match(
      () => T.of(undefined),
      (addresses) => Contracts.storeContracts(addresses),
    ),
  )();

  await pipe(
    Array.from(transactionCounts.entries()),
    A.map(([address, count]) => ({
      contract_address: address,
      block_number: blockNumber,
      transaction_count: count,
    })),
    NEA.fromArray,
    O.match(
      () => T.of(undefined),
      (insertables) =>
        pipe(
          // Not all contracts are known?! Store all blocks and contract_base_fees again.
          Contracts.storeContracts(
            pipe(
              insertables,
              NEA.map((row) => row.contract_address),
            ),
          ),
          T.chain(
            () => sqlTVoid`
              INSERT INTO contract_base_fees
                ${sql(insertables)}
              ON CONFLICT (contract_address, block_number) DO UPDATE SET
                transaction_count = excluded.transaction_count
            `,
          ),
        ),
    ),
  )();
}

Log.info("done");
