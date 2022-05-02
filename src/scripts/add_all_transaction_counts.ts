import makeEta from "simple-eta";
import * as Blocks from "../blocks/blocks.js";
import * as Contracts from "../contracts/contracts.js";
import { sql, sqlTVoid } from "../db.js";
import { A, NEA, O, pipe, T, TOAlt } from "../fp.js";
import * as Log from "../log.js";
import * as Transactions from "../transactions.js";

const lastStoredBlock = await Blocks.getLastStoredBlock()();

const lastAddedBlock = await sql<{ value: number | null }[]>`
  SELECT "value" FROM key_value_store
  WHERE "key" = 'last-added-transaction-count-block'
`.then((rows) => rows[0]?.value);

const blocksToStore = Blocks.getBlockRange(
  lastAddedBlock ?? Blocks.londonHardForkBlockNumber,
  lastStoredBlock.number,
);

const storeLastAdded = (blockNumber: number) => sql`
  INSERT INTO key_value_store
    (key, value)
  VALUES
    ('last-added-transaction-count-block', ${sql.json(blockNumber)})
  ON CONFLICT (key) DO UPDATE SET
    value = excluded.value
`;

const eta = makeEta({
  max: blocksToStore.length,
});
let blocksDone = 0;

for (const blockNumber of blocksToStore) {
  const block = await Blocks.getBlockSafe(blockNumber)();
  if (O.isNone(block)) {
    throw new Error(`failed to get block ${blockNumber}`);
  }
  const transactionReceipts = await pipe(
    Transactions.getTransactionReceiptsSafe(block.value),
    TOAlt.getOrThrow(`transactions for ${blockNumber} came back null`),
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

  if (blockNumber % 100 === 0 && blocksDone !== 0) {
    Log.debug(
      `blocks done: ${blocksDone}, eta: ${eta.estimate().toFixed(0)}s left`,
    );
    await storeLastAdded(blockNumber);
  }

  blocksDone++;
  eta.report(blocksDone);
}
