import makeEta from "simple-eta";
import * as BaseFees from "../base_fees.js";
import * as Blocks from "../blocks/blocks.js";
import { sql, sqlTVoid } from "../db.js";
import { A, NEA, O, pipe, T, TEAlt } from "../fp.js";
import * as Log from "../log.js";
import * as Transactions from "../transactions.js";

const progressKey = "last-added-missing-contract-base-fee";

const lastStoredBlock = await Blocks.getLastStoredBlock()();

const lastAddedBlock = await sql<{ value: number | null }[]>`
  SELECT "value" FROM key_value_store
  WHERE "key" = ${progressKey}
`.then((rows) => rows[0]?.value);

const blocksToUpdate = Blocks.getBlockRange(
  lastAddedBlock ?? Blocks.londonHardForkBlockNumber,
  lastStoredBlock.number,
);

const storeLastAdded = (blockNumber: number) => sql`
  INSERT INTO key_value_store
    (key, value)
  VALUES
    (${progressKey}, ${sql.json(blockNumber)})
  ON CONFLICT (key) DO UPDATE SET
    value = excluded.value
`;

const eta = makeEta({
  max: blocksToUpdate.length,
});
let blocksDone = 0;

for (const blockNumber of blocksToUpdate) {
  const block = await Blocks.getBlockSafe(blockNumber)();
  if (O.isNone(block)) {
    throw new Error(`failed to get block ${blockNumber}`);
  }
  const transactionReceipts = await pipe(
    Transactions.transactionReceiptsFromBlock(block.value),
    TEAlt.getOrThrow,
  )();
  const transactionSegments =
    Transactions.segmentTransactions(transactionReceipts);
  const feeSegments = BaseFees.sumFeeSegments(block.value, transactionSegments);

  await pipe(
    Array.from(feeSegments.contractSumsEthBI.entries()),
    A.map(([address, fees]) => ({
      contract_address: address,
      block_number: blockNumber,
      base_fees_256: String(fees),
    })),
    NEA.fromArray,
    O.match(
      () => T.of(undefined),
      (insertables) => sqlTVoid`
        INSERT INTO contract_base_fees
          ${sql(insertables)}
        ON CONFLICT (contract_address, block_number) DO UPDATE SET
          base_fees_256 = excluded.base_fees_256
      `,
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

Log.info("done");
