import makeEta from "simple-eta";
import * as Blocks from "./blocks/blocks.js";
import * as ContractBaseFees from "./contract_base_fees.js";
import { sql } from "./db.js";
import * as Duration from "./duration.js";
import * as EthPrices from "./eth-prices/index.js";
import { pipe, TEAlt, TOAlt } from "./fp.js";
import * as Log from "./log.js";
import * as Transactions from "./transactions.js";

// After this process has run, all calculations based on blocks or contract_base_fees will be wrong and need to be recalculated.

const lastStoredBlock = await Blocks.getLastStoredBlock()();

const lastReanalyzedBlock = await sql<{ value: number | null }[]>`
  SELECT "value" FROM key_value_store
  WHERE "key" = 'last-reanalyzed-block'
`.then((rows) => rows[0]?.value);

const blocksToStore = Blocks.getBlockRange(
  lastReanalyzedBlock ?? Blocks.londonHardForkBlockNumber,
  lastStoredBlock.number,
);

const eta = makeEta({
  max: blocksToStore.length,
});
let blocksDone = 0;

const storeLastReanalyzed = (blockNumber: number) => sql`
  INSERT INTO key_value_store
    (key, value)
  VALUES
    ('last-reanalyzed-block', ${sql.json(blockNumber)})
  ON CONFLICT (key) DO UPDATE SET
    value = excluded.value
  WHERE
    key_value_store.key = 'last-reanalyzed-block'
`;

for (const blockNumber of blocksToStore) {
  const block = await pipe(
    Blocks.getBlockSafe(blockNumber),
    TOAlt.getOrThrow(`while reanalyzing block ${blockNumber} came back null`),
  )();

  if (blockNumber % 100 === 0 && blocksDone !== 0) {
    eta.report(blocksDone);
    const hoursLeft = (eta.estimate() / 60 / 60).toFixed(0);
    Log.info(`blocks done: ${blocksDone}, eta: ${hoursLeft} hours left`);
    await storeLastReanalyzed(blockNumber);
  }

  if (
    process.env.ONLY_MISMATCH !== undefined &&
    process.env.ONLY_MISMATCH !== "false"
  ) {
    const isBlockKnown = await Blocks.getBlockHashIsKnown(block.hash);
    if (isBlockKnown) {
      blocksDone++;
      continue;
    }
    Log.debug(`hash mismatch on block ${blockNumber}!`);
  }

    const txrs = await Transactions.getTxrsWithRetry(block);

  // Contracts marked as mined in a block that was rolled back are possibly wrong. Reanalyze 'contract mined at' data if we want very high confidence.
  await ContractBaseFees.deleteContractBaseFees(blockNumber)();
  await Blocks.deleteBlock(blockNumber)();

  // Add block
  const ethPrice = await pipe(
    EthPrices.getEthPrice(block.timestamp, Duration.millisFromMinutes(2)),
    TEAlt.getOrThrow,
  )();

  await Blocks.storeBlock(block, txrs, ethPrice.ethusd);

  await storeLastReanalyzed(blockNumber);
  blocksDone++;
}
