import * as DateFns from "date-fns";
import makeEta from "simple-eta";
import { calcBlockFeeBreakdown } from "./base_fees.js";
import * as Blocks from "./blocks/blocks.js";
import * as Contracts from "./contracts/contracts.js";
import { sql } from "./db.js";
import * as Duration from "./duration.js";
import * as EthPrices from "./eth-prices/eth_prices.js";
import { E } from "./fp.js";
import * as Leaderboards from "./leaderboards.js";
import * as LeaderboardsAll from "./leaderboards_all.js";
import * as Log from "./log.js";
import * as Transactions from "./transactions.js";

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
  const [storedBlock] = await Blocks.getBlocks(blockNumber, blockNumber);
  const block = await Blocks.getBlockWithRetry(blockNumber);

  if (blockNumber % 100 === 0 && blockNumber !== 0) {
    Log.debug(
      `blocks done: ${blocksDone}, eta: ${eta.estimate().toFixed(0)}s left`,
    );
    await storeLastReanalyzed(blockNumber);
  }

  if (storedBlock.hash === block.hash) {
    blocksDone++;
    eta.report(blocksDone);
    continue;
  }

  Log.debug(`block ${blockNumber} hash mismatch, reanalyzing!`);

  const txrs = await Transactions.getTxrsWithRetry(block);

  // Remove block
  const sumsToRollback = await Leaderboards.getRangeBaseFees(
    blockNumber,
    blockNumber,
  )();
  await LeaderboardsAll.removeContractBaseFeeSums(sumsToRollback);

  // Contracts marked as mined in a block that was rolled back are possibly wrong. Reanalyze 'contract mined at' data if we want very high confidence.
  // await Contracts.deleteContractsMinedAt(blockNumber);
  await Blocks.deleteContractBaseFees(blockNumber);
  await Blocks.deleteDerivedBlockStats(blockNumber);
  await Blocks.deleteBlock(blockNumber);

  // Add block
  const ethPrice = await EthPrices.getEthPrice(
    DateFns.fromUnixTime(block.timestamp),
    Duration.millisFromMinutes(2),
  )();
  if (E.isLeft(ethPrice)) {
    throw ethPrice.left;
  }
  await Blocks.storeBlock(block, txrs, ethPrice.right.ethusd);
  const feeBreakdown = calcBlockFeeBreakdown(
    block,
    txrs,
    ethPrice.right.ethusd,
  );
  await LeaderboardsAll.addBlock(
    block.number,
    feeBreakdown.contract_use_fees,
    feeBreakdown.contract_use_fees_usd!,
  );

  await storeLastReanalyzed(blockNumber);
  Log.debug(`reanalyzed ${blockNumber}`);
}
