import makeEta from "simple-eta";
import * as Blocks from "../blocks/blocks.js";
import { sql } from "../db.js";
import { Denomination, denominations } from "../denominations.js";
import * as Log from "../log.js";
import * as All from "./all.js";
import { getLastAnalyzedBlockNumber } from "./analysis_state.js";
import { FeeBlock, FeeRecord, Granularity, Sorting } from "./burn_records.js";
import * as BurnRecords from "./burn_records.js";
import { BlockDb } from "../blocks/blocks.js";
import * as Cartesian from "../cartesian.js";
import _ from "lodash";
import { logPerf } from "../performance.js";

type FeeRecordRow = {
  firstBlock: number;
  lastBlock: number;
  feeSum: string;
  granularity: string;
  denomination: string;
};

const feeRecordFromRecordRow = (row: FeeRecordRow): FeeRecord => ({
  firstBlock: row.firstBlock,
  lastBlock: row.lastBlock,
  feeSum: BigInt(row.feeSum),
});

const getFeeRecords = async (
  denomination: Denomination,
  granularity: Granularity,
  sorting: Sorting,
): Promise<FeeRecord[]> => {
  const rows = await sql<FeeRecordRow[]>`
      SELECT
        fee_sum,
        first_block,
        last_block
      FROM fee_records
      WHERE granularity = ${granularity}
      AND denomination = ${denomination}
      AND sorting = ${sorting}
    `;

  return rows.map(feeRecordFromRecordRow);
};

const readStoredFeeRecords = async (): Promise<void> => {
  const tasks = Cartesian.make3(
    denominations,
    BurnRecords.granularities,
    BurnRecords.sortings,
  ).map(async ([denomination, granularity, sorting]) => {
    const feeRecordsUnsorted = await getFeeRecords(
      denomination,
      granularity,
      sorting,
    );

    const feeRecords = feeRecordsUnsorted.sort(
      BurnRecords.orderingMap[sorting].compare,
    );

    // All.feeRecordMap[granularity][sorting][denomination] = feeRecords;
  });

  await Promise.all(tasks);
};

const getFeeBlocks = async (
  denomination: Denomination,
  granularity: Granularity,
  upToIncluding: BlockDb,
): Promise<FeeBlock[]> => {
  const blocks = await Blocks.getBlocksForGranularity(
    granularity,
    upToIncluding,
  );

  return blocks.map((blockRecord) => ({
    number: blockRecord.number,
    minedAt: blockRecord.minedAt,
    fees:
      denomination === "eth"
        ? blockRecord.baseFeePerGas * blockRecord.gasUsed
        : (blockRecord.baseFeePerGas *
            blockRecord.gasUsed *
            blockRecord.ethPriceCents) /
          10n ** 18n,
  }));
};

const readFeeSets = async (upToIncluding: BlockDb): Promise<void> => {
  for (const denomination of denominations) {
    for (const granularity of BurnRecords.granularities) {
      const feeBlocks = await getFeeBlocks(
        denomination,
        granularity,
        upToIncluding,
      );

      // All.feeSetMap[granularity][denomination] = {
      //   sum: BurnRecords.sumFeeBlocks(feeBlocks),
      //   blocks: feeBlocks,
      // };
    }
  }
};

const getNextBlockToAnalyze = async () => {
  const lastAnalyzed = await getLastAnalyzedBlockNumber();
  return lastAnalyzed === undefined
    ? Blocks.londonHardForkBlockNumber
    : lastAnalyzed + 1;
};

export const sync = async (): Promise<void> => {
  Log.debug("syncing burn records all");
  const tReadFeeRecords = performance.now();
  const [nextToAdd, lastStoredBlock] = await Promise.all([
    getNextBlockToAnalyze(),
    Blocks.getLastStoredBlock(),
    readStoredFeeRecords(),
  ]);
  logPerf("reading fee records all", tReadFeeRecords);

  const tReadFeeSets = performance.now();
  await readFeeSets(lastStoredBlock);
  logPerf("reading fee sets", tReadFeeSets);

  const missingBlocksCount = lastStoredBlock.number - nextToAdd + 1;

  // No blocks missing, we're done.
  if (missingBlocksCount <= 0) {
    Log.debug("sync burn records all, already in sync");
    return undefined;
  }

  const blocksToSync = Blocks.getBlockRange(nextToAdd, lastStoredBlock.number);
  Log.debug(`sync burn records all, ${blocksToSync.length} blocks to add`);

  const eta = makeEta({ max: blocksToSync.length });
  let blocksDone = 0;

  const id = setInterval(() => {
    eta.report(blocksDone);
    if (blocksToSync.length === blocksDone) {
      clearInterval(id);
      return;
    }
    Log.debug(`sync burn records all, eta: ${eta.estimate()}s`);
  }, 8000);

  // Grabbing blocks from the DB one-by-one is slow, yet we may need all blocks since the London hardfork, therefore we work in chunks of 10_000.
  for (const chunk of _.chunk(blocksToSync, 1000)) {
    const blocks = await Blocks.getBlocks(_.first(chunk)!, _.last(chunk)!);

    for (const block of blocks) {
      await All.onNewBlock(block);
      blocksDone = blocksDone + 1;
    }
  }

  Log.info("sync burn records all, done");
};
