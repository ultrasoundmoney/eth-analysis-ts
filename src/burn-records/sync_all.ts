import PQueue from "p-queue";
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

export const syncBlocksQueue = new PQueue({ concurrency: 1 });

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

    All.feeRecordMap[granularity][sorting][denomination] = feeRecords;
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

      All.feeSetMap[granularity][denomination] = {
        sum: BurnRecords.sumFeeBlocks(feeBlocks),
        blocks: feeBlocks,
      };
    }
  }
};

const getNextBlockToAnalyze = async () => {
  const lastAnalyzed = await getLastAnalyzedBlockNumber();
  return lastAnalyzed === undefined
    ? Blocks.londonHardForkBlockNumber
    : lastAnalyzed + 1;
};

const addAllMissingBlocks = async (blocks: Blocks.FeeBlockRow[]) => {
  Log.debug(`burn-records-all sync ${blocks.length} blocks`);

  const eta = makeEta({ max: blocks.length });

  const id = setInterval(() => {
    eta.report(blocks.length - syncBlocksQueue.size);
    if (syncBlocksQueue.size === 0) {
      clearInterval(id);
      return;
    }
    Log.debug(`sync burn-records-all blocks, eta: ${eta.estimate()}s`);
  }, 8000);

  syncBlocksQueue.addAll(
    blocks.map(
      (block) => () =>
        BurnRecords.addBlock(
          () => Promise.resolve(),
          All.feeSetMap,
          All.feeRecordMap,
          block,
        ),
    ),
  );
};

export const sync = async (): Promise<void> => {
  const [nextToAdd, lastStoredBlock] = await Promise.all([
    getNextBlockToAnalyze(),
    Blocks.getLastStoredBlock(),
    readStoredFeeRecords(),
  ]);

  await readFeeSets(lastStoredBlock);

  const missingBlocksCount = lastStoredBlock.number - nextToAdd + 1;

  // No blocks missing, we're done.
  if (missingBlocksCount <= 0) {
    Log.debug("init burn records all, already in sync");
    return undefined;
  }

  const blocks = await Blocks.getBlocks(nextToAdd, lastStoredBlock.number);
  Log.debug(`init burn records all, ${blocks.length} blocks to add`);
  await addAllMissingBlocks(blocks);

  return undefined;
};
