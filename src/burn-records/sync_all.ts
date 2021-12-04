import PQueue from "p-queue";
import makeEta from "simple-eta";
import {
  FeeBlockRow,
  getBlocks,
  getBlocksForGranularity,
  getLatestKnownBlockNumber,
  londonHardForkBlockNumber,
} from "../blocks.js";
import {
  addBlock,
  FeeBlock,
  FeeRecord,
  feeRecordMap,
  feeSetMap,
  granularities,
  Granularity,
  orderingMap,
  Sorting,
  sortings,
  sumFeeBlocks,
} from "../burn_records_all.js";
import { sql } from "../db.js";
import { Denomination, denominations } from "../denominations.js";
import { debug } from "../log.js";
import { getLastAnalyzedBlockNumber } from "./analysis_state.js";

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
  const tasks = denominations.flatMap((denomination) =>
    granularities.flatMap((granularity) =>
      sortings.flatMap(async (sorting) => {
        const feeRecordsUnsorted = await getFeeRecords(
          denomination,
          granularity,
          sorting,
        );

        const feeRecords = feeRecordsUnsorted.sort(
          orderingMap[sorting].compare,
        );

        feeRecordMap[granularity][sorting][denomination] = feeRecords;

        return undefined;
      }),
    ),
  );

  await Promise.all(tasks);
};

const getFeeBlocks = async (
  denomination: Denomination,
  granularity: Granularity,
  upToIncluding: number,
): Promise<FeeBlock[]> => {
  const blocks = await getBlocksForGranularity(granularity, upToIncluding);

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

const readFeeSets = async (upToIncluding: number): Promise<void> => {
  for (const denomination of denominations) {
    for (const granularity of granularities) {
      const feeBlocks = await getFeeBlocks(
        denomination,
        granularity,
        upToIncluding,
      );

      feeSetMap[granularity][denomination] = {
        sum: sumFeeBlocks(feeBlocks),
        blocks: feeBlocks,
      };
    }
  }
};

const getNextBlockToAnalyze = async () => {
  const lastAnalyzed = await getLastAnalyzedBlockNumber();
  return lastAnalyzed === undefined
    ? londonHardForkBlockNumber
    : lastAnalyzed + 1;
};

const addAllMissingBlocks = async (blocks: FeeBlockRow[]) => {
  debug(`burn-records-all sync ${blocks.length} blocks`);

  const eta = makeEta({ max: blocks.length });

  const id = setInterval(() => {
    eta.report(blocks.length - syncBlocksQueue.size);
    if (syncBlocksQueue.size === 0) {
      clearInterval(id);
      return;
    }
    debug(`sync burn-records-all blocks, eta: ${eta.estimate()}s`);
  }, 8000);

  syncBlocksQueue.addAll(blocks.map(addBlock));
};

export const sync = async (): Promise<void> => {
  const [nextToAdd, lastToAdd] = await Promise.all([
    getNextBlockToAnalyze(),
    getLatestKnownBlockNumber(),
    readStoredFeeRecords(),
  ]);

  await readFeeSets(lastToAdd);

  const missingBlocksCount = lastToAdd - nextToAdd + 1;

  // No blocks missing, we're done.
  if (missingBlocksCount <= 0) {
    debug("init burn records all, already in sync");
    return undefined;
  }

  debug("sets", feeSetMap);
  debug("records", feeRecordMap);

  const blocks = await getBlocks(nextToAdd, lastToAdd)();
  debug(`init burn records all, ${blocks.length} blocks to add`);
  await addAllMissingBlocks(blocks);

  return undefined;
};
