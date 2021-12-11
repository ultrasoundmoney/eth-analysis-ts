import _ from "lodash";
import PQueue from "p-queue";
import * as Blocks from "../blocks/blocks.js";
import { BlockDb } from "../blocks/blocks.js";
import * as DateFnsAlt from "../date_fns_alt.js";
import { sql } from "../db.js";
import { Denomination, denominations } from "../denominations.js";
import { A, pipe } from "../fp.js";
import * as Log from "../log.js";
import { logPerf } from "../performance.js";
import {
  addBlockToState,
  FeeBlock,
  FeeRecord,
  Granularity,
  RecordState,
  rollbackBlock,
  Sorting,
} from "./burn_records.js";

export const recordStates: RecordState[] = [];
// export const recordStates = Cartesian.make3(
//   denominations,
//   granularities,
//   sortings,
// ).map();

export const expireOldBlocks = (
  maxAge: number,
  referenceDate: Date,
  inScopeBlocks: FeeBlock[],
): FeeBlock[] =>
  pipe(
    inScopeBlocks,
    A.filter(
      (block) =>
        DateFnsAlt.millisecondsBetweenAbs(referenceDate, block.minedAt) <=
        maxAge,
    ),
  );

const storeLastAnalyzed = async (lastAnalyzedBlock: number): Promise<void> => {
  await sql`
    INSERT INTO analysis_state (
      key,
      last_analyzed_block
    ) VALUES (
      'burn_records_all',
      NULL
    ) ON CONFLICT (key) DO UPDATE SET
      last_analyzed_block = ${lastAnalyzedBlock}
  `;

  return undefined;
};

export const storeNewBlockQueue = new PQueue({
  concurrency: 1,
  autoStart: false,
});

type InsertableFeeRecordRow = {
  denomination: string;
  granularity: string;
  sorting: string;
  first_block: number;
  last_block: number;
  fee_sum: string;
};

const rowFromFeeRecord = (
  denomination: Denomination,
  granularity: Granularity,
  sorting: Sorting,
  feeRecord: FeeRecord,
): InsertableFeeRecordRow => ({
  denomination,
  sorting,
  granularity,
  first_block: feeRecord.firstBlock,
  last_block: feeRecord.lastBlock,
  fee_sum: feeRecord.feeSum.toString(),
});

const writeFeeRecordsToDb = async (
  denomination: Denomination,
  granularity: Granularity,
  sorting: Sorting,
  feeRecords: FeeRecord[],
): Promise<void> => {
  const rows = feeRecords.map((feeRecord) =>
    rowFromFeeRecord(denomination, granularity, sorting, feeRecord),
  );

  await sql.begin(async (sql) => {
    await sql`
      DELETE FROM fee_records
      WHERE denomination = ${denomination}
      AND sorting = ${sorting}
      AND granularity = ${granularity}
    `;
    await sql<InsertableFeeRecordRow[]>`
      INSERT INTO fee_records
        ${sql(rows)}
    `;
  });
};

export const onNewBlock = async (block: BlockDb): Promise<void> => {
  const t0 = performance.now();
  addBlockToState(recordStates[0], block);
  // TODO: write new records to DB
  logPerf("add block to burn record all took: ", t0);
  await storeLastAnalyzed(block.number);
};

export const onRollback = async (
  rollbackToAndIncluding: number,
): Promise<void> => {
  Log.debug(
    `burn record all rollback to and including block: ${rollbackToAndIncluding}`,
  );

  const latestIncludedBlock = recordStates[0].feeBlocks.peekBack();

  if (latestIncludedBlock === undefined) {
    Log.warn(
      "tried to rollback burn-records-all but no block in fee set sum, skipping",
    );
    return;
  }

  const blocksToRollback = Blocks.getBlockRange(
    rollbackToAndIncluding,
    latestIncludedBlock.number,
  ).reverse();

  for (const blockNumber of blocksToRollback) {
    const [block] = await Blocks.getBlocks(blockNumber, blockNumber);
    rollbackBlock(recordStates[0], block);
    await storeLastAnalyzed(blockNumber - 1);
  }
};
