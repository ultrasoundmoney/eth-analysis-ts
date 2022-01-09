import * as DateFns from "date-fns";
import { test } from "uvu";
import * as assert from "uvu/assert";
import { BlockDb } from "../blocks/blocks.js";
import * as BlocksData from "../blocks_data.js";
import * as BurnRecords from "../burn-records/burn_records.js";
import * as BurnRecordsNewHead from "../burn-records/new_head.js";
import * as BurnRecordsSync from "../burn-records/sync.js";
import { runMigrations, sql } from "../db.js";
import { A, O, pipe } from "../fp.js";
import * as TimeFrames from "../time_frames.js";

// eslint-disable-next-line @typescript-eslint/no-explicit-any
(BigInt.prototype as any).toJSON = function () {
  return this.toString() + "n";
};

const insertableFromBlock = (block: BlockDb) => ({
  base_fee_sum: block.baseFeeSum,
  hash: block.hash,
  mined_at: block.minedAt,
  number: block.number,
});

const insertTestBlocks = async (blocks: BlockDb[]) =>
  pipe(
    blocks,
    A.map(insertableFromBlock),
    (blocks) => sql`
      INSERT INTO blocks
        ${sql(blocks)}
    `,
  );

const setBlocksToNow = (blocks: BlockDb[]): BlockDb[] =>
  pipe(
    blocks,
    A.reverse,
    A.reduceWithIndex([] as BlockDb[], (index, blocks, block) => [
      ...blocks,
      { ...block, minedAt: new Date(Date.now() - index * 1000 * 60) },
    ]),
    A.reverse,
  );

const clearBurnRecordTables = async () => {
  await sql`DELETE FROM burn_records`;
  await sql`DELETE FROM blocks`;
  await sql`DELETE FROM analysis_state`;
};

test.before(async () => {
  await runMigrations();
});

test.after.each(async () => {
  await clearBurnRecordTables();
});

test("should return none when no block has been included", async () => {
  const lastIncludedBlock = await BurnRecords.getLastIncludedBlock()();
  assert.equal(lastIncludedBlock, O.none);
});

test("should set the last included block on init", async () => {
  const block = await BlocksData.getSingleBlock();
  await insertTestBlocks([block]);

  await BurnRecordsSync.init()();

  const lastIncludedBlock = await BurnRecords.getLastIncludedBlock()();
  assert.equal(lastIncludedBlock, O.some(block.number));
});

test("should set the last included block on new head", async () => {
  const block = await BlocksData.getSingleBlock();
  await insertTestBlocks([block]);

  await BurnRecordsNewHead.onNewBlock(block)();

  const lastIncludedBlock = await BurnRecords.getLastIncludedBlock()();
  assert.equal(lastIncludedBlock, O.some(block.number));
});

test("should sync new blocks on init", async () => {
  const block = await BlocksData.getSingleBlock();
  const [nowBlock] = setBlocksToNow([block]);
  await insertTestBlocks([nowBlock]);

  await BurnRecordsSync.init()();

  for (const timeFrame of TimeFrames.timeFrames) {
    const [topRecord] = await BurnRecords.getBurnRecords(timeFrame)();
    assert.equal(topRecord, {
      baseFeeSum: Number(2868590697273401000n),
      blockNumber: block.number,
      minedAt: nowBlock.minedAt,
    });
  }
});

test("should add a new block on new head", async () => {
  const block = await BlocksData.getSingleBlock();
  const [nowBlock] = setBlocksToNow([block]);
  await insertTestBlocks([nowBlock]);

  await BurnRecordsNewHead.onNewBlock(block)();

  for (const timeFrame of TimeFrames.timeFrames) {
    const [topRecord] = await BurnRecords.getBurnRecords(timeFrame)();
    assert.equal(topRecord, {
      baseFeeSum: Number(2868590697273401000n),
      blockNumber: block.number,
      minedAt: nowBlock.minedAt,
    });
  }
});

test("should expire records outside time frame on init", async () => {
  const block = await BlocksData.getSingleBlock();
  await insertTestBlocks([block]);

  await BurnRecordsSync.init()();

  const topRecords = await BurnRecords.getBurnRecords("5m")();
  assert.is(topRecords.length, 0);
});

test("should expire a record that's fallen outside the time frame", async () => {
  const blocks = await BlocksData.getM5Blocks();
  const oldBlock = {
    ...blocks[0],
    minedAt: pipe(new Date(), (dt) => DateFns.subMinutes(dt, 6)),
  };
  const newBlock = {
    ...blocks[1],
    minedAt: new Date(),
  };
  await insertTestBlocks([oldBlock, newBlock]);

  await BurnRecordsNewHead.onNewBlock(newBlock)();

  const topRecords = await BurnRecords.getBurnRecords("5m")();
  assert.is(topRecords.length, 1);

  const newRecord = topRecords[0];
  assert.is(newRecord.blockNumber, newBlock.number);
});

test("should remove records on rollback", async () => {
  const block = await BlocksData.getSingleBlock();
  const [nowBlock] = setBlocksToNow([block]);
  await insertTestBlocks([nowBlock]);

  await BurnRecordsNewHead.onNewBlock(block)();
  const [topRecord] = await BurnRecords.getBurnRecords("5m")();
  assert.equal(topRecord, {
    baseFeeSum: Number(2868590697273401000n),
    blockNumber: block.number,
    minedAt: nowBlock.minedAt,
  });

  await BurnRecordsNewHead.onRollback(block.number)();
  const topRecords = await BurnRecords.getBurnRecords("5m")();
  assert.is(topRecords.length, 0);
});

test("should update last included on rollback", async () => {
  const block = await BlocksData.getSingleBlock();
  const [nowBlock] = setBlocksToNow([block]);
  await insertTestBlocks([nowBlock]);

  await BurnRecordsNewHead.onNewBlock(block)();
  await BurnRecordsNewHead.onRollback(block.number)();

  const lastIncludedBlock = await BurnRecords.getLastIncludedBlock()();
  assert.equal(lastIncludedBlock, O.some(block.number - 1));
});

test("should prune records outside max rank", async () => {
  const blocks = await BlocksData.getH1Blocks();
  const nowBlocks = setBlocksToNow(blocks);
  await insertTestBlocks(nowBlocks);
  await BurnRecordsSync.init()();

  const topRecords = await BurnRecords.getBurnRecords("all")();
  assert.is(topRecords.length, 100);
});

test.after(async () => {
  await sql.end();
});

test.run();
