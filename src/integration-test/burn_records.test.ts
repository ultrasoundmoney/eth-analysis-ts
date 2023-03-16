import test from "ava";
import * as DateFns from "date-fns";
import _ from "lodash";
import { BlockV1 } from "../blocks/blocks.js";
import * as BurnRecords from "../burn-records/burn_records.js";
import * as BurnRecordsNewHead from "../burn-records/new_head.js";
import * as BurnRecordsSync from "../burn-records/sync.js";
import * as Db from "../db.js";
import { NEA, O } from "../fp.js";
import * as SamplesBlocks from "../samples/blocks.js";
import * as TimeFrames from "../time_frames.js";
import * as MockDb from "./mock_db.js";

test.before(() => Db.runMigrations());

test.after(() => Db.closeConnection());

test.afterEach(() => MockDb.resetTables()());

test("should return none when no block has been included", async (t) => {
  const lastIncludedBlock = await BurnRecords.getLastIncludedBlock()();
  t.deepEqual(lastIncludedBlock, O.none);
});

test("should set the last included block on sync", async (t) => {
  await MockDb.seedBlocks("m5")();

  await BurnRecordsSync.sync()();

  const lastIncludedBlock = await BurnRecords.getLastIncludedBlock()();
  t.deepEqual(lastIncludedBlock, O.some(12965022));
});

test("should set the last included block on new head", async (t) => {
  const [block] = await SamplesBlocks.getBlocksFromFile("m5")();
  await MockDb.seedBlocks("m5")();
  await BurnRecordsNewHead.onNewBlock(block)();

  const lastIncludedBlock = await BurnRecords.getLastIncludedBlock()();
  t.deepEqual(lastIncludedBlock, O.some(block.number));
});

test("should sync new blocks on sync", async (t) => {
  await MockDb.seedBlocks("m5")();

  await BurnRecordsSync.sync()();

  for (const timeFrame of TimeFrames.timeFramesNext) {
    const [topRecord] = await BurnRecords.getBurnRecords(timeFrame)();
    t.is(topRecord.blockNumber, 12965022);
    t.is(topRecord.baseFeeSum, Number(395313673917850200n));
  }
});

test("should add a new block on new head", async (t) => {
  const blocks = await SamplesBlocks.getBlocksFromFile("m5")();
  const block = _.last(blocks)!;
  await MockDb.seedBlocks("m5")();

  await BurnRecordsNewHead.onNewBlock(block)();

  for (const timeFrame of TimeFrames.timeFramesNext) {
    const [topRecord] = await BurnRecords.getBurnRecords(timeFrame)();
    t.is(topRecord.blockNumber, 12965022);
    t.is(topRecord.baseFeeSum, Number(395313673917850200n));
  }
});

test("should expire records outside time frame on sync", async (t) => {
  await MockDb.seedBlocks("m5", true, true)();

  await BurnRecordsSync.sync()();

  const topRecords = await BurnRecords.getBurnRecords("m5")();
  for (const record of topRecords) {
    t.true(DateFns.differenceInSeconds(new Date(), record.minedAt) <= 5 * 60);
  }
});

// Weak test. Should be improved to somehow first add the record, then expire it. Normally this takes time. We don't want to wait.
test.skip("should expire a record that's fallen outside the time frame", async (t) => {
  const blocks = await MockDb.getSeedBlocks("m5", true, true)();
  const firstBlock = NEA.head(blocks);
  const lastBlock = NEA.last(blocks);
  await MockDb.seedBlocks("m5", true, true)();

  await BurnRecordsNewHead.onNewBlock(firstBlock)();
  const topRecordsPre = await BurnRecords.getBurnRecords("m5")();
  t.true(
    topRecordsPre.some((record) => record.blockNumber === firstBlock.number),
  );

  await BurnRecordsNewHead.onNewBlock(lastBlock)();
  const topRecords = await BurnRecords.getBurnRecords("m5")();
  t.false(
    topRecords.some((record) => record.blockNumber === firstBlock.number),
  );
});

test("should remove records on rollback", async (t) => {
  await MockDb.seedBlocks("m5")();

  await BurnRecordsSync.sync()();
  const [topRecord] = await BurnRecords.getBurnRecords("m5")();
  t.is(topRecord.blockNumber, 12965022);
  t.is(topRecord.baseFeeSum, Number(395313673917850200n));

  await BurnRecordsNewHead.rollbackBlocks(
    NEA.of({ number: 12965022 } as BlockV1),
  )();
  const [topRecordPostRollback] = await BurnRecords.getBurnRecords("m5")();
  t.not(topRecordPostRollback.blockNumber, 12965022);
});

test("should update last included on rollback", async (t) => {
  const [block] = await SamplesBlocks.getBlocksFromFile("m5")();
  await MockDb.seedBlocks("m5")();

  await BurnRecordsNewHead.onNewBlock(block)();
  await BurnRecordsNewHead.rollbackBlocks(NEA.of(block))();

  const lastIncludedBlock = await BurnRecords.getLastIncludedBlock()();
  t.deepEqual(lastIncludedBlock, O.some(block.number - 1));
});

test("should prune records outside max rank", async (t) => {
  await MockDb.seedBlocks("h1")();
  await BurnRecordsSync.sync()();

  const topRecords = await BurnRecords.getBurnRecords("since_burn")();
  t.is(topRecords.length, 10);
});
