import * as DateFns from "date-fns";
import _ from "lodash";
import { test } from "uvu";
import * as assert from "uvu/assert";
import * as BurnRecords from "../burn-records/burn_records.js";
import * as BurnRecordsNewHead from "../burn-records/new_head.js";
import * as BurnRecordsSync from "../burn-records/sync.js";
import * as Db from "../db.js";
import { runMigrations } from "../db.js";
import { NEA, O } from "../fp.js";
import * as SamplesBlocks from "../samples/blocks.js";
import * as TimeFrames from "../time_frames.js";
import * as MockDb from "./mock_db.js";

test.before(async () => {
  await runMigrations();
});

test.after.each(() => resetTables());

test.after(() => Db.closeConnection());

const resetTables = async () => {
  await Db.sql`DELETE FROM contract_base_fees`;
  await Db.sql`DELETE FROM contracts`;
  await Db.sql`DELETE FROM burn_records`;
  await Db.sql`DELETE FROM blocks`;
  await Db.sql`DELETE FROM analysis_state`;
  await Db.sql`DELETE FROM key_value_store`;
};

test("should return none when no block has been included", async () => {
  const lastIncludedBlock = await BurnRecords.getLastIncludedBlock()();
  assert.equal(lastIncludedBlock, O.none);
});

test("should set the last included block on sync", async () => {
  await MockDb.seedBlocks("m5")();

  await BurnRecordsSync.sync()();

  const lastIncludedBlock = await BurnRecords.getLastIncludedBlock()();
  assert.equal(lastIncludedBlock, O.some(12965022));
});

test("should set the last included block on new head", async () => {
  const [block] = await SamplesBlocks.getBlocksFromFile("m5")();
  await MockDb.seedBlocks("m5")();
  await BurnRecordsNewHead.onNewBlock(block)();

  const lastIncludedBlock = await BurnRecords.getLastIncludedBlock()();
  assert.equal(lastIncludedBlock, O.some(block.number));
});

test("should sync new blocks on sync", async () => {
  await MockDb.seedBlocks("m5")();

  await BurnRecordsSync.sync()();

  for (const timeFrame of TimeFrames.timeFramesNext) {
    const [topRecord] = await BurnRecords.getBurnRecords(timeFrame)();
    assert.is(topRecord.blockNumber, 12965022);
    assert.is(topRecord.baseFeeSum, Number(395313673917850200n));
  }
});

test("should add a new block on new head", async () => {
  const blocks = await SamplesBlocks.getBlocksFromFile("m5")();
  const block = _.last(blocks)!;
  await MockDb.seedBlocks("m5")();

  await BurnRecordsNewHead.onNewBlock(block)();

  for (const timeFrame of TimeFrames.timeFramesNext) {
    const [topRecord] = await BurnRecords.getBurnRecords(timeFrame)();
    assert.is(topRecord.blockNumber, 12965022);
    assert.is(topRecord.baseFeeSum, Number(395313673917850200n));
  }
});

test("should expire records outside time frame on sync", async () => {
  await MockDb.seedBlocks("m5", true, true)();

  await BurnRecordsSync.sync()();

  const topRecords = await BurnRecords.getBurnRecords("m5")();
  for (const record of topRecords) {
    assert.ok(
      DateFns.differenceInSeconds(new Date(), record.minedAt) <= 5 * 60,
    );
  }
});

// Weak test. Should be improved to somehow first add the record, then expire it. Normally this takes time. We don't want to wait.
test.skip("should expire a record that's fallen outside the time frame", async () => {
  const blocks = await MockDb.getSeedBlocks("m5", true, true)();
  const firstBlock = NEA.head(blocks);
  const lastBlock = NEA.last(blocks);
  await MockDb.seedBlocks("m5", true, true)();

  await BurnRecordsNewHead.onNewBlock(firstBlock)();
  const topRecordsPre = await BurnRecords.getBurnRecords("m5")();
  assert.ok(
    topRecordsPre.some((record) => record.blockNumber === firstBlock.number),
  );

  await BurnRecordsNewHead.onNewBlock(lastBlock)();
  const topRecords = await BurnRecords.getBurnRecords("m5")();
  assert.not.ok(
    topRecords.some((record) => record.blockNumber === firstBlock.number),
  );
});

test("should remove records on rollback", async () => {
  await MockDb.seedBlocks("m5")();

  await BurnRecordsSync.sync()();
  const [topRecord] = await BurnRecords.getBurnRecords("m5")();
  assert.is(topRecord.blockNumber, 12965022);
  assert.is(topRecord.baseFeeSum, Number(395313673917850200n));

  await BurnRecordsNewHead.onRollback(12965022)();
  const [topRecordPostRollback] = await BurnRecords.getBurnRecords("m5")();
  assert.is.not(topRecordPostRollback.blockNumber, 12965022);
});

test("should update last included on rollback", async () => {
  const [block] = await SamplesBlocks.getBlocksFromFile("m5")();
  await MockDb.seedBlocks("m5")();

  await BurnRecordsNewHead.onNewBlock(block)();
  await BurnRecordsNewHead.onRollback(block.number)();

  const lastIncludedBlock = await BurnRecords.getLastIncludedBlock()();
  assert.equal(lastIncludedBlock, O.some(block.number - 1));
});

test("should prune records outside max rank", async () => {
  await MockDb.seedBlocks("h1")();
  await BurnRecordsSync.sync()();

  const topRecords = await BurnRecords.getBurnRecords("all")();
  assert.is(topRecords.length, 10);
});

test.run();
