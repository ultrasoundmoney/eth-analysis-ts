import { test } from "uvu";
import * as assert from "uvu/assert";
import * as Blocks from "../blocks/blocks.js";
import { runMigrations, sql } from "../db.js";
import * as MockDb from "./mock_db.js";

test.before(async () => {
  await runMigrations();
});

test.after.each(async () => {
  MockDb.resetTables();
});

test.after(async () => {
  await sql.end();
});

test("return the last stored block", async () => {
  await MockDb.seedBlocks("m5")();
  const storedBlock = await Blocks.getLastStoredBlock()();
  assert.is(
    storedBlock.hash,
    "0xd896114fc465d6217b94e9198de99502a990b3482756797a90ae3e2ee6dc1168",
  );
});

test.run();
