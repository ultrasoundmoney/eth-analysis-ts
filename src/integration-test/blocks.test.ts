import test from "ava";
import * as Blocks from "../blocks/blocks.js";
import * as Db from "../db.js";
import * as MockDb from "./mock_db.js";

test.before(() => Db.runMigrations());

test.after(() => Db.closeConnection());

test.afterEach(() => MockDb.resetTables()());

test("return the last stored block", async (t) => {
  await MockDb.seedBlocks("m5")();
  const storedBlock = await Blocks.getLastStoredBlock()();
  t.is(
    storedBlock.hash,
    "0xd896114fc465d6217b94e9198de99502a990b3482756797a90ae3e2ee6dc1168",
  );
});
