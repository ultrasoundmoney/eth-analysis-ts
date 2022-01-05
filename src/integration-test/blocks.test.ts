import { test } from "uvu";
import * as assert from "uvu/assert";
import * as Blocks from "../blocks/blocks.js";
import { BlockDb } from "../blocks/blocks.js";
import * as BlocksData from "../blocks_data.js";
import { runMigrations, sql } from "../db.js";

const insertTestBlock = async (block: BlockDb) => {
  const insertable = {
    base_fee_sum: block.baseFeeSum,
    hash: block.hash,
    mined_at: block.minedAt,
    number: block.number,
  };

  await sql`
    INSERT INTO blocks
      ${sql([insertable])}
  `;
};

const removeTestBlock = async (blockNumber: number) => sql`
  DELETE FROM blocks
  WHERE number = ${blockNumber}
`;

test.before(async () => {
  await runMigrations();
});

test("return the last stored block", async () => {
  const block = await BlocksData.getSingleBlock();
  await insertTestBlock(block);
  const storedBlock = await Blocks.getLastStoredBlock();
  assert.is(storedBlock.hash, block.hash);

  await removeTestBlock(block.number);
});

test.after(async () => {
  await sql.end();
});

test.run();
