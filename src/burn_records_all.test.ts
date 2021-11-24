import { test } from "uvu";
import * as BlocksData from "./blocks_data.js";
import * as assert from "uvu/assert";
import * as BurnRecordsAll from "./burn_records_all.js";
import { BlockMap, FeeRecordMap } from "./burn_records_all.js";
import BigNumber from "bignumber.js";

// #### On new block
//     * Recalculate max block age for each granularity, expire older blocks.
//     * Add the new block.
//     * For each denomination:
//         * Calculate the base fee sum.
//         * Compare with existing records.
//         * If the sum is greater than the lowest record's sum, remove that one, and insert the new one. Update the DB.

const feeRecordMap: FeeRecordMap = {
  block: [],
  m5: [],
  h1: [],
  d1: [],
  d7: [],
};
const blockMap: BlockMap = {
  block: [],
  m5: [],
  h1: [],
  d1: [],
  d7: [],
};

// const m5InMillis = 5 * 60 * 1000;
// const getIsBlockExpired = BurnRecordsAll.getIsBlockWithinReferenceMaxAge(
//   m5InMillis,
//   feeBlock,
// );

test("adds a new record", async () => {
  const singleBlock = await BlocksData.getSingleBlock()();

  const feeBlock = {
    number: singleBlock.number,
    minedAt: singleBlock.minedAt,
    fees: new BigNumber(singleBlock.baseFeeSum),
  };

  const { feeRecords, inScopeBlocks } = BurnRecordsAll.addBlock(
    [],
    [],
    BurnRecordsAll.sortingOrdMap["max"],
    feeBlock,
  );

  // Added block to empty records is now nr. 1 record.
  assert.is(feeRecords[0].number, feeBlock.number);

  // Added block is now in scope.
  assert.is(inScopeBlocks[0].number, feeBlock.number);
});

test.run();
