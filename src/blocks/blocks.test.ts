import test from "ava";
import * as Blocks from "./blocks.js";

test("should make block number sequences", (t) => {
  const blockRange = Blocks.getBlockRange(10, 14);
  t.deepEqual(blockRange, [10, 11, 12, 13, 14]);
});
