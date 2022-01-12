import { test } from "uvu";
import * as assert from "uvu/assert";
import * as Blocks from "./blocks.js";

test("should make block number sequences", () => {
  const blockRange = Blocks.getBlockRange(10, 14);
  assert.equal(blockRange, [10, 11, 12, 13, 14]);
});

test.run();
