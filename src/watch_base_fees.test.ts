// eslint-disable-next-line node/no-unpublished-import
import test from "ava";
import * as BaseFees from "./base_fees.js";

test("getBlockRange", (t) => {
  const blockRange = BaseFees.getBlockRange(10, 14);
  t.deepEqual(blockRange, [10, 11, 12, 13, 14]);
});
