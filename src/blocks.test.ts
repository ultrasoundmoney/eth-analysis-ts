import { test } from "uvu";
import * as assert from "uvu/assert";
import * as Blocks from "../src/blocks.js";
import * as EthNode from "./eth_node.js";

test("getBlockRange", () => {
  const blockRange = Blocks.getBlockRange(10, 14);
  assert.equal(blockRange, [10, 11, 12, 13, 14]);
});

test.after(() => {
  EthNode.closeConnection();
});

test.run();
