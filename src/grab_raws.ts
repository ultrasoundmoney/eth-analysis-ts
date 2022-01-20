import { readFileSync, writeFileSync } from "fs";
import { RawHead } from "./eth_node.js";
import * as EthNode from "./eth_node.js";
import * as Transactions from "./transactions.js";
import "./json.js";
import { translateBlock } from "./blocks/blocks.js";

const rawHeadsFileStr = readFileSync("./raw_heads.ndjson", "utf8");
const rawHeadsStrings = rawHeadsFileStr.trimEnd().split("\n");

for (const rawHeadStr of rawHeadsStrings) {
  const rawHead: RawHead = JSON.parse(rawHeadStr);
  const block = await EthNode.getRawBlockByHash(rawHead.hash);

  if (block === null) {
    throw new Error(
      "tried to grab raws, but block from hash came back undefined",
    );
  }

  const txrs = await Transactions.getTxrsWithRetry(translateBlock(block));

  writeFileSync("./raw_blocks.ndjson", JSON.stringify(block));
  writeFileSync("./raw_txrs.ndjson", JSON.stringify(txrs));
}
