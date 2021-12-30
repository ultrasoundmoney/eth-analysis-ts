import { sql } from "./db.js";
import * as EthNode from "./eth_node.js";
import * as Log from "./log.js";

await EthNode.connect();

await sql<{ hash: string; number: number }[]>`
  SELECT hash, number FROM blocks
`.cursor(1000, async (rows) => {
  for (const row of rows) {
    const block = await EthNode.getBlock(row.number);
    if (row.hash !== block?.hash) {
      throw new Error(
        `found bad block ${row.number}, our hash: ${row.hash}, their hash: ${block?.hash}`,
      );
    }
  }
  Log.debug("1000 blocks checked..");
});
