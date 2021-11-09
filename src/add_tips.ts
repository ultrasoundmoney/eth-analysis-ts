import ProgressBar from "progress";
import * as BaseFees from "./base_fees.js";
import * as Blocks from "./blocks.js";
import { sql } from "./db.js";
import * as EthNode from "./eth_node.js";
import * as Log from "./log.js";
import * as Transactions from "./transactions.js";

const addTipsToAnalyzedBlocks = async (): Promise<void> => {
  const blocksMissingTips = await sql<{ number: number }[]>`
    SELECT number FROM blocks
    WHERE tips IS NULL
  `.then((rows) => rows.map((row) => row.number));

  Log.info(`adding tips for ${blocksMissingTips.length} blocks`);

  const bar = new ProgressBar("[:bar] :rate/s :percent :etas", {
    total: blocksMissingTips.length,
  });

  for (const blockNumber of blocksMissingTips) {
    const block = await Blocks.getBlockWithRetry(blockNumber);
    const txrs = await Transactions.getTxrsWithRetry(block);
    const tips = BaseFees.calcBlockTips(block, txrs);
    await sql`
      UPDATE blocks
      SET tips = ${tips}
      WHERE number = ${block.number}
    `;
    bar.tick();
  }
};

addTipsToAnalyzedBlocks()
  .then(async () => {
    Log.info("done adding tips");
    EthNode.closeConnection();
    await sql.end();
  })
  .catch((error) => {
    Log.error("error adding tips", { error });
    throw error;
  });
