import * as Log from "./log.js";
import { sql } from "./db.js";
import * as BaseFees from "./base_fees.js";
import * as Transactions from "./transactions.js";
import * as eth from "./web3.js";
import ProgressBar from "progress";

const addTipsToAnalyzedBlocks = async (): Promise<void> => {
  const blocksMissingTips = await sql<{ number: number }[]>`
    SELECT number FROM base_fees_per_block
    WHERE tips IS NULL
  `.then((rows) => rows.map((row) => row.number));

  Log.info(`adding tips for ${blocksMissingTips.length} blocks`);

  const bar = new ProgressBar("[:bar] :rate/s :percent :etas", {
    total: blocksMissingTips.length,
  });

  for (const blockNumber of blocksMissingTips) {
    const block = await eth.getBlock(blockNumber);
    const txrs = await Transactions.getTxrsWithRetry(block);
    const tips = BaseFees.calcBlockTips(block, txrs);
    await sql`
      UPDATE base_fees_per_block
      SET tips = ${tips}
      WHERE number = ${block.number}
    `;
    bar.tick();
  }
};

addTipsToAnalyzedBlocks()
  .then(async () => {
    Log.info("done adding tips");
    eth.closeWeb3Ws();
    await sql.end();
  })
  .catch((error) => {
    Log.error("error adding tips", { error });
    throw error;
  });
