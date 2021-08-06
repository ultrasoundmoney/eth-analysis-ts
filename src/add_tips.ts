import * as Log from "./log.js";
import { sql } from "./db.js";
import * as BaseFees from "./base_fees.js";
import * as Transactions from "./transactions.js";
import * as eth from "./web3.js";

const addTipsToAnalyzedBlocks = async (): Promise<void> => {
  const blocksMissingTips = await sql<{ number: number }[]>`
    SELECT number FROM base_fees_per_block
    WHERE tips IS NULL
  `.then((rows) => rows.map((row) => row.number));

  Log.info(`adding tips for ${blocksMissingTips.length} blocks`);

  for (const blockNumber of blocksMissingTips) {
    const block = await eth.getBlock(blockNumber);
    const txrs = await Transactions.getTxrs1559(block.transactions);
    const tips = BaseFees.calcBlockTips(block, txrs);
    await sql`
      UPDATE base_fees_per_block
      SET tips = ${tips}
      WHERE number = ${block.number}
    `;
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
