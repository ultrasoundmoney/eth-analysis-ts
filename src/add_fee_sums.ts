import * as Log from "./log.js";
import { sql } from "./db.js";
import * as EthNode from "./eth_node.js";
import ProgressBar from "progress";
import * as Blocks from "./blocks.js";

const addTipsToAnalyzedBlocks = async (): Promise<void> => {
  const blocksMissingBaseFeeSum = await sql<{ number: number }[]>`
    SELECT number FROM blocks
    WHERE base_fee_sum IS NULL
  `.then((rows) => rows.map((row) => row.number));

  Log.info(`adding base fee sums for ${blocksMissingBaseFeeSum.length} blocks`);

  const bar = new ProgressBar("[:bar] :rate/s :percent :etas", {
    total: blocksMissingBaseFeeSum.length,
  });

  for (const blockNumber of blocksMissingBaseFeeSum) {
    const block = await Blocks.getBlockWithRetry(blockNumber);
    const baseFeeSum = Number(block.baseFeePerGas) * block.gasUsed;
    await sql`
      UPDATE blocks
      SET base_fee_sum = ${baseFeeSum}
      WHERE number = ${block.number}
    `;
    bar.tick();
  }
};

addTipsToAnalyzedBlocks()
  .then(async () => {
    Log.info("done adding base fee sums");
    EthNode.closeConnection();
    await sql.end();
  })
  .catch((error) => {
    Log.error("error adding base fee sum", { error });
    throw error;
  });
