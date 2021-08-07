import * as Log from "./log.js";
import { sql } from "./db.js";
import * as eth from "./web3.js";
import ProgressBar from "progress";

const addTipsToAnalyzedBlocks = async (): Promise<void> => {
  const blocksMissingBaseFeeSum = await sql<{ number: number }[]>`
    SELECT number FROM base_fees_per_block
    WHERE base_fee_sum IS NULL
  `.then((rows) => rows.map((row) => row.number));

  Log.info(`adding base fee sums for ${blocksMissingBaseFeeSum.length} blocks`);

  const bar = new ProgressBar("[:bar] :rate/s :percent :etas", {
    total: blocksMissingBaseFeeSum.length,
  });

  for (const blockNumber of blocksMissingBaseFeeSum) {
    const block = await eth.getBlock(blockNumber);
    const baseFeeSum = Number(block.baseFeePerGas) * block.gasUsed;
    await sql`
      UPDATE base_fees_per_block
      SET base_fee_sum = ${baseFeeSum}
      WHERE number = ${block.number}
    `;
    bar.tick();
  }
};

addTipsToAnalyzedBlocks()
  .then(async () => {
    Log.info("done adding base fee sums");
    eth.closeWeb3Ws();
    await sql.end();
  })
  .catch((error) => {
    Log.error("error adding base fee sum", { error });
    throw error;
  });
