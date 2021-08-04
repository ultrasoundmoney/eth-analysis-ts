import PQueue from "p-queue";
import { getBlockRange } from "./base_fees.js";
import { sql } from "./db.js";
import * as eth from "./web3.js";
import * as Log from "./log.js";

const checkAll = async () => {
  const blockNumberRopstenFirst1559Block = 10499401;

  const latestBlock = await eth.getBlock("latest");
  const txrQueue = new PQueue({ concurrency: 16 });
  const blockRange = getBlockRange(
    blockNumberRopstenFirst1559Block,
    latestBlock.number,
  );
  Log.info(`> blocks to check: ${blockRange.length}`);

  for (const blockNumber of blockRange) {
    const block = await eth.getBlock(blockNumber);
    Log.debug(`> checking block: ${blockNumber}`);
    const baseFeePerGas = Number.parseInt(block.baseFeePerGas, 16);
    Log.debug(`> base fee per gas: ${baseFeePerGas}`);
    const txrs = await txrQueue.addAll(
      block.transactions.map(
        (tx) => () =>
          eth.getTransactionReceipt(tx).then((txr) => {
            if (txr === null) {
              throw new Error(`block: ${blockNumber}, got null for txr: ${tx}`);
            }
            return txr;
          }),
      ),
    );
    let blockFeeSum = 0;
    for (const txr of txrs) {
      const fee = txr.gasUsed * baseFeePerGas;
      blockFeeSum += fee;
    }

    Log.debug(`> total fee for block ${blockFeeSum}`);
  }
};

checkAll()
  .then(async () => {
    Log.info("> done checking all blocks");
    eth.closeWeb3Ws();
    await sql.end();
  })
  .catch((error) => {
    Log.error("> error checking block", { error });
    throw error;
  });
