import * as Log from "./log.js";
import { sql } from "./db.js";
import * as BaseFees from "./base_fees.js";
import * as Transactions from "./transactions.js";
import * as eth from "./web3.js";
import ProgressBar from "progress";
import { hexToNumber } from "./hexadecimal.js";
import PQueue from "p-queue";
import * as Contracts from "./contracts.js";

const addDataQueue = new PQueue({ concurrency: 8 });

const addDataToBlocks = async (): Promise<void> => {
  const blocksMissingData = await sql<{ number: number }[]>`
    SELECT number FROM base_fees_per_block
    WHERE eth_transfer_sum IS NULL
  `.then((rows) => rows.map((row) => row.number));

  Log.info(`adding new analysis for ${blocksMissingData.length} blocks`);

  const bar = new ProgressBar(":rate/s :percent :etas", {
    renderThrottle: 3000,
    total: blocksMissingData.length,
  });

  const addForBlockNumber = async (number: number): Promise<void> => {
    const block = await eth.getBlock(number);
    const txrs = await Transactions.getTxrsWithRetry(block);
    const feeBreakdown = BaseFees.calcBlockFeeBreakdown(block, txrs);
    const tips = BaseFees.calcBlockTips(block, txrs);
    const ethTransferSum = feeBreakdown.transfers;
    const contractCreationSum = feeBreakdown.contract_creation_fees;
    const baseFeeSum = BaseFees.calcBlockBaseFeeSum(block);
    const gasUsed = block.gasUsed;
    const addresses = Object.keys(feeBreakdown.contract_use_fees);

    await Contracts.insertContracts(addresses);

    await sql`
      UPDATE base_fees_per_block
      SET
        tips = ${tips},
        eth_transfer_sum = ${ethTransferSum},
        contract_creation_sum = ${contractCreationSum},
        base_fee_per_gas = ${hexToNumber(block.baseFeePerGas)},
        base_fee_sum = ${baseFeeSum},
        gas_used = ${gasUsed}
      WHERE number = ${block.number}
    `;

    bar.tick();
  };

  await addDataQueue.addAll(
    blocksMissingData.map((number) => () => addForBlockNumber(number)),
  );
};

addDataToBlocks()
  .then(async () => {
    Log.info("done adding new analysis");
    eth.closeWeb3Ws();
    await sql.end();
  })
  .catch((error) => {
    Log.error("error adding tips", { error });
    throw error;
  });
