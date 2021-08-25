import * as Log from "./log.js";
import { sql } from "./db.js";
import * as BaseFees from "./base_fees.js";
import * as Transactions from "./transactions.js";
import * as eth from "./web3.js";
import ProgressBar from "progress";
import { hexToNumber } from "./hexadecimal.js";
import PQueue from "p-queue";
import * as Contracts from "./contracts.js";
import A from "fp-ts/lib/Array.js";

const buildSqlQueue = new PQueue({ concurrency: 8 });

const addDataToBlocks = async (): Promise<void> => {
  const blocksMissingData = await sql<{ number: number }[]>`
    SELECT number FROM base_fees_per_block
    WHERE eth_transfer_sum IS NULL
  `.then((rows) => rows.map((row) => row.number));

  Log.info(`adding new analysis for ${blocksMissingData.length} blocks`);

  const bar = new ProgressBar("[:bar] :rate/s :percent :etas", {
    total: Math.ceil(blocksMissingData.length / 1000),
    stream: process.stdout,
  });

  const toSqlBits = async (number: number) => {
    const block = await eth.getBlock(number);
    const txrs = await Transactions.getTxrsWithRetry(block);
    const feeBreakdown = BaseFees.calcBlockFeeBreakdown(block, txrs);
    const tips = BaseFees.calcBlockTips(block, txrs);
    const ethTransferSum = feeBreakdown.transfers;
    const contractCreationSum = feeBreakdown.contract_creation_fees;
    const baseFeeSum = BaseFees.calcBlockBaseFeeSum(block);
    const baseFeePerGas = hexToNumber(block.baseFeePerGas);
    const gasUsed = block.gasUsed;
    const addresses = Object.keys(feeBreakdown.contract_use_fees);

    return {
      rowText: `(${block.number}, ${tips}, ${ethTransferSum}, ${contractCreationSum}, ${baseFeeSum}, ${baseFeePerGas}, ${gasUsed})`,
      addresses,
    };
  };

  for (const chunk of A.chunksOf(1000)(blocksMissingData)) {
    const sqlBuildingBar = new ProgressBar("[:bar] :rate/s :percent :etas", {
      total: 1000,
    });
    const bits = await buildSqlQueue.addAll(
      chunk.map(
        (number) => () =>
          toSqlBits(number).then((result) => {
            sqlBuildingBar.tick();
            return result;
          }),
      ),
    );

    const addresses = bits.map((bit) => bit.addresses).flat();
    await Contracts.insertContracts(addresses);

    await sql.unsafe(`
      UPDATE base_fees_per_block
      SET
        tips = value_list.tips,
        eth_transfer_sum = value_list.eth_transfer_sum,
        contract_creation_sum = value_list.contract_creation_sum,
        base_fee_sum = value_list.base_fee_sum,
        base_fee_per_gas = value_list.base_fee_per_gas,
        gas_used = value_list.gas_used
      FROM (
        VALUES ${bits.map((bit) => bit.rowText).join(",")}
      ) AS value_list (number, tips, eth_transfer_sum, contract_creation_sum, base_fee_sum, base_fee_per_gas, gas_used)
      WHERE base_fees_per_block.number = value_list.number
  `);
    bar.tick();
  }
};

addDataToBlocks()
  .then(async () => {
    Log.info("done adding new analysis");
    eth.closeWeb3Ws();
    await sql.end();
  })
  .catch((error) => {
    Log.error("error adding new analysis", { error });
    throw error;
  });
