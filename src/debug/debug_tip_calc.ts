import * as BaseFees from "../base_fees.js";
import * as Blocks from "../blocks/blocks.js";
import { weiToEth, weiToGwei } from "../convert_unit.js";
import * as EthNode from "../eth_node";
import * as Log from "../log";
import {
  transactionReceiptFromRaw,
  TransactionReceiptV1,
} from "../transactions.js";

const main = async () => {
  await EthNode.connect();

  const block = await Blocks.getBlockWithRetry(12965893);
  Log.debug(`block: ${block.number}, base fee per gas: ${block.baseFeePerGas}`);

  const txrs = await Promise.all(
    block.transactions.map((txHash) => EthNode.getTransactionReceipt(txHash)),
  ).then((arr) =>
    arr.reduce(
      (arr: TransactionReceiptV1[], rawTxr) =>
        rawTxr === null ? arr : [...arr, transactionReceiptFromRaw(rawTxr)],
      [],
    ),
  );

  const fees = txrs
    .map((txr) => {
      if (txr === undefined) {
        throw new Error("txr null");
      }

      const baseFee = BaseFees.calcTxrBaseFee(block, txr);

      const tip =
        txr.gasUsed * txr.effectiveGasPrice - txr.gasUsed * block.baseFeePerGas;

      Log.debug(`txr: ${Log.shortenHash(txr.transactionHash)}`);
      Log.debug(`  gas used: ${txr.gasUsed}`);
      Log.debug(`  effective gas price: ${txr.effectiveGasPrice}`);
      Log.debug(`  tip: ${weiToGwei(tip)}`);
      Log.debug(`  base fee: ${weiToGwei(baseFee)}`);
      Log.debug(`  fee: ${weiToEth(baseFee + tip)}`);

      return tip;
    })
    .reduce((sum, num) => sum + num, 0);

  Log.debug(`fees: ${weiToEth(fees)}`);

  EthNode.closeConnection();
};

main();
