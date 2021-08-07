import * as eth from "./web3";
import * as Log from "./log";
import { hexToNumber, weiToEth, weiToGwei } from "./numbers";
import * as BaseFees from "./base_fees.js";

(async () => {
  await eth.webSocketOpen;

  const block = await eth.getBlock(12965893);
  Log.debug(`block: ${block.number}, base fee per gas: ${block.baseFeePerGas}`);

  const txrs = await Promise.all(
    block.transactions.map((txHash) => eth.getTransactionReceipt(txHash)),
  );

  const fees = txrs
    .map((txr) => {
      if (txr === undefined) {
        throw new Error("txr null");
      }

      const baseFee = BaseFees.calcTxrBaseFee(block, txr);

      const tip =
        txr.gasUsed * hexToNumber(txr.effectiveGasPrice) -
        txr.gasUsed * hexToNumber(block.baseFeePerGas);

      Log.debug(`txr: ${Log.shortenHash(txr.transactionHash)}`);
      Log.debug(`  gas used: ${txr.gasUsed}`);
      Log.debug(`  effective gas price: ${hexToNumber(txr.effectiveGasPrice)}`);
      Log.debug(`  tip: ${weiToGwei(tip)}`);
      Log.debug(`  base fee: ${weiToGwei(baseFee)}`);
      Log.debug(`  fee: ${weiToEth(baseFee + tip)}`);

      return tip;
    })
    .reduce((sum, num) => sum + num, 0);

  Log.debug(`fees: ${weiToEth(fees)}`);

  eth.closeWeb3Ws();
})();
