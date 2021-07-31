import * as BaseFees from "./base_fees.js";
import * as Log from "./log.js";
import * as Transactions from "./transactions.js";
import * as eth from "./web3.js";
import Config from "./config.js";
import { sql } from "./db.js";
import * as DisplayProgress from "./display_progress.js";
import { sum } from "./numbers.js";
import A from "fp-ts/lib/Array.js";
import { pipe } from "fp-ts/lib/function.js";
import { BlockBaseFees } from "./base_fees.js";
import PQueue from "p-queue";
import { closeWeb3Ws } from "./web3.js";
import { delay } from "./delay.js";
import * as BaseFeeTotals from "./base_fee_totals.js";

// TODO: update implementation to analyze mainnet after fork block.

const blockAnalysisQueue = new PQueue({ concurrency: 1 });

const analyzeBlock = async (blockNumber: number): Promise<void> => {
  Log.debug(`> analyzing block ${blockNumber}`);

  const block = await eth.getBlock(blockNumber);

  Log.debug(`>> fetching ${block.transactions.length} transaction receipts`);
  const txrs = await Transactions.getTxrs1559(block.transactions)();

  const { contractCreationTxrs, ethTransferTxrs, contractUseTxrs } =
    Transactions.segmentTxrs(txrs);

  const ethTransferFees = pipe(
    ethTransferTxrs,
    A.map((txr) => BaseFees.calcTxrBaseFee(block, txr)),
    sum,
  );

  const contractCreationFees = pipe(
    contractCreationTxrs,
    A.map((txr) => BaseFees.calcTxrBaseFee(block, txr)),
    sum,
  );

  const feePerContract = BaseFees.calcBaseFeePerContract(
    block,
    contractUseTxrs,
  );

  const baseFees: BlockBaseFees = {
    transfers: ethTransferFees,
    contract_use_fees: feePerContract,
    contract_creation_fees: contractCreationFees,
  };

  const baseFeesSum = BaseFees.calcBlockBaseFeeSum(baseFees);

  Log.debug(`>> fees burned for block ${blockNumber} - ${baseFeesSum} wei`);

  if (process.env.ENV === "dev" && process.env.SHOW_PROGRESS !== undefined) {
    DisplayProgress.onBlockAnalyzed();
  }

  await BaseFees.storeBaseFeesForBlock(block, baseFees);
  if (process.env.NO_UPDATE_TOTALS === undefined) {
    BaseFeeTotals.updateTotalsWithFees(baseFees);
  }
  BaseFees.notifyNewBaseFee(block, baseFees);
};

// const blockNumberLondonHardFork = 12965000;
// first ropsten eip1559 block
const blockNumberRopstenFirst1559Block = 10499401;
const monthOfBlocksCount = 196364;

const main = async () => {
  Log.info("> starting gas analysis");
  Log.info(`> chain: ${Config.chain}`);

  // eslint-disable-next-line no-constant-condition
  while (true) {
    await eth.webSocketOpen;

    const latestAnalyzedBlockNumber =
      await BaseFees.getLatestAnalyzedBlockNumber();
    const latestBlock = await eth.getBlock("latest");

    const backstopBlockNumber =
      Config.chain === "ropsten"
        ? blockNumberRopstenFirst1559Block
        : // TODO: London hardfork block number after London hardfork
          latestBlock.number - monthOfBlocksCount;

    // Figure out which blocks we'd like to analyze.
    const blocksMissingCount =
      latestBlock.number - (latestAnalyzedBlockNumber || backstopBlockNumber);

    if (Config.env === "dev" && process.env.SHOW_PROGRESS !== undefined) {
      DisplayProgress.start(blocksMissingCount);
    }

    const blocksToAnalyze = new Array(blocksMissingCount)
      .fill(undefined)
      .map((_, i) => latestBlock.number - i)
      .reverse();

    if (blocksMissingCount === 0) {
      Log.debug("> no new blocks to analyze");
    } else {
      Log.info(`> ${blocksMissingCount} blocks to analyze`);
    }

    await blockAnalysisQueue.addAll(
      blocksToAnalyze.map((blockNumber) => () => analyzeBlock(blockNumber)),
    );

    // Wait 1s before checking for new blocks to analyze
    await delay(2000);
  }
};

main()
  .then(async () => {
    Log.info("> done analyzing gas");
    closeWeb3Ws();
    await sql.end();
  })
  .catch((error) => {
    Log.error("> error analyzing gas", { error });
    throw error;
  });
