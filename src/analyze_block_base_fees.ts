import * as BaseFeeBurn from "./base_fee_burn.js";
import * as Log from "./log.js";
import * as Transactions from "./transactions.js";
import { eth } from "./web3.js";
import Config from "./config.js";
import { sql } from "./db.js";
import { BlockTransactionString as BlockWeb3 } from "web3-eth/types/index";
import { BaseFees } from "./base_fee_burn.js";
import * as DisplayProgress from "./display_progress.js";

// const blockNumberFirstOfJulyMainnet = 12738509;
const blockNumberLondonHardFork = 12965000;
const blockNumberFirstOfJulyRopsten = 10543930;
const blockNumberOneWeekAgoRopsten = 10671342;

type BlockWeb3London = BlockWeb3 & {
  baseFeePerGas: string;
};

// TODO: update implementation to analyze mainnet after fork block.

(async () => {
  const latestAnalyzedBlockNumber =
    await BaseFeeBurn.getLatestAnalyzedBlockNumber();
  const latestBlock = await eth.getBlock("latest");

  // Figure out which blocks we'd like to analyze.
  const blocksMissingCount =
    latestBlock.number -
    (latestAnalyzedBlockNumber || blockNumberOneWeekAgoRopsten);

  if (process.env.ENV === "dev" && process.env.SHOW_PROGRESS !== undefined) {
    DisplayProgress.start(blocksMissingCount);
  }

  const blocksToAnalyze = new Array(blocksMissingCount)
    .fill(undefined)
    .map((_, i) => latestBlock.number - i)
    .reverse();
  Log.info(`> ${blocksMissingCount} blocks to analyze`);

  for (const blockNumber of blocksToAnalyze) {
    Log.debug(`> analyzing block ${blockNumber}`);

    // We only know how to analyze 1559 blocks, guard against other blocks.
    if (
      Config.network === "mainnet" &&
      blockNumber < blockNumberLondonHardFork
    ) {
      throw new Error("tried to analyze non-1559 block");
    }

    const block = (await eth.getBlock(blockNumber)) as BlockWeb3London;
    Log.debug(`>> fetched block ${blockNumber}`);
    Log.debug(`>> fetching ${block.transactions.length} transaction receipts`);
    const txrs = await Transactions.getTxrs1559(block.transactions)();

    const { contractCreationTxrs, ethTransferTxrs, contractUseTxrs } =
      Transactions.segmentTxrs(txrs);

    const ethTransferFees = BaseFeeBurn.calcTxrBaseFee(
      block.baseFeePerGas,
      ethTransferTxrs,
    );

    const contractCreationFees = BaseFeeBurn.calcTxrBaseFee(
      block.baseFeePerGas,
      contractCreationTxrs,
    );

    const feePerContract = BaseFeeBurn.calcContractUseBaseFees(
      block.baseFeePerGas,
      contractUseTxrs,
    );

    const baseFees: BaseFees = {
      transfers: ethTransferFees,
      contract_use_fees: feePerContract,
      contract_creation_fees: contractCreationFees,
    };

    const sumFees =
      baseFees.transfers +
      Object.values(baseFees.contract_use_fees).reduce(
        (sum, fee) => sum + fee,
        0,
      );
    Log.debug(`>> fees burned for block ${blockNumber} - ${sumFees} ETH`);

    BaseFeeBurn.storeBaseFeesForBlock(block.hash, block.number, baseFees);

    if (process.env.ENV === "dev" && process.env.SHOW_PROGRESS !== undefined) {
      DisplayProgress.onBlockAnalyzed();
    }
  }
})()
  .then(async () => {
    Log.info("> done analyzing gas");
    // The websocket connection keeps the process from exiting. Alchemy doesn't expose any method to close the connection. We use undocumented values.
    if (
      typeof eth.currentProvider !== "string" &&
      eth.currentProvider !== null &&
      "ws" in eth.currentProvider
    ) {
      (
        eth.currentProvider as { stopHeartbeatAndBackfill: () => void }
      ).stopHeartbeatAndBackfill();
      (
        eth.currentProvider as { ws: { disposeSocket: () => void } }
      ).ws.disposeSocket();
    }
    await sql.end();
  })
  .catch((error) => {
    Log.error("> error analyzing gas", { error });
    throw error;
  });
