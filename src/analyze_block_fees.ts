import * as FeeUse from "./fee_use";
import * as Log from "./log";
import * as Transactions from "./transactions";
import { eth } from "./web3";
import Config from "./config";
import { sql } from "./db";
import type { TxRWeb3London } from "./transactions";
import ProgressBar from "progress";

// const blockNumberFirstOfJulyMainnet = 12738509;
const blockNumberLondonHardFork = 12965000;
const blockNumberFirstOfJulyRopsten = 10543930;
const blockNumberOneWeekAgoRopsten = 10671342;

type TxrSegments = {
  contractCreationTxrs: TxRWeb3London[];
  ethTransferTxrs: TxRWeb3London[];
  contractUseTxrs: TxRWeb3London[];
};

const segmentTxrs = (txrs: readonly TxRWeb3London[]): TxrSegments => {
  const contractUseTxrs: TxRWeb3London[] = [];
  const contractCreationTxrs: TxRWeb3London[] = [];
  const ethTransferTxrs: TxRWeb3London[] = [];

  txrs.forEach((txr) => {
    if (txr.to === null) {
      contractCreationTxrs.push(txr);
    } else if (txr.gasUsed === 21000) {
      ethTransferTxrs.push(txr);
    } else {
      contractUseTxrs.push(txr);
    }
  });

  return { contractCreationTxrs, contractUseTxrs, ethTransferTxrs };
};

// TODO: update implementation to analyze mainnet after fork block.

(async () => {
  const latestAnalyzedBlockNumber = await FeeUse.getLatestAnalyzedBlockNumber();
  const latestBlock = await eth.getBlock("latest");

  // Figure out which blocks we'd like to analyze.
  const blocksMissingCount =
    latestBlock.number -
    (latestAnalyzedBlockNumber || blockNumberOneWeekAgoRopsten);
  const blocksToAnalyze = new Array(blocksMissingCount)
    .fill(undefined)
    .map((_, i) => latestBlock.number - i)
    .reverse();
  Log.info(`> ${blocksMissingCount} blocks to analyze`);

  // On dev show progress fetching blocks.
  let bar: ProgressBar | undefined = undefined;
  if (Config.env === "dev" && process.env.LOG_LEVEL === "INFO") {
    bar = new ProgressBar(">> [:bar] :rate/s :percent :etas", {
      total: blocksToAnalyze.length,
    });
    const timer = setInterval(() => {
      if (bar?.complete) {
        clearInterval(timer);
      }
    }, 100);
  }

  for (const blockNumber of blocksToAnalyze) {
    Log.debug(`> analyzing block ${blockNumber}`);

    // We only know how to analyze 1559 blocks, guard against other blocks.
    if (
      Config.network === "mainnet" &&
      blockNumber < blockNumberLondonHardFork
    ) {
      throw new Error("tried to analyze non-1559 block");
    }

    const block = await eth.getBlock(blockNumber);
    Log.debug(`>> fetched block ${blockNumber}`);
    Log.debug(`>> fetching ${block.transactions.length} transaction receipts`);
    const txrs = await Transactions.getTxrs1559(block.transactions)();

    const { contractCreationTxrs, ethTransferTxrs, contractUseTxrs } =
      segmentTxrs(txrs);

    const ethTransferFees = FeeUse.calcTxrFees(ethTransferTxrs);

    const contractCreationFees = FeeUse.calcTxrFees(contractCreationTxrs);

    const feePerContract = FeeUse.calcContractUseFees(contractUseTxrs);

    const feesPaid: FeeUse.FeesPaid = {
      transfers: ethTransferFees,
      contract_use_fees: feePerContract,
      contract_creation_fees: contractCreationFees,
    };

    const sumFees =
      feesPaid.transfers +
      Object.values(feesPaid.contract_use_fees).reduce(
        (sum, fee) => sum + fee,
        0,
      );
    Log.debug(`>> fees paid for block ${blockNumber} - ${sumFees} ETH`);

    FeeUse.storeFeesPaidForBlock(block.hash, block.number, feesPaid);

    bar?.tick();
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
