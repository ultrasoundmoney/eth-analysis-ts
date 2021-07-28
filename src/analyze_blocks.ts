import * as BaseFeeBurn from "./base_fee_burn.js";
import * as Log from "./log.js";
import * as Transactions from "./transactions.js";
import { eth } from "./web3.js";
import Config from "./config.js";
import { sql } from "./db.js";
import { BlockTransactionString as BlockWeb3 } from "web3-eth/types/index";
import { BaseFees } from "./base_fee_burn.js";
import * as DisplayProgress from "./display_progress.js";
import { hexToNumber, sum } from "./numbers.js";
import A from "fp-ts/lib/Array.js";
import { pipe } from "fp-ts/lib/function";

// const blockNumberFirstOfJulyMainnet = 12738509;
const blockNumberLondonHardFork = 12965000;
// ~21 July
const blockNumberOneWeekAgoRopsten = 10677000;
// ~21 July
const blockNumberOneWeekAgo = 12870000;

type BlockWeb3London = BlockWeb3 & {
  baseFeePerGas: string;
};

// TODO: update implementation to analyze mainnet after fork block.

(async () => {
  Log.info("> starting gas analysis");
  Log.info(`> chain: ${Config.chain}`);

  const latestAnalyzedBlockNumber =
    await BaseFeeBurn.getLatestAnalyzedBlockNumber();
  const latestBlock = await eth.getBlock("latest");

  const backstopBlockNumber =
    Config.chain === "ropsten"
      ? blockNumberOneWeekAgoRopsten
      : blockNumberOneWeekAgo;

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
  Log.info(`> ${blocksMissingCount} blocks to analyze`);

  for (const blockNumber of blocksToAnalyze) {
    Log.debug(`> analyzing block ${blockNumber}`);

    // We only know how to analyze 1559 blocks, guard against other blocks.
    if (Config.chain === "mainnet" && blockNumber < blockNumberLondonHardFork) {
      throw new Error("tried to analyze non-1559 block");
    }

    const block = (await eth.getBlock(blockNumber)) as BlockWeb3London;
    Log.debug(`>> fetched block ${blockNumber}`);
    Log.debug(`>> fetching ${block.transactions.length} transaction receipts`);
    const txrs = await Transactions.getTxrs1559(block.transactions)();

    const { contractCreationTxrs, ethTransferTxrs, contractUseTxrs } =
      Transactions.segmentTxrs(txrs);

    const ethTransferFees = pipe(
      ethTransferTxrs,
      A.map((txr) => BaseFeeBurn.calcTxrBaseFee(block.baseFeePerGas, txr)),
      sum,
    );

    const contractCreationFees = pipe(
      contractCreationTxrs,
      A.map((txr) => BaseFeeBurn.calcTxrBaseFee(block.baseFeePerGas, txr)),
      sum,
    );

    const feePerContract = BaseFeeBurn.calcBaseFeePerContract(
      block.baseFeePerGas,
      contractUseTxrs,
    );

    const baseFees: BaseFees = {
      transfers: ethTransferFees,
      contract_use_fees: feePerContract,
      contract_creation_fees: contractCreationFees,
    };

    const totalBaseFees =
      baseFees.transfers +
      baseFees.contract_creation_fees +
      Object.values(baseFees.contract_use_fees).reduce(
        (sum, fee) => sum + fee,
        0,
      );
    Log.debug(`>> fees burned for block ${blockNumber} - ${totalBaseFees} ETH`);

    const timestampNumber =
      typeof block.timestamp === "string"
        ? hexToNumber(block.timestamp)
        : block.timestamp;

    BaseFeeBurn.storeBaseFeesForBlock({
      hash: block.hash,
      number: block.number,
      baseFees: baseFees,
      minedAt: timestampNumber,
    });

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
