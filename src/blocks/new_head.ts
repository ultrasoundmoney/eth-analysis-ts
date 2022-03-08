import PQueue from "p-queue";
import * as BaseFees from "../base_fees.js";
import { sumFeeSegments } from "../base_fees.js";
import * as BurnRecordsNewHead from "../burn-records/new_head.js";
import * as Contracts from "../contracts/contracts.js";
import { sqlTNotify } from "../db.js";
import * as DeflationaryStreaks from "../deflationary_streaks.js";
import * as Duration from "../duration.js";
import * as EthPricesAverages from "../eth-prices/averages.js";
import * as EthPrices from "../eth-prices/eth_prices.js";
import { Head } from "../eth_node.js";
import { NEA, O, pipe, TAlt, TEAlt, TOAlt } from "../fp.js";
import * as GroupedAnalysis1 from "../grouped_analysis_1.js";
import * as Leaderboards from "../leaderboards.js";
import * as LeaderboardsAll from "../leaderboards_all.js";
import * as LeaderboardsLimitedTimeframe from "../leaderboards_limited_timeframe.js";
import * as Log from "../log.js";
import * as Performance from "../performance.js";
import * as ScarcityCache from "../scarcity/cache.js";
import * as Transactions from "../transactions.js";
import * as Blocks from "./blocks.js";

export type BlocksUpdate = {
  number: number;
};

export const newBlockQueue = new PQueue({
  concurrency: 1,
  autoStart: false,
});

export const rollbackToIncluding = async (
  blockNumber: number,
): Promise<void> => {
  Log.info(`rolling back to and including: ${blockNumber}`);
  const syncedBlockHeight = await Blocks.getSyncedBlockHeight();

  const blocksToRollback = Blocks.getBlockRange(blockNumber, syncedBlockHeight);
  const blocksNewestFirst = blocksToRollback.reverse();

  for (const blockNumber of blocksNewestFirst) {
    Log.debug(`rolling back ${blockNumber}`);
    const t0 = performance.now();

    const sumsToRollback = await Leaderboards.getRangeBaseFees(
      blockNumber,
      blockNumber,
    )();
    LeaderboardsLimitedTimeframe.onRollback(blockNumber, sumsToRollback);
    await Promise.all([
      LeaderboardsAll.removeContractBaseFeeSums(sumsToRollback),
      LeaderboardsAll.setNewestIncludedBlockNumber(blockNumber - 1),
      BurnRecordsNewHead.onRollback(blockNumber)(),
    ]);

    await Blocks.deleteContractBaseFees(blockNumber);
    await Contracts.deleteContractsMinedAt(blockNumber);
    await Blocks.deleteDerivedBlockStats(blockNumber);
    await Blocks.deleteBlock(blockNumber);

    Performance.logPerf("rollback", t0);
  }
};

export const addBlock = async (head: Head): Promise<void> => {
  const t0 = performance.now();
  Log.debug(`add block from new head ${head.number}`);
  const oBlock = await Blocks.getBlockByHash(head.hash)();

  if (O.isNone(oBlock)) {
    Log.info("queued head is no longer valid, skipping");
    return;
  }

  const block = oBlock.value;

  const isParentKnown = await Blocks.getBlockHashIsKnown(block.parentHash);

  if (!isParentKnown) {
    // NOTE: sometimes a new head has a parent never seen before. In this case we rollback to the last known parent, roll back to that block, then roll forwards to the current block.
    Log.warn(
      "new head's parent is not in our DB, rollback one block and try to add the parent",
    );
    const rollbackTarget = head.number - 1;
    await rollbackToIncluding(rollbackTarget);
    const previousBlock = await pipe(
      Blocks.getBlockSafe(rollbackTarget),
      TOAlt.getOrThrow(
        `after rolling back, when adding old block ${rollbackTarget}, block came back null`,
      ),
    )();
    await addBlock(previousBlock);
  }

  const syncedBlockHeight = await Blocks.getSyncedBlockHeight();
  if (block.number <= syncedBlockHeight) {
    await rollbackToIncluding(block.number);
  }

  const oTransactionReceipts = await Transactions.getTransactionReceiptsSafe(
    block,
  )();

  if (O.isNone(oTransactionReceipts)) {
    // Block got dropped during transaction receipt fetching or something else went wrong. Either way, we skip this block and figure out on the next head whether we are missing parents.
    Log.info(
      `failed to fetch transaction receipts for head: ${head.hash}, skipping`,
    );
    return;
  }

  const transactionReceipts = oTransactionReceipts.value;

  const ethPrice = await pipe(
    EthPrices.getEthPrice(block.timestamp, Duration.millisFromMinutes(5)),
    TEAlt.getOrThrow,
  )();

  await Blocks.storeBlock(block, transactionReceipts, ethPrice.ethusd);

  const blocksUpdate: BlocksUpdate = { number: block.number };
  await sqlTNotify("blocks-update", JSON.stringify(blocksUpdate))();

  const feeSegments = sumFeeSegments(
    block,
    Transactions.segmentTransactions(transactionReceipts),
    ethPrice.ethusd,
  );

  const tips = BaseFees.calcBlockTips(block, transactionReceipts);

  const blockDb = Blocks.blockDbFromAnalysis(
    block,
    feeSegments,
    tips,
    ethPrice.ethusd,
  );

  const tStartAnalyze = performance.now();

  LeaderboardsLimitedTimeframe.addBlockForAllTimeframes(
    blockDb,
    feeSegments.contractSumsEth,
    feeSegments.contractSumsUsd!,
  );

  const addToLeaderboardAllTask = () =>
    LeaderboardsAll.addBlock(
      block.number,
      feeSegments.contractSumsEth,
      feeSegments.contractSumsUsd!,
    );

  const addBlockToBurnRecords = Performance.withPerfLogT(
    "add block to burn record all",
    BurnRecordsNewHead.onNewBlock,
  );

  await TAlt.seqTPar(
    LeaderboardsLimitedTimeframe.removeExpiredBlocksFromSumsForAllTimeframes(),
    addToLeaderboardAllTask,
    addBlockToBurnRecords(blockDb),
    DeflationaryStreaks.analyzeNewBlocks(NEA.of(blockDb)),
  )();

  Performance.logPerf("second order analyze block", tStartAnalyze);

  Log.debug(`store block seq queue ${newBlockQueue.size}`);
  const allBlocksProcessed =
    newBlockQueue.size === 0 &&
    // This function is on this queue.
    newBlockQueue.pending <= 1;

  if (allBlocksProcessed) {
    await TAlt.seqTPar(
      Performance.measureTaskPerf(
        "update grouped analysis 1",
        GroupedAnalysis1.updateAnalysis(blockDb),
      ),
      Performance.measureTaskPerf(
        "update scarcity",
        ScarcityCache.updateScarcityCache(blockDb),
      ),
      Performance.measureTaskPerf(
        "update average eth prices",
        EthPricesAverages.updateAveragePrices(),
      ),
    )();
  } else {
    Log.debug(
      "more than one block queued for analysis, skipping further computation",
    );
  }
  Performance.logPerf("add block", t0);
};

export const onNewBlock = async (head: Head): Promise<void> =>
  newBlockQueue.add(() => addBlock(head));
