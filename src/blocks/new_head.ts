import * as DateFns from "date-fns";
import PQueue from "p-queue";
import { calcBlockFeeBreakdown } from "../base_fees.js";
import * as BurnRecordsNewHead from "../burn-records/new_head.js";
import * as Contracts from "../contracts/contracts.js";
import * as Duration from "../duration.js";
import * as EthPrices from "../eth-prices/eth_prices.js";
import { Head } from "../eth_node.js";
import { pipe, TAlt, TEAlt } from "../fp.js";
import * as GroupedAnalysis1 from "../grouped_analysis_1.js";
import * as Leaderboards from "../leaderboards.js";
import * as LeaderboardsAll from "../leaderboards_all.js";
import * as LeaderboardsLimitedTimeframe from "../leaderboards_limited_timeframe.js";
import * as Log from "../log.js";
import * as Performance from "../performance.js";
import * as ScarcityCache from "../scarcity/cache.js";
import * as Transactions from "../transactions.js";
import * as Blocks from "./blocks.js";

export const newBlockQueue = new PQueue({
  concurrency: 1,
  autoStart: false,
});

export const rollbackToBefore = async (blockNumber: number): Promise<void> => {
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

    await Contracts.deleteContractsMinedAt(blockNumber);
    await Blocks.deleteContractBaseFees(blockNumber);
    await Blocks.deleteDerivedBlockStats(blockNumber);
    await Blocks.deleteBlock(blockNumber);

    Performance.logPerf("rollback", t0);
  }
};

export const addBlock = async (head: Head): Promise<void> => {
  const t0 = performance.now();
  Log.debug(`add block from new head ${head.number}`);
  const block = await Blocks.getBlockWithRetry(head.number);

  if (head.hash !== block.hash) {
    Log.warn("queued head is no longer valid, skipping");
    return;
  }

  const isParentKnown = await Blocks.getBlockHashIsKnown(block.parentHash);

  if (!isParentKnown) {
    // NOTE: sometimes a new head has a parent never seen before. In this case we rollback to the last known parent, roll back to that block, then roll forwards to the current block.
    Log.warn(
      "new head's parent is not in our DB, rollback one block and try to add the parent",
    );
    await rollbackToBefore(head.number - 1);
    const previousBlock = await Blocks.getBlockWithRetry(head.number - 1);
    await addBlock(previousBlock);
  }

  const syncedBlockHeight = await Blocks.getSyncedBlockHeight();
  if (block.number <= syncedBlockHeight) {
    await rollbackToBefore(block.number);
  }

  const [txrs, ethPrice] = await TAlt.seqTParT(
    () => Transactions.getTxrsWithRetry(block),
    pipe(
      EthPrices.getEthPrice(
        DateFns.fromUnixTime(block.timestamp),
        Duration.millisFromMinutes(5),
      ),
      TEAlt.getOrThrow,
    ),
  )();
  await Blocks.storeBlock(block, txrs, ethPrice.ethusd);

  const feeBreakdown = calcBlockFeeBreakdown(
    block,
    Transactions.segmentTxrs(txrs),
    ethPrice.ethusd,
  );

  const blockDb = Blocks.blockDbFromBlock(block, txrs, ethPrice.ethusd);

  const tStartAnalyze = performance.now();

  LeaderboardsLimitedTimeframe.addBlockForAllTimeframes(
    blockDb,
    feeBreakdown.contract_use_fees,
    feeBreakdown.contract_use_fees_usd!,
  );

  const addToLeaderboardAllTask = () =>
    LeaderboardsAll.addBlock(
      block.number,
      feeBreakdown.contract_use_fees,
      feeBreakdown.contract_use_fees_usd!,
    );

  const addBlockToBurnRecords = Performance.withPerfLogT(
    "add block to burn record all",
    BurnRecordsNewHead.onNewBlock,
  );

  await Promise.all([
    LeaderboardsLimitedTimeframe.removeExpiredBlocksFromSumsForAllTimeframes()(),
    addToLeaderboardAllTask(),
    addBlockToBurnRecords(blockDb)(),
  ]);

  Performance.logPerf("second order analyze block", tStartAnalyze);

  Log.debug(`store block seq queue ${newBlockQueue.size}`);
  const allBlocksProcessed =
    newBlockQueue.size === 0 &&
    // This function is on this queue.
    newBlockQueue.pending <= 1;

  if (allBlocksProcessed) {
    await TAlt.seqTParT(
      Performance.measureTaskPerf(
        "update grouped analysis 1",
        GroupedAnalysis1.updateAnalysis(blockDb),
      ),
      Performance.measureTaskPerf(
        "calc scarcity",
        ScarcityCache.updateScarcityCache(blockDb),
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
