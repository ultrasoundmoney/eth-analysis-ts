import PQueue from "p-queue";
import { performance } from "perf_hooks";
import * as BaseFees from "../base_fees.js";
import { sumFeeSegments } from "../base_fees.js";
import * as BurnRecordsNewHead from "../burn-records/new_head.js";
import * as Contracts from "../contracts/contracts.js";
import * as ContractBaseFees from "../contract_base_fees.js";
import { sqlTNotify } from "../db.js";
import * as DeflationaryStreaks from "../deflationary_streaks.js";
import * as Duration from "../duration.js";
import * as EthPricesAverages from "../eth-prices/averages.js";
import * as EthPrices from "../eth-prices/index.js";
import { Head } from "../execution_node.js";
import { E, flow, NEA, O, OAlt, pipe, T, TAlt, TEAlt, TOAlt } from "../fp.js";
import * as GroupedAnalysis1 from "../grouped_analysis_1.js";
import * as LeaderboardsUnlimitedTimeframe from "../leaderboards_unlimited_time_frames.js";
import * as LeaderboardsLimitedTimeframe from "../leaderboards_limited_timeframe.js";
import * as Log from "../log.js";
import * as Performance from "../performance.js";
import * as ScarcityCache from "../scarcity/cache.js";
import * as Transactions from "../transactions.js";
import * as Blocks from "./blocks.js";

export const headsQueue = new PQueue({
  concurrency: 1,
  autoStart: false,
});

const rollbackBlocks = (
  blocksToRollbackNewestFirst: NEA.NonEmptyArray<Blocks.BlockV1>,
) =>
  pipe(
    T.Do,
    T.apS(
      "rollbackDeflationaryStreaks",
      DeflationaryStreaks.rollbackBlocks(blocksToRollbackNewestFirst, false),
    ),
    T.apS(
      "rollbackDeflationaryBlobStreaks",
      DeflationaryStreaks.rollbackBlocks(blocksToRollbackNewestFirst, true),
    ),
    T.apS(
      "rollbackBurnRecords",
      BurnRecordsNewHead.rollbackBlocks(blocksToRollbackNewestFirst),
    ),
    T.apS(
      "rollbackLeaderboardAll",
      LeaderboardsUnlimitedTimeframe.rollbackBlocks(blocksToRollbackNewestFirst, "all"),
    ),
    T.apS(
      "rollbackLeaderboardSinceMerge",
      LeaderboardsUnlimitedTimeframe.rollbackBlocks(blocksToRollbackNewestFirst, "since_merge"),
    ),
    T.apS(
      "rollbackLeaderboardsLimitedTimeFrames",
      LeaderboardsLimitedTimeframe.rollbackBlocks(blocksToRollbackNewestFirst),
    ),
    T.chain(() =>
      pipe(
        blocksToRollbackNewestFirst,
        T.traverseSeqArray(({ number }) =>
          pipe(
            ContractBaseFees.deleteContractBaseFees(number),
            T.chain(() => Contracts.deleteContractsMinedAt(number)),
            T.chain(() => Blocks.deleteBlock(number)),
          ),
        ),
      ),
    ),
  );

export const rollbackToIncluding = (
  block: Blocks.BlockV1 | Blocks.BlockNodeV2,
) =>
  pipe(
    Blocks.getBlocksFromAndIncluding(block.number),
    T.map(
      flow(
        NEA.fromArray,
        OAlt.getOrThrow("expected blocks to roll back to be one or more"),
        NEA.sort(Blocks.sortDesc),
      ),
    ),
    TAlt.chainFirstLogDebug(
      (blocks) =>
        `rolling back ${blocks.length} blocks to and including: ${block.number}`,
    ),
    T.chain(rollbackBlocks),
  );

export type BlocksUpdate = {
  number: number;
};

const broadcastBlocksUpdate = (block: Blocks.BlockNodeV2) =>
  pipe({ number: block.number }, (blocksUpdate: BlocksUpdate) =>
    sqlTNotify("blocks-update", JSON.stringify(blocksUpdate)),
  );

export const addBlock = async (head: Head): Promise<void> => {
  const t0 = performance.now();
  Log.info(`add block from new head ${head.number}`);
  const t0GetBlock = performance.now();
  const oBlock = await Blocks.getBlockByHash(head.hash)();
  Performance.logPerf("getting the block from the node", t0GetBlock);

  if (O.isNone(oBlock)) {
    Log.info("queued head is no longer valid, skipping");
    return;
  }

  const block = oBlock.value;

  const t0Rollback = performance.now();
  const isParentKnown = await Blocks.getBlockHashIsKnown(block.parentHash);

  // After this step the chain to the current head should be unbroken to the received head.
  if (!isParentKnown) {
    // NOTE: sometimes a new head has a parent never seen before. In this case we drop recursively, then add recursively all parents to get back to the head.
    Log.warn(
      "new head's parent is not in our DB, rollback one block and try to add the parent",
    );
    const parentBlockNumber = head.number - 1;
    const oBlock = await Blocks.getBlock(parentBlockNumber)();

    // We stored a parent, but it's not the one this head expected, rollback the parent.
    if (O.isSome(oBlock)) {
      await rollbackToIncluding(oBlock.value)();
    }

    const previousBlock = await pipe(
      Blocks.getBlockSafe(parentBlockNumber),
      TOAlt.expect(
        `expected block ${parentBlockNumber} to exist on-chain whilst replacing a parent`,
      ),
    )();
    await addBlock(previousBlock);
  }
  Performance.logPerf("missing parent rollback", t0Rollback);

  const t0PresentParentRollback = performance.now();
  // This block rolls back the chain.
  const syncedBlockHeight = await Blocks.getSyncedBlockHeight();
  if (block.number <= syncedBlockHeight) {
    await rollbackToIncluding(block)();
  }
  Performance.logPerf("present parent rollback", t0PresentParentRollback);

  const t0GetTransactions = performance.now();
  Log.info(`getting transactions for block ${block.number}`);

  const transactionReceiptsE = await Transactions.transactionReceiptsFromBlock(
    block,
  )();

  if (E.isLeft(transactionReceiptsE)) {
    // Block got superseded between the time we received the head and finished retrieving all transactions. We stop working on the current head and let the next head guide us to the current on-chain truth.
    Log.warn(
      `failed to fetch transaction receipts for head: ${head.hash}, skipping`,
    );
    return;
  }
  Log.debug("got transactions from node");

  const transactionReceipts = transactionReceiptsE.right;

  Performance.logPerf(
    `get ${transactionReceipts.length} transactions from node`,
    t0GetTransactions,
  );

  const t0EthPrice = performance.now();
  const ethPrice = await pipe(
    EthPrices.getEthPrice(block.timestamp, Duration.millisFromMinutes(5)),
    TEAlt.getOrThrow,
  )();
  Performance.logPerf("getting eth price", t0EthPrice);

  const t0StoreBlock = performance.now();
  await Blocks.storeBlock(block, transactionReceipts, ethPrice.ethusd);
  Performance.logPerf("storing block after node fetch", t0StoreBlock);

  const t0Broadcast = performance.now();
  await broadcastBlocksUpdate(block)();
  Performance.logPerf("broadcast new block", t0Broadcast);

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

  const t0LeaderboardLimitedTimeFrames = performance.now();
  LeaderboardsLimitedTimeframe.addBlockForAllTimeframes(
    blockDb,
    feeSegments.contractSumsEth,
    feeSegments.contractSumsUsd!,
  );
  await LeaderboardsLimitedTimeframe.removeExpiredBlocksFromSumsForAllTimeframes()();
  Performance.logPerf(
    "add block to leaderboard limited time frames",
    t0LeaderboardLimitedTimeFrames,
  );

  await Performance.measurePromisePerf(
    "add block to leaderboard all",
    LeaderboardsUnlimitedTimeframe.addBlock(
      block.number,
      feeSegments.contractSumsEth,
      feeSegments.contractSumsUsd!,
      "all"
    ),
  );

  await Performance.measurePromisePerf(
    "add block to leaderboard since_merge",
    LeaderboardsUnlimitedTimeframe.addBlock(
      block.number,
      feeSegments.contractSumsEth,
      feeSegments.contractSumsUsd!,
      "since_merge"
    ),
  );

  await Performance.measurePromisePerf(
    "add block to burn records",
    BurnRecordsNewHead.onNewBlock(blockDb)(),
  );

  await pipe(
    DeflationaryStreaks.analyzeNewBlocks(NEA.of(blockDb)),
    Performance.measureTaskPerf("DeflationaryStreaks.analyzeNewBlocks"),
  )();

  Log.debug(`heads queue: ${headsQueue.size}`);
  const allBlocksProcessed =
    headsQueue.size === 0 &&
    // This function is on this queue.
    headsQueue.pending <= 1;

  if (allBlocksProcessed) {
    await TAlt.seqTSeq(
      pipe(
        GroupedAnalysis1.updateAnalysis(blockDb),
        Performance.measureTaskPerf("update grouped analysis 1"),
      ),
      pipe(
        ScarcityCache.updateScarcityCache(blockDb),
        Performance.measureTaskPerf("update scarcity"),
      ),
      pipe(
        EthPricesAverages.updateAveragePrices(),
        Performance.measureTaskPerf("update average eth prices"),
      ),
    )();
  } else {
    Log.debug("more than one head queued, skipping some computation");
  }
  Performance.logPerf("completely process new head", t0, "INFO");
};

export const onNewBlock = async (head: Head): Promise<void> =>
  headsQueue.add(() => addBlock(head));
