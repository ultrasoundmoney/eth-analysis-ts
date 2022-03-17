import PQueue from "p-queue";
import * as BaseFees from "../base_fees.js";
import { sumFeeSegments } from "../base_fees.js";
import * as BurnRecordsNewHead from "../burn-records/new_head.js";
import * as Contracts from "../contracts/contracts.js";
import * as ContractBaseFees from "../contract_base_fees.js";
import { sqlTNotify } from "../db.js";
import * as DeflationaryStreaks from "../deflationary_streaks.js";
import * as Duration from "../duration.js";
import * as EthPricesAverages from "../eth-prices/averages.js";
import * as EthPrices from "../eth-prices/eth_prices.js";
import { Head } from "../eth_node.js";
import { flow, NEA, O, OAlt, pipe, T, TAlt, TEAlt, TOAlt } from "../fp.js";
import * as GroupedAnalysis1 from "../grouped_analysis_1.js";
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

export const headsQueue = new PQueue({
  concurrency: 1,
  autoStart: false,
});

export const rollbackToIncluding = (
  block: Blocks.BlockV1 | Blocks.BlockNodeV2,
) =>
  pipe(
    Blocks.getBlocksAfter(block.number),
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
    T.chain((blocksToRollbackNewestFirst) =>
      pipe(
        T.Do,
        T.apS(
          "rollbackDeflationaryStreaks",
          DeflationaryStreaks.rollbackBlocks(blocksToRollbackNewestFirst),
        ),
        T.apS(
          "rollbackBurnRecords",
          BurnRecordsNewHead.rollbackBlocks(blocksToRollbackNewestFirst),
        ),
        T.apS(
          "rollbackLeaderboardsAll",
          LeaderboardsAll.rollbackBlocks(blocksToRollbackNewestFirst),
        ),
        T.apS(
          "rollbackLeaderboardsLimitedTimeFrames",
          LeaderboardsLimitedTimeframe.rollbackBlocks(
            blocksToRollbackNewestFirst,
          ),
        ),
        T.bind("rollbackBlock", () =>
          pipe(
            blocksToRollbackNewestFirst,
            T.traverseSeqArray((block) => async () => {
              const blockNumber = block.number;
              await ContractBaseFees.deleteContractBaseFees(blockNumber);
              await Contracts.deleteContractsMinedAt(blockNumber);
              await Blocks.deleteBlock(blockNumber);
            }),
            TAlt.concatAllVoid,
          ),
        ),
        T.map((): void => undefined),
      ),
    ),
  );

const broadcastBlocksUpdate = (block: Blocks.BlockNodeV2) =>
  pipe({ number: block.number }, (blocksUpdate: BlocksUpdate) =>
    sqlTNotify("blocks-update", JSON.stringify(blocksUpdate)),
  );

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
      TOAlt.getOrThrow(
        `expected block ${parentBlockNumber} to exist on-chain whilst replacing a parent`,
      ),
    )();
    await addBlock(previousBlock);
  }

  // This block rolls back the chain.
  const syncedBlockHeight = await Blocks.getSyncedBlockHeight();
  if (block.number <= syncedBlockHeight) {
    await rollbackToIncluding(block)();
  }

  const oTransactionReceipts = await Transactions.getTransactionReceiptsSafe(
    block,
  )();

  if (O.isNone(oTransactionReceipts)) {
    // Block got superseded between the time we received the head and finished retrieving all transactions. We stop working on the current head and let the next head guide us to the current on-chain truth.
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

  await broadcastBlocksUpdate(block)();

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

  Log.debug(`heads queue: ${headsQueue.size}`);
  const allBlocksProcessed =
    headsQueue.size === 0 &&
    // This function is on this queue.
    headsQueue.pending <= 1;

  if (allBlocksProcessed) {
    await TAlt.seqTSeq(
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
    Log.debug("more than one head queued, skipping some computation");
  }
  Performance.logPerf("add block", t0);
};

export const onNewBlock = async (head: Head): Promise<void> =>
  headsQueue.add(() => addBlock(head));
