import PQueue from "p-queue";
import { performance } from "perf_hooks";
import * as BaseFees from "../base_fees.js";
import { sumFeeSegments } from "../base_fees.js";
import * as BurnRecordsNewHead from "../burn-records/new_head.js";
import * as Contracts from "../contracts/contracts.js";
import * as ContractBaseFees from "../contract_base_fees.js";
import { closeConnection, sqlTNotify } from "../db.js";
import * as DeflationaryStreaks from "../deflationary_streaks.js";
import * as Duration from "../duration.js";
import * as EthPrices from "../eth-prices/index.js";
import { O, pipe, TAlt, TEAlt } from "../fp.js";
import * as GroupedAnalysis1 from "../grouped_analysis_1.js";
import * as LeaderboardsAll from "../leaderboards_all.js";
import * as LeaderboardsLimitedTimeframe from "../leaderboards_limited_timeframe.js";
import * as Log from "../log.js";
import * as Performance from "../performance.js";
import * as ScarcityCache from "../scarcity/cache.js";
import * as Transactions from "../transactions.js";
import * as Blocks from "../blocks/blocks.js";

const lastStoredBlock = await Blocks.getLastStoredBlock()();
Log.info(`Updating grouped analysis for block: ${lastStoredBlock.number}`);
const oBlock = await Blocks.getBlockByHash(lastStoredBlock.hash)();

if (O.isSome(oBlock)) {
    const block = oBlock.value;
    const transactionReceipts = await Transactions.getTxrsWithRetry(block);
    Log.info("Finished getting transaction receipts");

    const ethPrice = await pipe(
        EthPrices.getEthPrice(block.timestamp, Duration.millisFromMinutes(5)),
        TEAlt.getOrThrow,
    )();

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
    await TAlt.seqTSeq(
        pipe(
            GroupedAnalysis1.updateAnalysis(blockDb),
            Performance.measureTaskPerf("update grouped analysis 1"),
        ),
    )();
    Log.info("Finished updating grouped analysis 1");
    await closeConnection();
}
