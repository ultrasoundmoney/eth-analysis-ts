import * as Sentry from "@sentry/node";
import Config from "./config.js";
import Koa, { Middleware } from "koa";
import * as Log from "./log.js";
import * as BaseFees from "./base_fees.js";
import Router from "@koa/router";
import { sql } from "./db.js";
import * as EthPrice from "./eth_price.js";
import conditional from "koa-conditional-get";
import etag from "koa-etag";
import { pipe } from "fp-ts/lib/function.js";
import * as A from "fp-ts/lib/Array.js";
import * as EthNode from "./eth_node.js";
import { hexToNumber } from "./hexadecimal.js";
import {
  LeaderboardEntry,
  BurnRates,
  FeesBurned,
  NewBlockPayload,
} from "./base_fees.js";
import * as Blocks from "./blocks.js";
import * as Duration from "./duration.js";
import * as BaseFeeTotals from "./base_fee_totals.js";
import * as Canary from "./canary.js";

if (Config.env !== "dev") {
  Sentry.init({
    dsn: "https://aa7ee1839c7b4ed4993023a300b438de@o920717.ingest.sentry.io/5896640",
    environment: Config.env,
  });
}

const handleGetFeesBurned: Middleware = async (ctx) => {
  ctx.res.setHeader("Cache-Control", "max-age=5, stale-while-revalidate=30");
  ctx.res.setHeader("Content-Type", "application/json");
  ctx.body = { number: cache.number, feesBurned: cache.feesBurned };
};

const handleGetFeesBurnedPerInterval: Middleware = async (ctx) => {
  ctx.res.setHeader(
    "Cache-Control",
    `max-age=6, stale-while-revalidate=${Duration.secondsFromHours(24)}`,
  );
  ctx.res.setHeader("Content-Type", "application/json");
  ctx.body = {
    feesBurnedPerInterval: cache.feesBurnedPerInterval,
    number: cache.number,
  };
};

const handleGetEthPrice: Middleware = async (ctx) => {
  const ethPrice = await EthPrice.getEthPrice();
  ctx.res.setHeader("Cache-Control", "max-age=60, stale-while-revalidate=600");
  ctx.res.setHeader("Content-Type", "application/json");
  ctx.body = ethPrice;
};

const handleGetBurnRate: Middleware = async (ctx) => {
  ctx.res.setHeader("Cache-Control", "max-age=6, stale-while-revalidate=16");
  ctx.res.setHeader("Content-Type", "application/json");
  ctx.body = { burnRates: cache.burnRates, number: cache.number };
};

let latestBlockFees: { fees: number; number: number }[] = [];

const handleGetLatestBlocks: Middleware = async (ctx) => {
  ctx.res.setHeader("Cache-Control", "max-age=6, stale-while-revalidate=16");
  ctx.res.setHeader("Content-Type", "application/json");
  ctx.body = latestBlockFees;
};

const handleGetBaseFeePerGas: Middleware = async (ctx) => {
  ctx.res.setHeader("Cache-Control", "max-age=6, stale-while-revalidate=16");
  ctx.res.setHeader("Content-Type", "application/json");
  ctx.body = { baseFeePerGas: cache.baseFeePerGas };
};

type LeaderboardCache = {
  leaderboard1h: LeaderboardEntry[] | undefined;
  leaderboard24h: LeaderboardEntry[] | undefined;
  leaderboard7d: LeaderboardEntry[] | undefined;
  leaderboard30d: LeaderboardEntry[] | undefined;
  leaderboardAll: LeaderboardEntry[] | undefined;
  number: number | undefined;
};

let leaderboardCache: LeaderboardCache = {
  leaderboard1h: undefined,
  leaderboard24h: undefined,
  leaderboard30d: undefined,
  leaderboard7d: undefined,
  leaderboardAll: undefined,
  number: undefined,
};

const handleGetBurnLeaderboard: Middleware = async (ctx) => {
  ctx.res.setHeader("Cache-Control", "max-age=6, stale-while-revalidate=16");
  ctx.res.setHeader("Content-Type", "application/json");
  ctx.body = leaderboardCache;
};

type Cache = {
  baseFeePerGas?: number;
  burnRates?: BurnRates;
  feesBurned?: Record<keyof FeesBurned, number>;
  feesBurnedPerInterval?: Record<string, number>;
  latestBlockFees?: { fees: number; number: number }[];
  number?: number;
};

const cache: Cache = {
  baseFeePerGas: undefined,
  burnRates: undefined,
  feesBurned: undefined,
  feesBurnedPerInterval: undefined,
  latestBlockFees: undefined,
  number: undefined,
};

const handleGetAll: Middleware = async (ctx) => {
  ctx.res.setHeader("Cache-Control", "max-age=6, stale-while-revalidate=16");
  ctx.res.setHeader("Content-Type", "application/json");
  ctx.body = cache;
};

const updateCachesForBlockNumber = async (
  newLatestBlockNumber: number,
): Promise<void> => {
  const block = await Blocks.getBlockWithRetry(newLatestBlockNumber);

  const [newBurnRates, newFeesBurnedPerInterval] = await Promise.all([
    BaseFees.getBurnRates(),
    BaseFees.getFeesBurnedPerInterval(),
    BaseFees.updateTotalFeeBurnCache(block),
  ]);

  const newTotalFeesBurned = BaseFees.getTotalFeesBurned();

  // There are multiple cases where the new block is not simply the next block from the last we saw.
  // Sometimes a new block has the same number as an old block. Blocks our node sees are not always final.
  // Sometimes the node advances the chain multiple blocks at once.
  // We consider our list of latest blocks out of sync and refetch.
  if (newLatestBlockNumber === (cache.number ?? 0) + 1) {
    // we're in sync, append one
    latestBlockFees.push({
      fees: BaseFees.calcBlockBaseFeeSum(block),
      number: newLatestBlockNumber,
    });

    if (latestBlockFees.length > 10) {
      latestBlockFees = pipe(latestBlockFees, A.takeRight(10));
    }
  } else {
    // we're out of resync, refetch last ten
    const blocksToFetch = Blocks.getBlockRange(
      newLatestBlockNumber - 10,
      newLatestBlockNumber,
    );

    const blocks = await Promise.all(
      blocksToFetch.map(Blocks.getBlockWithRetry),
    );
    latestBlockFees = blocks.map((block) => ({
      fees: BaseFees.calcBlockBaseFeeSum(block),
      number: block.number,
    }));
  }

  cache.baseFeePerGas = hexToNumber(block.baseFeePerGas);
  cache.burnRates = newBurnRates;
  cache.feesBurned = newTotalFeesBurned;
  cache.feesBurnedPerInterval = newFeesBurnedPerInterval;
  cache.latestBlockFees = latestBlockFees;
  cache.number = newLatestBlockNumber;
};

sql.listen("new-block", (payload) => {
  Log.debug("new block update received");
  Canary.resetCanary("block");
  const latestBlock: NewBlockPayload = JSON.parse(payload!);
  updateCachesForBlockNumber(latestBlock.number);
});

type BurnLeaderboardUpdate = {
  number: number;
  leaderboard1h: LeaderboardEntry[];
  leaderboard24h: LeaderboardEntry[];
  leaderboard7d: LeaderboardEntry[];
  leaderboard30d: LeaderboardEntry[];
  leaderboardAll: LeaderboardEntry[];
};

const updateLeaderboardCacheForBlockNumber = async (
  number: number,
): Promise<void> => {
  const leaderboard = await BaseFeeTotals.getNewLeaderboard();

  leaderboardCache = {
    number,
    ...leaderboard,
  };
};

sql.listen("burn-leaderboard-update", async (payload) => {
  Log.debug("new leaderboard update received");
  Canary.resetCanary("leaderboard");

  const update: BurnLeaderboardUpdate = JSON.parse(payload!);

  updateLeaderboardCacheForBlockNumber(update.number);
});

const port = process.env.PORT || 8080;

const app = new Koa();

app.use(async (ctx, next) => {
  ctx.res.setHeader("Access-Control-Allow-Origin", "*");
  await next();
});

app.use(conditional());
app.use(etag());

// usual error handler
app.on("error", (err, ctx) => {
  Sentry.withScope((scope) => {
    scope.addEventProcessor((event) => {
      return Sentry.Handlers.parseRequest(event, ctx.request);
    });
    Sentry.captureException(err);
  });
});

// Health check middleware
app.use(async (ctx, next) => {
  if (ctx.path === "/healthz" || ctx.path === "/health") {
    ctx.res.writeHead(200);
    ctx.res.end();
    return;
  }
  await next();
});

const router = new Router();

router.get("/fees/total-burned", handleGetFeesBurned);
router.get("/fees/burned-per-interval", handleGetFeesBurnedPerInterval);
router.get("/fees/eth-price", handleGetEthPrice);
router.get("/fees/burn-rate", handleGetBurnRate);
router.get("/fees/latest-blocks", handleGetLatestBlocks);
router.get("/fees/base-fee-per-gas", handleGetBaseFeePerGas);
router.get("/fees/burn-leaderboard", handleGetBurnLeaderboard);
router.get("/fees/all", handleGetAll);

app.use(router.routes());
app.use(router.allowedMethods());

const serveFees = async () => {
  try {
    await EthNode.connect();
    const block = await Blocks.getBlockWithRetry("latest");
    await updateCachesForBlockNumber(block.number);
    await updateLeaderboardCacheForBlockNumber(block.number);

    await new Promise((resolve) => {
      app.listen(port, () => {
        resolve(undefined);
      });
    });
    Log.info(`listening on ${port}`);
    Canary.releaseCanary("block");
    Canary.releaseCanary("leaderboard");
  } catch (error) {
    Log.error("serve fees top level error", { error });
    throw error;
  } finally {
    EthNode.closeConnection();
    sql.end();
  }
};

serveFees();

process.on("unhandledRejection", (error) => {
  throw error;
});
