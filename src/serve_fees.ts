import Koa, { Middleware } from "koa";
import {
  requestHandler,
  Sentry,
  tracingMiddleWare,
} from "./serve_fees_sentry.js";
import * as Log from "./log.js";
import * as BaseFees from "./base_fees.js";
import Router from "@koa/router";
import { sql } from "./db.js";
import * as EthPrice from "./eth_price.js";
import conditional from "koa-conditional-get";
import etag from "koa-etag";
import { startWebSocketServer } from "./socket.js";
import { pipe } from "fp-ts/lib/function.js";
import * as A from "fp-ts/lib/Array.js";
import * as eth from "./web3.js";
import { hexToNumber } from "./numbers.js";
import {
  BaseFeeBurner,
  BurnRates,
  FeesBurned,
  NewBlockPayload,
} from "./base_fees.js";
import * as Blocks from "./blocks.js";

let feesBurned: Record<keyof FeesBurned, number> = {
  feesBurned1h: 0,
  feesBurned24h: 0,
  feesBurned7d: 0,
  feesBurned30d: 0,
  feesBurnedAll: 0,
};

const handleGetFeesBurned: Middleware = async (ctx) => {
  ctx.res.setHeader("Cache-Control", "max-age=6, stale-while-revalidate=16");
  ctx.res.setHeader("Content-Type", "application/json");
  ctx.body = { number: latestBlockNumber, feesBurned };
};

let feesBurnedPerInterval = {};
const handleGetFeesBurnedPerInterval: Middleware = async (ctx) => {
  ctx.res.setHeader("Cache-Control", "max-age=6, stale-while-revalidate=86400");
  ctx.res.setHeader("Content-Type", "application/json");
  ctx.body = { feesBurnedPerInterval, number: latestBlockNumber };
};

const handleGetEthPrice: Middleware = async (ctx) => {
  const ethPrice = await EthPrice.getEthPrice();
  ctx.res.setHeader(
    "Cache-Control",
    "max-age=600, stale-while-revalidate=1800",
  );
  ctx.res.setHeader("Content-Type", "application/json");
  ctx.body = ethPrice;
};

let burnRates: BurnRates = {
  burnRate1h: 0,
  burnRate24h: 0,
  burnRate7d: 0,
  burnRate30d: 0,
  burnRateAll: 0,
};

const handleGetBurnRate: Middleware = async (ctx) => {
  ctx.res.setHeader("Cache-Control", "max-age=6, stale-while-revalidate=16");
  ctx.res.setHeader("Content-Type", "application/json");
  ctx.body = { burnRates, number: latestBlockNumber };
};

let latestBlockFees: { fees: number; number: number }[] = [];

const handleGetLatestBlocks: Middleware = async (ctx) => {
  ctx.res.setHeader("Cache-Control", "max-age=6, stale-while-revalidate=16");
  ctx.res.setHeader("Content-Type", "application/json");
  ctx.body = latestBlockFees;
};

let baseFeePerGas = 0;

const handleGetBaseFeePerGas: Middleware = async (ctx) => {
  ctx.res.setHeader("Cache-Control", "max-age=6, stale-while-revalidate=16");
  ctx.res.setHeader("Content-Type", "application/json");
  ctx.body = { baseFeePerGas };
};

let leaderboard1h: BaseFeeBurner[] = [];
let leaderboard24h: BaseFeeBurner[] = [];
let leaderboard7d: BaseFeeBurner[] = [];
let leaderboard30d: BaseFeeBurner[] = [];
let leaderboardAll: BaseFeeBurner[] = [];
// the most recently analyzed block for the leaderboard
let leaderboardNumber = 0;

const handleGetBurnLeaderboard: Middleware = async (ctx) => {
  ctx.res.setHeader("Cache-Control", "max-age=6, stale-while-revalidate=16");
  ctx.res.setHeader("Content-Type", "application/json");
  ctx.body = {
    number: leaderboardNumber,
    leaderboard1h,
    leaderboard24h,
    leaderboard7d,
    leaderboard30d,
    leaderboardAll,
  };
};

const handleGetAll: Middleware = async (ctx) => {
  ctx.res.setHeader("Cache-Control", "max-age=6, stale-while-revalidate=16");
  ctx.res.setHeader("Content-Type", "application/json");
  ctx.body = {
    baseFeePerGas,
    burnRates,
    feesBurnedPerInterval,
    latestBlockFees,
    number: latestBlockNumber,
    feesBurned,
  };
};

let latestBlockNumber: number | undefined = undefined;
let previousLatestBlockNumber: number | undefined = undefined;

const updateCachesForBlockNumber = async (
  newLatestBlockNumber: number,
): Promise<void> => {
  latestBlockNumber = newLatestBlockNumber;
  const [newBurnRates, newTotalFeesBurned, newFeesBurnedPerInterval] =
    await Promise.all([
      BaseFees.getBurnRates(),
      BaseFees.getTotalFeesBurned(),
      BaseFees.getFeesBurnedPerInterval(),
    ]);

  const block = await eth.getBlock(latestBlockNumber);

  burnRates = newBurnRates;
  feesBurned = newTotalFeesBurned;
  feesBurnedPerInterval = newFeesBurnedPerInterval;
  baseFeePerGas = hexToNumber(block.baseFeePerGas);

  // There are multiple cases where the new block is not simply the next block from the last we saw.
  // Sometimes a new block has the same number as an old block. Blocks our node sees are not always final.
  // Sometimes the node advances the chain multiple blocks at once.
  // We consider our list of latest blocks out of sync and refetch.
  if (latestBlockNumber === (previousLatestBlockNumber ?? 0) + 1) {
    // we're in sync, append one
    latestBlockFees.push({
      fees: BaseFees.calcBlockBaseFeeSum(block),
      number: latestBlockNumber,
    });

    if (latestBlockFees.length > 10) {
      latestBlockFees = pipe(latestBlockFees, A.takeRight(10));
    }
  } else {
    // we're out of resync, refetch last ten
    const blocksToFetch = Blocks.getBlockRange(
      latestBlockNumber - 10,
      latestBlockNumber,
    );

    const blocks = await Promise.all(blocksToFetch.map(eth.getBlock));
    latestBlockFees = blocks.map((block) => ({
      fees: BaseFees.calcBlockBaseFeeSum(block),
      number: block.number,
    }));
  }

  previousLatestBlockNumber = block.number;
};

sql.listen("new-block", (payload) => {
  Log.debug("new block update received");
  const latestBlock: NewBlockPayload = JSON.parse(payload!);
  updateCachesForBlockNumber(latestBlock.number);
});

type BurnLeaderboardUpdate = {
  number: number;
  leaderboard1h: BaseFeeBurner[];
  leaderboard24h: BaseFeeBurner[];
  leaderboard7d: BaseFeeBurner[];
  leaderboard30d: BaseFeeBurner[];
  leaderboardAll: BaseFeeBurner[];
};

sql.listen("burn-leaderboard-update", async (payload) => {
  const update: BurnLeaderboardUpdate = JSON.parse(payload!);

  leaderboardNumber = update.number;
  leaderboard1h = update.leaderboard1h;
  leaderboard24h = update.leaderboard24h;
  leaderboard7d = update.leaderboard7d;
  leaderboard30d = update.leaderboard30d;
  leaderboardAll = update.leaderboardAll;
});

const port = process.env.PORT || 8080;

const app = new Koa();

app.use(async (ctx, next) => {
  ctx.res.setHeader("Access-Control-Allow-Origin", "*");
  await next();
});

app.use(requestHandler);
app.use(tracingMiddleWare);
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
  if (ctx.path === "/health") {
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
  await eth.webSocketOpen;
  const block = await eth.getBlock("latest");
  await updateCachesForBlockNumber(block.number);

  await new Promise((resolve) => {
    const server = app.listen(port, () => {
      resolve(undefined);
    });
    startWebSocketServer(server);
  });
  Log.info(`listening on ${port}`);
};

serveFees().catch((error) => {
  Log.error(error);
  throw error;
});
