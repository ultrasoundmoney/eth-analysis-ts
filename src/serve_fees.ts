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
import { BaseFeeBurner, BurnRates } from "./base_fees.js";

let number = 0;
let totalFeesBurned = 0;

const handleGetFeesBurned: Middleware = async (ctx) => {
  ctx.res.setHeader("Cache-Control", "max-age=6, stale-while-revalidate=16");
  ctx.res.setHeader("Content-Type", "application/json");
  ctx.body = { number, totalFeesBurned };
};

let feesBurnedPerInterval = {};
const handleGetFeesBurnedPerInterval: Middleware = async (ctx) => {
  ctx.res.setHeader("Cache-Control", "max-age=6, stale-while-revalidate=86400");
  ctx.res.setHeader("Content-Type", "application/json");
  ctx.body = { feesBurnedPerInterval, number };
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
  burnRate1d: 0,
  burnRate7d: 0,
  burnRate30d: 0,
  burnRateAll: 0,
};

const handleGetBurnRate: Middleware = async (ctx) => {
  ctx.res.setHeader("Cache-Control", "max-age=6, stale-while-revalidate=16");
  ctx.res.setHeader("Content-Type", "application/json");
  ctx.body = { burnRates, number };
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
    number,
    totalFeesBurned,
  };
};

sql.listen("new-block", async (payload) => {
  Log.debug("new block update received");
  const latestBlock: { number: number } = JSON.parse(payload!);

  number = latestBlock.number;

  const [newBurnRates, newTotalFeesBurned, newFeesBurnedPerInterval] =
    await Promise.all([
      BaseFees.getBurnRates(),
      BaseFees.getTotalFeesBurned(),
      BaseFees.getFeesBurnedPerInterval(),
    ]);

  const block = await eth.getBlock(number);

  burnRates = newBurnRates;
  totalFeesBurned = newTotalFeesBurned;
  feesBurnedPerInterval = newFeesBurnedPerInterval;
  baseFeePerGas = hexToNumber(block.baseFeePerGas);

  // Sometimes a new block has the same number as an old block. These updates are not final! In this case we replace the block in the list instead of pushing it onto the end.
  const existingIndex = latestBlockFees.findIndex(
    (blockFee) => blockFee.number === number,
  );
  if (existingIndex === -1) {
    latestBlockFees.push({ fees: BaseFees.calcBlockBaseFeeSum(block), number });
  } else {
    // We already have this block! Overwrite with new block.
    latestBlockFees[existingIndex] = {
      fees: BaseFees.calcBlockBaseFeeSum(block),
      number,
    };
  }
  if (latestBlockFees.length > 10) {
    latestBlockFees = pipe(latestBlockFees, A.takeRight(10));
  }
});

type BurnLeaderboardUpdate = {
  number: number;
  leaderboard24h: BaseFeeBurner[];
  leaderboard7d: BaseFeeBurner[];
  leaderboard30d: BaseFeeBurner[];
  leaderboardAll: BaseFeeBurner[];
};

sql.listen("burn-leaderboard-update", async (payload) => {
  const update: BurnLeaderboardUpdate = JSON.parse(payload!);

  leaderboardNumber = update.number;
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

const server = app.listen(port, () => {
  Log.info(`listening on ${port}`);
});

startWebSocketServer(server);
