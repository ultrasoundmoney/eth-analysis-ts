import * as Sentry from "@sentry/node";
import * as Blocks from "./blocks.js";
import * as Canary from "./canary.js";
import * as Contracts from "./contracts.js";
import * as Duration from "./duration.js";
import * as EthNode from "./eth_node.js";
import * as Coingecko from "./coingecko.js";
import * as LatestBlockFees from "./latest_block_fees.js";
import * as Log from "./log.js";
import * as T from "fp-ts/lib/Task.js";
import Config, { getAdminToken } from "./config.js";
import Koa, { Middleware } from "koa";
import Router from "@koa/router";
import conditional from "koa-conditional-get";
import etag from "koa-etag";
import { hexToNumber } from "./hexadecimal.js";
import { pipe } from "fp-ts/lib/function.js";
import { sql } from "./db.js";
import { FeesBurnedT } from "./fees_burned.js";
import { BurnRatesT } from "./burn_rates.js";
import { seqSPar } from "./sequence.js";
import * as DerivedBlockStats from "./derived_block_stats.js";
import { NewBlockPayload } from "./blocks.js";
import { LeaderboardEntries } from "./leaderboards.js";
import * as FeesBurnedPerInterval from "./fees_burned_per_interval.js";

if (Config.env !== "dev") {
  Sentry.init({
    dsn: "https://aa7ee1839c7b4ed4993023a300b438de@o920717.ingest.sentry.io/5896640",
    environment: Config.env,
  });
}

type Cache = {
  baseFeePerGas?: number;
  burnRates?: BurnRatesT;
  feesBurned?: FeesBurnedT;
  feesBurnedPerInterval?: Record<string, number>;
  latestBlockFees?: { fees: number; number: number }[];
  number?: number;
  leaderboards?: LeaderboardEntries;
};

let cache: Cache = {
  baseFeePerGas: undefined,
  burnRates: undefined,
  feesBurned: undefined,
  latestBlockFees: undefined,
  number: undefined,
  leaderboards: undefined,
};

const handleGetFeesBurned: Middleware = async (ctx) => {
  ctx.res.setHeader("Cache-Control", "max-age=5, stale-while-revalidate=30");
  ctx.res.setHeader("Content-Type", "application/json");
  ctx.body = { number: cache.number, feesBurned: cache.feesBurned };
};

const handleGetFeesBurnedPerInterval: Middleware = async (ctx) => {
  const feesBurnedPerInterval =
    await FeesBurnedPerInterval.getFeesBurnedPerInterval();
  ctx.res.setHeader(
    "Cache-Control",
    `max-age=6, stale-while-revalidate=${Duration.secondsFromHours(24)}`,
  );
  ctx.res.setHeader("Content-Type", "application/json");
  ctx.body = {
    feesBurnedPerInterval: feesBurnedPerInterval,
    number: "unknown",
  };
};

const handleGetEthPrice: Middleware = async (ctx) => {
  const { eth } = await Coingecko.getMarketData();
  ctx.res.setHeader("Cache-Control", "max-age=60, stale-while-revalidate=600");
  ctx.res.setHeader("Content-Type", "application/json");
  ctx.body = eth;
};

const handleGetMarketData: Middleware = async (ctx) => {
  const marketData = await Coingecko.getMarketData();
  ctx.res.setHeader("Cache-Control", "max-age=60, stale-while-revalidate=600");
  ctx.res.setHeader("Content-Type", "application/json");
  ctx.body = marketData;
};

const handleGetBurnRate: Middleware = async (ctx) => {
  ctx.res.setHeader("Cache-Control", "max-age=6, stale-while-revalidate=16");
  ctx.res.setHeader("Content-Type", "application/json");
  ctx.body = { burnRates: cache.burnRates, number: cache.number };
};

const handleGetLatestBlocks: Middleware = async (ctx) => {
  ctx.res.setHeader("Cache-Control", "max-age=6, stale-while-revalidate=16");
  ctx.res.setHeader("Content-Type", "application/json");
  ctx.body = cache.latestBlockFees;
};

const handleGetBaseFeePerGas: Middleware = async (ctx) => {
  ctx.res.setHeader("Cache-Control", "max-age=6, stale-while-revalidate=16");
  ctx.res.setHeader("Content-Type", "application/json");
  ctx.body = { baseFeePerGas: cache.baseFeePerGas };
};

const handleGetBurnLeaderboard: Middleware = async (ctx) => {
  ctx.res.setHeader("Cache-Control", "max-age=6, stale-while-revalidate=16");
  ctx.res.setHeader("Content-Type", "application/json");
  ctx.body = cache.leaderboards;
};

const handleGetAll: Middleware = async (ctx) => {
  ctx.res.setHeader("Cache-Control", "max-age=6, stale-while-revalidate=16");
  ctx.res.setHeader("Content-Type", "application/json");
  ctx.body = cache;
};

const handleSetContractTwitterHandle: Middleware = async (ctx) => {
  const token = ctx.query.token;
  if (typeof token !== "string") {
    ctx.status = 400;
    ctx.body = { msg: "missing token param" };
    return;
  }

  if (token !== getAdminToken()) {
    ctx.status = 403;
    ctx.body = { msg: "invalid token" };
    return;
  }

  const handle = ctx.query.handle;
  const address = ctx.query.address;

  if (typeof handle !== "string") {
    ctx.status = 400;
    ctx.body = { msg: "missing handle" };
    return;
  }
  if (typeof address !== "string") {
    ctx.status = 400;
    ctx.body = { msg: "missing address" };
    return;
  }

  await Contracts.setTwitterHandle(address, handle)();
  ctx.status = 200;
};

const updateCachesForBlockNumber = async (newBlock: number): Promise<void> => {
  const block = await Blocks.getBlockWithRetry(newBlock);
  const derivedBlockStats = DerivedBlockStats.getDerivedBlockStats(block);
  const latestBlockFees = LatestBlockFees.getLatestBlockFees(block);
  const baseFeePerGas = hexToNumber(block.baseFeePerGas);
  const number = block.number;

  return pipe(
    seqSPar({
      derivedBlockStats,
      latestBlockFees,
    }),
    T.map(({ derivedBlockStats, latestBlockFees }) => {
      cache = {
        latestBlockFees,
        burnRates: derivedBlockStats?.burnRates,
        number,
        feesBurned: derivedBlockStats?.feesBurned,
        baseFeePerGas,
        leaderboards: derivedBlockStats?.leaderboards,
      };
    }),
    T.map(() => undefined),
  )();
};

sql.listen("new-derived-stats", (payload) => {
  Canary.resetCanary("block");
  const latestBlock: NewBlockPayload = JSON.parse(payload!);
  Log.debug(`derived stats available for block: ${latestBlock.number}`);
  updateCachesForBlockNumber(latestBlock.number);
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
router.get("/fees/market-data", handleGetMarketData);
router.get("/fees/burn-rate", handleGetBurnRate);
router.get("/fees/latest-blocks", handleGetLatestBlocks);
router.get("/fees/base-fee-per-gas", handleGetBaseFeePerGas);
router.get("/fees/burn-leaderboard", handleGetBurnLeaderboard);
router.get("/fees/all", handleGetAll);
router.get("/set-contract-twitter-handle", handleSetContractTwitterHandle);

app.use(router.routes());
app.use(router.allowedMethods());

const serveFees = async () => {
  try {
    await EthNode.connect();
    const blockNumber = await EthNode.getLatestBlockNumber();
    await updateCachesForBlockNumber(blockNumber);

    await new Promise((resolve) => {
      app.listen(port, () => {
        resolve(undefined);
      });
    });
    Log.info(`listening on ${port}`);
    Canary.releaseCanary("block");
  } catch (error) {
    Log.error("serve fees top level error", { error });
    EthNode.closeConnection();
    sql.end();
    throw error;
  }
};

serveFees();

process.on("unhandledRejection", (error) => {
  throw error;
});
