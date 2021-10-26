import * as Blocks from "./blocks.js";
import * as Canary from "./canary.js";
import * as Coingecko from "./coingecko.js";
import * as Config from "./config.js";
import * as Contracts from "./contracts.js";
import * as Duration from "./duration.js";
import * as LatestBlockFees from "./latest_block_fees.js";
import * as Log from "./log.js";
import * as Sentry from "@sentry/node";
import * as T from "fp-ts/lib/Task.js";
import Koa, { Context, Middleware } from "koa";
import Router from "@koa/router";
import conditional from "koa-conditional-get";
import etag from "koa-etag";
import bodyParser from "koa-bodyparser";
import { pipe } from "fp-ts/lib/function.js";
import { sql } from "./db.js";
import { FeesBurnedT } from "./fees_burned.js";
import { BurnRatesT } from "./burn_rates.js";
import * as DerivedBlockStats from "./derived_block_stats.js";
import { NewBlockPayload } from "./blocks.js";
import { LeaderboardEntries } from "./leaderboards.js";
import * as FeesBurnedPerInterval from "./fees_burned_per_interval.js";
import { seqSParT, TE } from "./fp.js";
import { MarketDataError } from "./coingecko.js";

if (Config.getEnv() !== "dev") {
  Sentry.init({
    dsn: "https://aa7ee1839c7b4ed4993023a300b438de@o920717.ingest.sentry.io/5896640",
    environment: Config.getEnv(),
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
  ctx.set("Cache-Control", "max-age=5, stale-while-revalidate=30");
  ctx.set("Content-Type", "application/json");
  ctx.body = { number: cache.number, feesBurned: cache.feesBurned };
};

const handleGetFeesBurnedPerInterval: Middleware = async (ctx) => {
  const feesBurnedPerInterval =
    await FeesBurnedPerInterval.getFeesBurnedPerInterval();
  ctx.set(
    "Cache-Control",
    `max-age=6, stale-while-revalidate=${Duration.secondsFromHours(24)}`,
  );
  ctx.set("Content-Type", "application/json");
  ctx.body = {
    feesBurnedPerInterval: feesBurnedPerInterval,
    number: "unknown",
  };
};

const handleMarketDataError = (ctx: Context, error: MarketDataError) => {
  switch (error._tag) {
    case "fetch-error": {
      Log.error(String(error.error));
      ctx.status = 500;
      ctx.body = { msg: "coingecko fetch error" };
      return undefined;
    }
    case "bad-response": {
      Log.error(`coingecko bad response, status: ${error.status}`);
      ctx.status = error.status;
      ctx.body = {
        msg: `coingecko bad response, status: ${error.status}`,
      };
      return undefined;
    }
    default: {
      Log.error("unexpected get market data error", error);
      ctx.status = 500;
      ctx.body = {
        msg: "unexpected get market data error",
      };
      return undefined;
    }
  }
};

const handleGetEthPrice: Middleware = async (ctx) =>
  pipe(
    Coingecko.getMarketData(),
    TE.match(
      (error) => handleMarketDataError(ctx, error),
      (marketData) => {
        ctx.set("Cache-Control", "max-age=60, stale-while-revalidate=600");
        ctx.set("Content-Type", "application/json");
        ctx.body = marketData.eth;
      },
    ),
  )();

const handleGetMarketData: Middleware = async (ctx) =>
  pipe(
    Coingecko.getMarketData(),
    TE.match(
      (error) => handleMarketDataError(ctx, error),
      (marketData) => {
        ctx.set("Cache-Control", "max-age=60, stale-while-revalidate=600");
        ctx.set("Content-Type", "application/json");
        ctx.body = marketData;
      },
    ),
  )();

const handleGetBurnRate: Middleware = async (ctx) => {
  ctx.set("Cache-Control", "max-age=6, stale-while-revalidate=16");
  ctx.set("Content-Type", "application/json");
  ctx.body = { burnRates: cache.burnRates, number: cache.number };
};

const handleGetLatestBlocks: Middleware = async (ctx) => {
  ctx.set("Cache-Control", "max-age=6, stale-while-revalidate=16");
  ctx.set("Content-Type", "application/json");
  ctx.body = cache.latestBlockFees;
};

const handleGetBaseFeePerGas: Middleware = async (ctx) => {
  ctx.set("Cache-Control", "max-age=6, stale-while-revalidate=16");
  ctx.set("Content-Type", "application/json");
  ctx.body = { baseFeePerGas: cache.baseFeePerGas };
};

const handleGetBurnLeaderboard: Middleware = async (ctx) => {
  ctx.set("Cache-Control", "max-age=6, stale-while-revalidate=16");
  ctx.set("Content-Type", "application/json");
  ctx.body = cache.leaderboards;
};

const handleGetAll: Middleware = async (ctx) => {
  ctx.set("Cache-Control", "max-age=6, stale-while-revalidate=16");
  ctx.set("Content-Type", "application/json");
  ctx.body = cache;
};

const handleSetContractTwitterHandle: Middleware = async (ctx) => {
  const token = ctx.query.token;
  if (typeof token !== "string") {
    ctx.status = 400;
    ctx.body = { msg: "missing token param" };
    return undefined;
  }

  if (token !== Config.getAdminToken()) {
    ctx.status = 403;
    ctx.body = { msg: "invalid token" };
    return undefined;
  }

  const handle = ctx.query.handle;
  const address = ctx.query.address;

  if (typeof handle !== "string") {
    ctx.status = 400;
    ctx.body = { msg: "missing handle" };
    return undefined;
  }
  if (typeof address !== "string") {
    ctx.status = 400;
    ctx.body = { msg: "missing address" };
    return undefined;
  }

  await Contracts.setTwitterHandle(address, handle)();
  ctx.status = 200;
  return undefined;
};

const handleSetContractName: Middleware = async (ctx) => {
  const token = ctx.query.token;
  if (typeof token !== "string") {
    ctx.status = 400;
    ctx.body = { msg: "missing token param" };
    return undefined;
  }

  if (token !== Config.getAdminToken()) {
    ctx.status = 403;
    ctx.body = { msg: "invalid token" };
    return undefined;
  }

  const name = ctx.query.name;
  const address = ctx.query.address;

  if (typeof name !== "string") {
    ctx.status = 400;
    ctx.body = { msg: "missing name" };
    return undefined;
  }
  if (typeof address !== "string") {
    ctx.status = 400;
    ctx.body = { msg: "missing address" };
    return undefined;
  }

  await Contracts.setName(address, name)();
  ctx.status = 200;
  return undefined;
};

const handleSetContractCategory: Middleware = async (ctx) => {
  const token = ctx.query.token;
  if (typeof token !== "string") {
    ctx.status = 400;
    ctx.body = { msg: "missing token param" };
    return undefined;
  }

  if (token !== Config.getAdminToken()) {
    ctx.status = 403;
    ctx.body = { msg: "invalid token" };
    return undefined;
  }

  const category = ctx.query.category;
  const address = ctx.query.address;

  if (typeof category !== "string") {
    ctx.status = 400;
    ctx.body = { msg: "missing category" };
    return undefined;
  }
  if (typeof address !== "string") {
    ctx.status = 400;
    ctx.body = { msg: "missing address" };
    return undefined;
  }

  await Contracts.setCategory(address, category)();
  ctx.status = 200;
  return undefined;
};

const updateCachesForBlockNumber = async (
  blockNumber: number,
): Promise<void> => {
  const derivedBlockStats = DerivedBlockStats.getDerivedBlockStats(blockNumber);
  const latestBlockFees = LatestBlockFees.getLatestBlockFees(blockNumber);
  const baseFeePerGas = Blocks.getBaseFeesPerGas(blockNumber);
  const number = blockNumber;

  return pipe(
    seqSParT({
      derivedBlockStats,
      latestBlockFees,
      baseFeePerGas,
    }),
    T.map(({ derivedBlockStats, latestBlockFees, baseFeePerGas }) => {
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

app.on("error", (err, ctx) => {
  Log.error(err);
  Sentry.withScope((scope) => {
    scope.addEventProcessor((event) => {
      return Sentry.Handlers.parseRequest(event, ctx.request);
    });
    Sentry.captureException(err);
  });
});

app.use(async (ctx, next) => {
  ctx.set("Access-Control-Allow-Origin", "*");
  await next();
});

app.use(conditional());
app.use(etag());

// Health check middleware
app.use(async (ctx, next) => {
  if (ctx.path === "/healthz" || ctx.path === "/health") {
    ctx.res.writeHead(200);
    ctx.res.end();
    return undefined;
  }
  await next();
  return undefined;
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
router.get("/fees/set-contract-twitter-handle", handleSetContractTwitterHandle);
router.get("/fees/set-contract-name", handleSetContractName);
router.get("/fees/set-contract-category", handleSetContractCategory);

app.use(bodyParser());
app.use(router.routes());
app.use(router.allowedMethods());

const serveFees = async () => {
  try {
    const blockNumber = await sql<{ blockNumber: number }[]>`
      SELECT block_number FROM derived_block_stats
      ORDER BY block_number DESC
      LIMIT 1
    `.then((rows) => rows[0]?.blockNumber);

    if (blockNumber === undefined) {
      throw new Error("missing derived block stats");
    }

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
    sql.end();
    throw error;
  }
};

serveFees();

process.on("unhandledRejection", (error) => {
  throw error;
});
