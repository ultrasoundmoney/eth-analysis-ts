import Router from "@koa/router";
import * as Sentry from "@sentry/node";
import Koa, { Middleware } from "koa";
import bodyParser from "koa-bodyparser";
import conditional from "koa-conditional-get";
import etag from "koa-etag";
import * as Blocks from "../blocks/blocks.js";
import * as BurnRecordsCache from "../burn-records/cache.js";
import { BurnRatesT } from "../burn_rates.js";
import * as Canary from "../canary.js";
import * as Config from "../config.js";
import * as ContractsAdmin from "../contracts/admin.js";
import { runMigrations, sql } from "../db.js";
import * as Duration from "../duration.js";
import * as EthPrices from "../eth_prices.js";
import * as FeesBurnedPerInterval from "../fees_burned_per_interval.js";
import { FeesBurnedT } from "../fee_burns.js";
import { O, pipe, T, TE } from "../fp.js";
import * as GroupedStats1 from "../grouped_stats_1.js";
import { LeaderboardEntries } from "../leaderboards.js";
import * as Log from "../log.js";
import * as MarketCaps from "../market-caps/market_caps.js";
import * as ScarcityCache from "../scarcity/cache.js";
import * as SupplyProjection from "../supply-projection/supply_projection.js";

if (Config.getEnv() !== "dev") {
  Sentry.init({
    dsn: "https://aa7ee1839c7b4ed4993023a300b438de@o920717.ingest.sentry.io/5896640",
    environment: Config.getEnv(),
  });
}

process.on("unhandledRejection", (error) => {
  throw error;
});

const handleGetFeesBurned: Middleware = async (ctx) => {
  ctx.set("Cache-Control", "max-age=5, stale-while-revalidate=30");
  ctx.set("Content-Type", "application/json");
  ctx.body = {
    number: groupedStats1Cache.number,
    feesBurned: groupedStats1Cache.feesBurned,
  };
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

const handleGetEthPrice: Middleware = async (ctx): Promise<void> =>
  pipe(
    EthPrices.getEthStats(),
    TE.match(
      (e) => {
        Log.error("unhandled get eth price error", e);
        ctx.status = 500;
        return;
      },
      (ethStats) => {
        ctx.set("Cache-Control", "max-age=15, stale-while-revalidate=600");
        ctx.set("Content-Type", "application/json");
        ctx.body = ethStats;
        return undefined;
      },
    ),
  )();

const handleGetBurnRate: Middleware = async (ctx) => {
  ctx.set("Cache-Control", "max-age=3, stale-while-revalidate=59");
  ctx.set("Content-Type", "application/json");
  ctx.body = {
    burnRates: groupedStats1Cache.burnRates,
    number: groupedStats1Cache.number,
  };
};

const handleGetLatestBlocks: Middleware = async (ctx) => {
  ctx.set("Cache-Control", "max-age=3, stale-while-revalidate=59");
  ctx.set("Content-Type", "application/json");
  ctx.body = groupedStats1Cache.latestBlockFees;
};

const handleGetBaseFeePerGas: Middleware = async (ctx) => {
  const baseFeePerGas = await Blocks.getLatestBaseFeePerGas()();
  ctx.set("Cache-Control", "max-age=3, stale-while-revalidate=59");
  ctx.set("Content-Type", "application/json");
  ctx.body = { baseFeePerGas };
};

const handleGetBurnLeaderboard: Middleware = async (ctx) => {
  ctx.set("Cache-Control", "max-age=3, stale-while-revalidate=59");
  ctx.set("Content-Type", "application/json");
  ctx.body = groupedStats1Cache.leaderboards;
};

const handleGetAll: Middleware = async (ctx) => {
  ctx.set("Cache-Control", "max-age=3, stale-while-revalidate=59");
  ctx.set("Content-Type", "application/json");
  ctx.body = groupedStats1Cache;
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

const handleAverageEthPrice: Middleware = async (ctx) => {
  const averageEthPrice = await EthPrices.getAveragePrice()();
  ctx.set("Cache-Control", "max-age=3, stale-while-revalidate=6");
  ctx.body = averageEthPrice;
  return undefined;
};

const handleGetMarketCaps: Middleware = async (ctx) =>
  pipe(
    () => MarketCaps.getStoredMarketCaps(),
    T.map((marketCaps) => {
      ctx.set("Cache-Control", "max-age=30, stale-while-revalidate=600");
      ctx.set("Content-Type", "application/json");
      ctx.body = marketCaps;
    }),
  )();

const handleGetScarcity: Middleware = (ctx) => {
  pipe(
    scarcityCache,
    O.match(
      () => {
        ctx.status = 503;
      },
      (scarcity) => {
        ctx.set("Cache-Control", "max-age=21600, stale-while-revalidate=43200");
        ctx.set("Content-Type", "application/json");
        ctx.body = scarcity;
      },
    ),
  );
};

const handleGetSupplyProjectionInputs: Middleware = async (ctx) => {
  await pipe(
    SupplyProjection.getInputs(),
    TE.match(
      (e) => {
        Log.error("unhandled get supply projection inputs error", e);
        ctx.status = 500;
      },
      (inputs) => {
        ctx.set("Cache-Control", "max-age=43200, stale-while-revalidate=86400");
        ctx.set("Content-Type", "application/json");
        ctx.body = inputs;
      },
    ),
  )();
};

const handleGetBurnRecords: Middleware = async (ctx) => {
  ctx.set("Cache-Control", "max-age=4, stale-while-revalidate=60");
  ctx.set("Content-Type", "application/json");
  ctx.body = burnRecordsCache;
};

sql.listen("cache-update", async (payload) => {
  Log.debug(`DB notify cache-update, cache key: ${payload}`);

  if (payload === undefined) {
    Log.error("DB cache-update with no payload, skipping");
    return;
  }

  if (payload === BurnRecordsCache.burnRecordsCacheKey) {
    burnRecordsCache = await BurnRecordsCache.getRecordsCache()();
    return;
  }

  if (payload === ScarcityCache.scarcityCacheKey) {
    scarcityCache = await ScarcityCache.getScarcityCache()();
    return;
  }

  if (payload === GroupedStats1.groupedStats1Key) {
    groupedStats1Cache = await GroupedStats1.getLatestStats()();
    return;
  }

  Log.error(`DB cache-update but did not recognize key ${payload}`);
});

const port = process.env.PORT || 8080;

const app = new Koa();

app.on("error", (err, ctx) => {
  Log.error("unhandled serve fees error", err);
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
router.get("/fees/burn-rate", handleGetBurnRate);
router.get("/fees/latest-blocks", handleGetLatestBlocks);
router.get("/fees/base-fee-per-gas", handleGetBaseFeePerGas);
router.get("/fees/burn-leaderboard", handleGetBurnLeaderboard);
router.get("/fees/all", handleGetAll);
router.get("/fees/set-contract-twitter-handle", handleSetContractTwitterHandle);
router.get("/fees/set-contract-name", handleSetContractName);
router.get("/fees/set-contract-category", handleSetContractCategory);
router.get("/fees/average-eth-price", handleAverageEthPrice);
router.get("/fees/market-caps", handleGetMarketCaps);
router.get("/fees/scarcity", handleGetScarcity);
router.get("/fees/supply-projection-inputs", handleGetSupplyProjectionInputs);
router.get("/fees/burn-records", handleGetBurnRecords);

app.use(bodyParser());
app.use(router.routes());
app.use(router.allowedMethods());

await runMigrations();

const blockNumberOnStart = await sql<{ blockNumber: number }[]>`
      SELECT block_number FROM derived_block_stats
      ORDER BY block_number DESC
      LIMIT 1
    `.then((rows) => rows[0]?.blockNumber);

if (blockNumberOnStart === undefined) {
  throw new Error("no derived block stats, can't serve fees");
}

let burnRecordsCache = await BurnRecordsCache.getRecordsCache()();
let scarcityCache = await ScarcityCache.getScarcityCache()();
let groupedStats1Cache = await GroupedStats1.getLatestStats()();

await new Promise((resolve) => {
  app.listen(port, () => {
    resolve(undefined);
  });
});

Log.info(`listening on ${port}`);
Canary.releaseCanary("block");
