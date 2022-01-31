import Router from "@koa/router";
import Koa, { Middleware } from "koa";
import bodyParser from "koa-bodyparser";
import conditional from "koa-conditional-get";
import etag from "koa-etag";
import * as Blocks from "../blocks/blocks.js";
import * as BurnCategories from "../burn-categories/burn_categories.js";
import * as BurnRecordsCache from "../burn-records/cache.js";
import * as ContractsRoutes from "../contracts/routes.js";
import { runMigrations, sql } from "../db.js";
import * as EthPricesAverages from "../eth-prices/averages.js";
import * as EthPrices from "../eth-prices/eth_prices.js";
import { O, pipe, TE } from "../fp.js";
import * as GroupedAnalysis1 from "../grouped_analysis_1.js";
import * as Log from "../log.js";
import * as MarketCaps from "../market-caps/market_caps.js";
import * as ScarcityCache from "../scarcity/cache.js";
import * as SupplyProjection from "../supply-projection/supply_projection.js";

process.on("unhandledRejection", (error) => {
  throw error;
});

await runMigrations();

// Prepare caches before registering routes or even starting the server.
let burnRecordsCache = await BurnRecordsCache.getRecordsCache()();
let scarcityCache = await ScarcityCache.getScarcityCache()();
let groupedAnalysis1Cache = await GroupedAnalysis1.getLatestAnalysis()();
let oMarketCapsCache = await MarketCaps.getStoredMarketCaps()();
let burnCategoriesCache = await BurnCategories.getCategoriesCache()();
let averagePricesCache = await EthPricesAverages.getAveragePricesCache()();

const handleGetFeeBurns: Middleware = async (ctx) => {
  ctx.set("Cache-Control", "max-age=5, stale-while-revalidate=30");
  ctx.set("Content-Type", "application/json");
  ctx.body = {
    number: groupedAnalysis1Cache.number,
    feeBurns: groupedAnalysis1Cache.feeBurns,
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
    burnRates: groupedAnalysis1Cache.burnRates,
    number: groupedAnalysis1Cache.number,
  };
};

const handleGetLatestBlocks: Middleware = async (ctx) => {
  ctx.set("Cache-Control", "max-age=3, stale-while-revalidate=59");
  ctx.set("Content-Type", "application/json");
  ctx.body = groupedAnalysis1Cache.latestBlockFees;
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
  ctx.body = groupedAnalysis1Cache.leaderboards;
};

const handleGetGroupedAnalysis1: Middleware = async (ctx) => {
  ctx.set("Cache-Control", "max-age=3, stale-while-revalidate=59");
  ctx.set("Content-Type", "application/json");
  ctx.body = {
    ...groupedAnalysis1Cache,
    feesBurned: groupedAnalysis1Cache.feeBurns,
  };
};

const handleAverageEthPrice: Middleware = async (ctx) => {
  ctx.set("Cache-Control", "max-age=3, stale-while-revalidate=16");
  ctx.body = averagePricesCache;
  return undefined;
};

const handleGetMarketCaps: Middleware = async (ctx) => {
  pipe(
    oMarketCapsCache,
    O.match(
      () => {
        ctx.status = 503;
      },
      (marketCapsCache) => {
        ctx.set("Cache-Control", "max-age=30, stale-while-revalidate=600");
        ctx.set("Content-Type", "application/json");
        ctx.body = marketCapsCache;
      },
    ),
  );
};

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

  if (payload === GroupedAnalysis1.groupedAnalysis1CacheKey) {
    groupedAnalysis1Cache = await GroupedAnalysis1.getLatestAnalysis()();
    return;
  }

  if (payload === MarketCaps.marketCapsCacheKey) {
    oMarketCapsCache = await MarketCaps.getStoredMarketCaps()();
    return;
  }

  if (payload === BurnCategories.burnCategoriesCacheKey) {
    burnCategoriesCache = await BurnCategories.getCategoriesCache()();
    return;
  }

  if (payload === EthPricesAverages.averagePricesCacheKey) {
    averagePricesCache = await EthPricesAverages.getAveragePricesCache()();
    return;
  }

  Log.error(`DB cache-update but did not recognize key ${payload}`);
});

const handleGetBurnCategories: Middleware = async (ctx) => {
  ctx.set("Cache-Control", "max-age=60, stale-while-revalidate=600");
  ctx.set("Content-Type", "application/json");
  ctx.body = burnCategoriesCache;
};

const port = process.env.PORT || 8080;

const app = new Koa();

app.on("error", (err) => {
  Log.error("unhandled serve fees error", err);
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

router.get("/fees/fee-burns", handleGetFeeBurns);
router.get("/fees/eth-price", handleGetEthPrice);
router.get("/fees/burn-rate", handleGetBurnRate);
router.get("/fees/latest-blocks", handleGetLatestBlocks);
router.get("/fees/base-fee-per-gas", handleGetBaseFeePerGas);
router.get("/fees/burn-leaderboard", handleGetBurnLeaderboard);
// deprecate as soon as frontend is switched over to /fees/grouped-analysis-1
router.get("/fees/all", handleGetGroupedAnalysis1);
router.get("/fees/average-eth-price", handleAverageEthPrice);
router.get("/fees/market-caps", handleGetMarketCaps);
router.get("/fees/scarcity", handleGetScarcity);
router.get("/fees/supply-projection-inputs", handleGetSupplyProjectionInputs);
router.get("/fees/burn-records", handleGetBurnRecords);
router.get("/fees/grouped-analysis-1", handleGetGroupedAnalysis1);
router.get("/fees/burn-categories", handleGetBurnCategories);

ContractsRoutes.registerRoutes(router);

app.use(bodyParser());
app.use(router.routes());
app.use(router.allowedMethods());

await new Promise((resolve) => {
  app.listen(port, () => {
    resolve(undefined);
  });
});

Log.info(`listening on ${port}`);
