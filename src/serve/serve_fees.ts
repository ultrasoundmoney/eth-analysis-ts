import Router from "@koa/router";
import Koa, { Middleware } from "koa";
import bodyParser from "koa-bodyparser";
import conditional from "koa-conditional-get";
import etag from "koa-etag";
import * as Blocks from "../blocks/blocks.js";
import * as BurnRecordsCache from "../burn-records/cache.js";
import * as Canary from "../canary.js";
import * as Config from "../config.js";
import * as ContractsAdmin from "../contracts/admin.js";
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

const handleGetFeesBurned: Middleware = async (ctx) => {
  ctx.set("Cache-Control", "max-age=5, stale-while-revalidate=30");
  ctx.set("Content-Type", "application/json");
  ctx.body = {
    number: groupedAnalysis1Cache.number,
    feesBurned: groupedAnalysis1Cache.feesBurned,
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
  ctx.body = groupedAnalysis1Cache;
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

  await ContractsAdmin.setTwitterHandle(address, handle)();
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

  await ContractsAdmin.setName(address, name)();
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

  await ContractsAdmin.setCategory(address, category)();
  ctx.status = 200;
  return undefined;
};

const handleSetContractLastManuallyVerified: Middleware = async (ctx) => {
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

  const address = ctx.query.address;

  if (typeof address !== "string") {
    ctx.status = 400;
    ctx.body = { msg: "missing address" };
    return undefined;
  }

  await ContractsAdmin.setLastManuallyVerified(address)();
  ctx.status = 200;
  return undefined;
};

const handleAverageEthPrice: Middleware = async (ctx) => {
  const averageEthPrice = await EthPricesAverages.getAveragePrice()();
  ctx.set("Cache-Control", "max-age=4, stale-while-revalidate=16");
  ctx.body = averageEthPrice;
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

  Log.error(`DB cache-update but did not recognize key ${payload}`);
});

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

router.get("/fees/total-burned", handleGetFeesBurned);
router.get("/fees/eth-price", handleGetEthPrice);
router.get("/fees/burn-rate", handleGetBurnRate);
router.get("/fees/latest-blocks", handleGetLatestBlocks);
router.get("/fees/base-fee-per-gas", handleGetBaseFeePerGas);
router.get("/fees/burn-leaderboard", handleGetBurnLeaderboard);
// deprecate as soon as frontend is switched over to /fees/grouped-analysis-1
router.get("/fees/all", handleGetGroupedAnalysis1);
router.get("/fees/set-contract-twitter-handle", handleSetContractTwitterHandle);
router.get("/fees/set-contract-name", handleSetContractName);
router.get("/fees/set-contract-category", handleSetContractCategory);
router.get(
  "/fees/set-contract-last-manually-verified",
  handleSetContractLastManuallyVerified,
);
router.get("/fees/average-eth-price", handleAverageEthPrice);
router.get("/fees/market-caps", handleGetMarketCaps);
router.get("/fees/scarcity", handleGetScarcity);
router.get("/fees/supply-projection-inputs", handleGetSupplyProjectionInputs);
router.get("/fees/burn-records", handleGetBurnRecords);
router.get("/fees/grouped-analysis-1", handleGetGroupedAnalysis1);

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
let groupedAnalysis1Cache = await GroupedAnalysis1.getLatestAnalysis()();
let oMarketCapsCache = await MarketCaps.getStoredMarketCaps()();

await new Promise((resolve) => {
  app.listen(port, () => {
    resolve(undefined);
  });
});

Log.info(`listening on ${port}`);
Canary.releaseCanary("block");
