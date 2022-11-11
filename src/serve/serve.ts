import Router from "@koa/router";
import Koa, { Middleware } from "koa";
import bodyParser from "koa-bodyparser";
import conditional from "koa-conditional-get";
import etag from "koa-etag";
import * as BeaconRewards from "../beacon_rewards.js";
import * as BlockLag from "../block_lag.js";
import * as BurnCategories from "../burn-categories/burn_categories.js";
import * as ContractsRoutes from "../contracts/routes.js";
import { query, runMigrations, sql } from "../db.js";
import * as EffectiveBalanceSum from "../effective_balance_sum.js";
import * as EthPricesAverages from "../eth-prices/averages.js";
import * as EthSupplyParts from "../eth_supply_parts.js";
import { O, pipe, TO } from "../fp.js";
import * as GroupedAnalysis1 from "../grouped_analysis_1.js";
import * as IssuanceBreakdown from "../issuance_breakdown.js";
import * as KeyValueStore from "../key_value_store.js";
import * as Log from "../log.js";
import * as MarketCaps from "../market-caps/market_caps.js";
import * as MergeEstimate from "../merge_estimate.js";
import * as PeRatios from "../pe_ratios.js";
import * as ScarcityCache from "../scarcity/cache.js";
import * as SupplyProjection from "../supply-projection/supply_projection.js";
import * as TotalValueSecured from "../total-value-secured/total_value_secured.js";

process.on("unhandledRejection", (error) => {
  throw error;
});

await runMigrations();
Log.debug("ran migrations");

// Prepare caches before registering routes or even starting the server.
let scarcityCache = await ScarcityCache.getScarcityCache()();
Log.debug("loaded scarcity cache");
let groupedAnalysis1Cache = await GroupedAnalysis1.getLatestAnalysis()();
Log.debug("loaded grouped analysis cache");
let oMarketCapsCache = await MarketCaps.getStoredMarketCaps()();
Log.debug("loaded market cap cache");
let burnCategoriesCache = await BurnCategories.getCategoriesCache()();
Log.debug("loaded burn categories cache");
let averagePricesCache = await EthPricesAverages.getAveragePricesCache()();
Log.debug("loaded average prices cache");
let peRatiosCache = await PeRatios.getPeRatiosCache()();
Log.debug("loaded pe ratios cache");
let oTotalValueSecuredCache =
  await TotalValueSecured.getCachedTotalValueSecured()();
Log.debug("loaded total value secured cache");
let blockLag = await KeyValueStore.getValue(BlockLag.blockLagCacheKey)();
Log.debug("loaded block lag");
let validatorRewards = await KeyValueStore.getValue(
  BeaconRewards.validatorRewardsCacheKey,
)();
Log.debug("loaded validator rewards");
let oSupplyProjectionInputs = await KeyValueStore.getValue(
  SupplyProjection.supplyProjectionInputsCacheKey,
)();
Log.debug("loaded supply projection inputs");
let oIssuanceBreakdown = await IssuanceBreakdown.getIssuanceBreakdown()();
Log.debug("loaded issuance breakdown");
let oEthSupplyParts = await pipe(
  KeyValueStore.getValueStr(EthSupplyParts.ethSupplyPartsCacheKey),
  TO.alt(() =>
    KeyValueStore.getValueStr(EthSupplyParts.ethSupplyPartsCacheKeyOld),
  ),
)();
Log.debug("loaded total supply");
let effectiveBalanceSum =
  await EffectiveBalanceSum.getLastEffectiveBalanceSum()();
let oMergeEstimate = await KeyValueStore.getValueStr(
  MergeEstimate.MERGE_ESTIMATE_CACHE_KEY,
)();
Log.debug("loaded merge estimate");

const BLOCK_LIFETIME_CACHE_HEADER =
  "public, max-age=6, stale-while-revalidate=120";

const handleGetGroupedAnalysis1: Middleware = async (ctx) => {
  ctx.set("Cache-Control", BLOCK_LIFETIME_CACHE_HEADER);
  ctx.set("Content-Type", "application/json");
  ctx.body = {
    ...groupedAnalysis1Cache,
    feesBurned: groupedAnalysis1Cache.feeBurns,
  };
};

const handleAverageEthPrice: Middleware = async (ctx) => {
  ctx.set("Cache-Control", BLOCK_LIFETIME_CACHE_HEADER);
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
        ctx.set(
          "Cache-Control",
          "public, max-age=30, stale-while-revalidate=600",
        );
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
        ctx.set(
          "Cache-Control",
          "public, max-age=21600, stale-while-revalidate=43200",
        );
        ctx.set("Content-Type", "application/json");
        ctx.body = scarcity;
      },
    ),
  );
};

const handleGetBurnCategories: Middleware = async (ctx) => {
  ctx.set("Cache-Control", "public, max-age=60, stale-while-revalidate=600");
  ctx.set("Content-Type", "application/json");
  ctx.body = burnCategoriesCache;
};

const handleGetPeRatios: Middleware = async (ctx) => {
  ctx.set(
    "Cache-Control",
    "public, max-age=43200, stale-while-revalidate=82800",
  );
  ctx.set("Content-Type", "application/json");
  ctx.body = peRatiosCache;
};

const handleGetTotalValueSecured: Middleware = (ctx) => {
  pipe(
    oTotalValueSecuredCache,
    O.match(
      () => {
        ctx.status = 503;
      },
      (totalValueSecured) => {
        ctx.set(
          "Cache-Control",
          "public, max-age=5, stale-while-revalidate=600",
        );
        ctx.set("Content-Type", "application/json");
        ctx.body = totalValueSecured;
      },
    ),
  );
};

const handleGetBlockLag: Middleware = async (ctx) => {
  pipe(
    blockLag,
    O.match(
      () => {
        ctx.status = 503;
      },
      (blockLag) => {
        ctx.set("Cache-Control", "public, max-age=5");
        ctx.set("Content-Type", "application/json");
        ctx.body = { blockLag };
      },
    ),
  );
};

const handleGetValidatorRewards: Middleware = async (ctx) => {
  pipe(
    validatorRewards,
    O.match(
      () => {
        ctx.status = 503;
      },
      (validatorRewards) => {
        ctx.set(
          "Cache-Control",
          "public, max-age=14400, stale-while-revalidate=86400",
        );
        ctx.set("Content-Type", "application/json");
        ctx.body = validatorRewards;
      },
    ),
  );
};

const handleGetSupplyProjectionInputs: Middleware = async (ctx) => {
  pipe(
    oSupplyProjectionInputs,
    O.match(
      () => {
        ctx.status = 503;
      },
      (validatorRewards) => {
        ctx.set(
          "Cache-Control",
          "public, max-age=43200, stale-while-revalidate=86400",
        );
        ctx.set("Content-Type", "application/json");
        ctx.body = validatorRewards;
      },
    ),
  );
};

const handleGetIssuanceBreakdown: Middleware = async (ctx) => {
  pipe(
    oIssuanceBreakdown,
    O.match(
      () => {
        ctx.status = 503;
      },
      (issuanceBreakdown) => {
        ctx.set(
          "Cache-Control",
          "public, max-age=43200, stale-while-revalidate=86400",
        );
        ctx.set("Content-Type", "application/json");
        ctx.body = issuanceBreakdown;
      },
    ),
  );
};

const handleGetEthSupplyParts: Middleware = async (ctx) => {
  pipe(
    oEthSupplyParts,
    O.match(
      () => {
        ctx.status = 503;
      },
      (ethSupplyParts) => {
        ctx.set("Cache-Control", BLOCK_LIFETIME_CACHE_HEADER);
        ctx.set("Content-Type", "application/json");
        ctx.body = ethSupplyParts;
      },
    ),
  );
};

const handleGetEffectiveBalanceSum: Middleware = async (ctx) => {
  ctx.set("Cache-Control", "public, max-age=300, stale-while-revalidate=1200");
  ctx.set("Content-Type", "application/json");
  ctx.body = effectiveBalanceSum;
};

const handleGetMergeEstimate: Middleware = async (ctx) => {
  pipe(
    oMergeEstimate,
    O.match(
      () => {
        ctx.status = 503;
      },
      (mergeEstimate) => {
        ctx.set("Cache-Control", BLOCK_LIFETIME_CACHE_HEADER);
        ctx.set("Content-Type", "application/json");
        ctx.body = mergeEstimate;
      },
    ),
  );
};

sql.listen("cache-update", async (payload) => {
  Log.debug(`DB notify cache-update, cache key: ${payload}`);

  if (payload === undefined) {
    Log.error("DB cache-update with no payload, skipping");
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

  if (payload === PeRatios.peRatiosCacheKey) {
    peRatiosCache = await PeRatios.getPeRatiosCache()();
    return;
  }

  if (payload === TotalValueSecured.totalValueSecuredCacheKey) {
    oTotalValueSecuredCache =
      await TotalValueSecured.getCachedTotalValueSecured()();
    return;
  }

  if (payload === BlockLag.blockLagCacheKey) {
    blockLag = await KeyValueStore.getValue(BlockLag.blockLagCacheKey)();
    return;
  }

  if (payload === BeaconRewards.validatorRewardsCacheKey) {
    validatorRewards = await KeyValueStore.getValue(
      BeaconRewards.validatorRewardsCacheKey,
    )();
    return;
  }

  if (payload === SupplyProjection.supplyProjectionInputsCacheKey) {
    oSupplyProjectionInputs = await KeyValueStore.getValue(
      SupplyProjection.supplyProjectionInputsCacheKey,
    )();
    return;
  }

  if (payload === IssuanceBreakdown.issuanceBreakdownCacheKey) {
    oIssuanceBreakdown = await IssuanceBreakdown.getIssuanceBreakdown()();
    return;
  }

  if (
    payload === EthSupplyParts.ethSupplyPartsCacheKey ||
    payload === EthSupplyParts.ethSupplyPartsCacheKeyOld
  ) {
    oEthSupplyParts = await pipe(
      KeyValueStore.getValueStr(EthSupplyParts.ethSupplyPartsCacheKey),
      TO.alt(() =>
        KeyValueStore.getValueStr(EthSupplyParts.ethSupplyPartsCacheKeyOld),
      ),
    )();
    return;
  }

  if (payload === EffectiveBalanceSum.EFFECTIVE_BALANCE_SUM_CACHE_KEY) {
    effectiveBalanceSum =
      await EffectiveBalanceSum.getLastEffectiveBalanceSum()();
    return;
  }

  if (payload === MergeEstimate.MERGE_ESTIMATE_CACHE_KEY) {
    oMergeEstimate = await KeyValueStore.getValueStr(
      MergeEstimate.MERGE_ESTIMATE_CACHE_KEY,
    )();
    return;
  }
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

const dbHealthCheck = async () => {
  await query`SELECT 1`;
};

// Health check middleware
app.use(async (ctx, next) => {
  if (
    ctx.path === "/healthz" ||
    ctx.path === "/health" ||
    ctx.path === "/api/fees/healthz"
  ) {
    await dbHealthCheck();
    ctx.res.writeHead(200);
    ctx.res.end();
    return undefined;
  }

  await next();
  return undefined;
});

const router = new Router();

// endpoints updating every block
// /api/fees/all is being used by someone. Should we drop it? Maybe wait until grouped-analysis-1 is migrated to rust side.
router.get("/api/fees/all", handleGetGroupedAnalysis1);
router.get("/api/fees/grouped-analysis-1", handleGetGroupedAnalysis1);
router.get("/api/fees/merge-estimate", handleGetMergeEstimate);

// endpoints with unique update cycle duration
router.get("/api/fees/market-caps", handleGetMarketCaps);
router.get("/api/fees/scarcity", handleGetScarcity);
router.get(
  "/api/fees/supply-projection-inputs",
  handleGetSupplyProjectionInputs,
);
router.get("/api/fees/pe-ratios", handleGetPeRatios);
router.get("/api/fees/total-value-secured", handleGetTotalValueSecured);
router.get("/api/fees/block-lag", handleGetBlockLag);
router.get("/api/fees/issuance-breakdown", handleGetIssuanceBreakdown);
router.get("/api/fees/eth-supply", handleGetEthSupplyParts);
router.get("/api/fees/eth-supply-parts", handleGetEthSupplyParts);
router.get("/api/fees/effective-balance-sum", handleGetEffectiveBalanceSum);

// endpoints for dev

router.get("/api/fees/validator-rewards", handleGetValidatorRewards);

// to be deprecated soon
// deprecate as soon as frontend is switched over to /fees/grouped-analysis-1
router.get("/api/fees/average-eth-price", handleAverageEthPrice);
// when #137 is resolved
router.get("/api/fees/burn-categories", handleGetBurnCategories);

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
