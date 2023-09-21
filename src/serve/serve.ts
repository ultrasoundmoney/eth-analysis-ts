import Router from "@koa/router";
import Koa, { Middleware } from "koa";
import bodyParser from "koa-bodyparser";
import conditional from "koa-conditional-get";
import etag from "koa-etag";
import * as ContractsRoutes from "../contracts/routes.js";
import * as Db from "../db.js";
import { O, pipe } from "../fp.js";
import * as Log from "../log.js";
import * as Cache from "./cache.js";

process.on("unhandledRejection", (error) => {
  throw error;
});

await Db.runMigrations();
Log.debug("ran migrations");

const BLOCK_LIFETIME_CACHE_HEADER =
  "public, max-age=6, stale-while-revalidate=120";

const handleGetGroupedAnalysis1: Middleware = async (ctx) => {
  ctx.set("Cache-Control", BLOCK_LIFETIME_CACHE_HEADER);
  ctx.set("Content-Type", "application/json");
  ctx.body = {
    ...Cache.store.groupedAnalysis1Cache,
    feesBurned: Cache.store.groupedAnalysis1Cache.feeBurns,
  };
};

const handleAverageEthPrice: Middleware = async (ctx) => {
  ctx.set("Cache-Control", BLOCK_LIFETIME_CACHE_HEADER);
  ctx.body = Cache.store.averagePricesCache;
  return undefined;
};

const handleGetMarketCaps: Middleware = async (ctx) => {
  pipe(
    Cache.store.oMarketCapsCache,
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
    Cache.store.scarcityCache,
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
  ctx.body = Cache.store.burnCategoriesCache;
};

const handleGetPeRatios: Middleware = async (ctx) => {
  ctx.set(
    "Cache-Control",
    "public, max-age=43200, stale-while-revalidate=82800",
  );
  ctx.set("Content-Type", "application/json");
  ctx.body = Cache.store.peRatiosCache;
};

const handleGetTotalValueSecured: Middleware = (ctx) => {
  pipe(
    Cache.store.oTotalValueSecuredCache,
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
    Cache.store.blockLag,
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
    Cache.store.validatorRewards,
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
    Cache.store.oSupplyProjectionInputs,
    O.match(
      () => {
        ctx.status = 503;
      },
      (supplyProjectionInputs) => {
        ctx.set(
          "Cache-Control",
          "public, max-age=43200, stale-while-revalidate=86400",
        );
        ctx.set("Content-Type", "application/json");
        ctx.body = supplyProjectionInputs;
      },
    ),
  );
};

const handleGetIssuanceBreakdown: Middleware = async (ctx) => {
  pipe(
    Cache.store.oIssuanceBreakdown,
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
    Cache.store.oEthSupplyParts,
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

const handleGetMergeEstimate: Middleware = async (ctx) => {
  pipe(
    Cache.store.oMergeEstimate,
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
  if (
    ctx.path === "/healthz" ||
    ctx.path === "/health" ||
    ctx.path === "/api/fees/healthz"
  ) {
    // Db health check.
    try {
      await Db.checkHealth();
    } catch (e) {
      ctx.res.writeHead(503);
      if (e instanceof Error) {
        ctx.body = { message: e.message };
      }
      ctx.res.end();
      return undefined;
    }

    // Cache health check.
    try {
      Cache.checkHealth();
    } catch (e) {
      ctx.res.writeHead(503);
      if (e instanceof Error) {
        ctx.body = { message: e.message };
      }
      ctx.res.end();
      return undefined;
    }

    // Healthy!
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
