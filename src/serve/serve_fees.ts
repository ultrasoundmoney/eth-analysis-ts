import Router from "@koa/router";
import * as Sentry from "@sentry/node";
import Koa, { Middleware } from "koa";
import bodyParser from "koa-bodyparser";
import conditional from "koa-conditional-get";
import etag from "koa-etag";
import { setInterval } from "timers/promises";
import { FeesBurnedT } from "../base_fee_sums.js";
import * as Blocks from "../blocks/blocks.js";
import { NewBlockPayload } from "../blocks/blocks.js";
import { BurnRatesT } from "../burn_rates.js";
import * as Canary from "../canary.js";
import * as Config from "../config.js";
import * as Contracts from "../contracts.js";
import { runMigrations, sql } from "../db.js";
import * as DerivedBlockStats from "../derived_block_stats.js";
import * as Duration from "../duration.js";
import * as EthPrices from "../eth_prices.js";
import * as FeesBurnedPerInterval from "../fees_burned_per_interval.js";
import { pipe, T, TAlt, TE } from "../fp.js";
import * as LatestBlockFees from "../latest_block_fees.js";
import { LeaderboardEntries } from "../leaderboards.js";
import * as Log from "../log.js";
import * as MarketCaps from "../market-caps/market_caps.js";
import * as Scarcity from "../scarcity/scarcity.js";
import { ScarcityT } from "../scarcity/scarcity.js";
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
  ctx.body = { burnRates: cache.burnRates, number: cache.number };
};

const handleGetLatestBlocks: Middleware = async (ctx) => {
  ctx.set("Cache-Control", "max-age=3, stale-while-revalidate=59");
  ctx.set("Content-Type", "application/json");
  ctx.body = cache.latestBlockFees;
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
  ctx.body = cache.leaderboards;
};

const handleGetAll: Middleware = async (ctx) => {
  ctx.set("Cache-Control", "max-age=3, stale-while-revalidate=59");
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

let scarcityCache: ScarcityT | undefined = undefined;
await Scarcity.getLastStoredScarcity();

const handleGetScarcity: Middleware = async (ctx) => {
  if (scarcityCache === undefined) {
    Log.error("scarcity was undefined, but should never be");
    ctx.body = 500;
    return undefined;
  }

  ctx.set("Cache-Control", "max-age=21600, stale-while-revalidate=43200");
  ctx.set("Content-Type", "application/json");
  ctx.body = scarcityCache;
  return undefined;
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

const everyMinuteIterator = setInterval(
  Duration.millisFromMinutes(1),
  Date.now(),
);

const updateCachesEveryMinute = async () => {
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  for await (const _ of everyMinuteIterator) {
    const lastScarcity = await Scarcity.getLastStoredScarcity();
    scarcityCache = lastScarcity;
  }
};

updateCachesEveryMinute();

const updateCachesForBlockNumber = (blockNumber: number) =>
  pipe(
    TAlt.seqSParT({
      derivedBlockStats: () =>
        DerivedBlockStats.getDerivedBlockStats(blockNumber),
      latestBlockFees: LatestBlockFees.getLatestBlockFees(blockNumber),
      baseFeePerGas: Blocks.getBaseFeesPerGas(blockNumber),
    }),
    T.map(({ derivedBlockStats, latestBlockFees, baseFeePerGas }) => {
      cache = {
        baseFeePerGas,
        burnRates: derivedBlockStats?.burnRates ?? undefined,
        feesBurned: derivedBlockStats?.feesBurned ?? undefined,
        latestBlockFees,
        leaderboards: derivedBlockStats?.leaderboards ?? undefined,
        number: blockNumber,
      };
    }),
  );

sql.listen("new-derived-stats", (payload) => {
  Canary.resetCanary("block");
  const latestBlock: NewBlockPayload = JSON.parse(payload!);
  Log.debug(`derived stats available for block: ${latestBlock.number}`);
  updateCachesForBlockNumber(latestBlock.number)();
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

app.use(bodyParser());
app.use(router.routes());
app.use(router.allowedMethods());

try {
  await runMigrations();

  const blockNumber = await sql<{ blockNumber: number }[]>`
      SELECT block_number FROM derived_block_stats
      ORDER BY block_number DESC
      LIMIT 1
    `.then((rows) => rows[0]?.blockNumber);

  if (blockNumber === undefined) {
    throw new Error("no derived block stats, can't serve fees");
  }

  await updateCachesForBlockNumber(blockNumber)();

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
