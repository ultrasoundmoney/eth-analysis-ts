import * as Log from "./log.js";
import QuickLru from "quick-lru";
import Koa, { Middleware } from "koa";
import * as BaseFeeBurn from "./base_fee_burn.js";
import type { TimeFrame } from "./base_fee_burn.js";

const milisFromSeconds = (seconds: number) => seconds * 1000;

const topFeeBurnerCache = new QuickLru({
  maxSize: 1,
  maxAge: milisFromSeconds(10),
});

const getIsTimeFrame = (raw: unknown): raw is TimeFrame =>
  raw === "24h" || raw === "7d" || raw === "30d" || raw === "all";

const handleAnyRequest: Middleware = async (ctx) => {
  if (ctx.path !== "/fees/leaderboard") {
    return;
  }

  const timeFrame = ctx.request.query["timeframe"];

  if (!getIsTimeFrame(timeFrame)) {
    ctx.status = 400;
    ctx.body = {
      msg: "missing 'timeframe' query param, one of '24h', '7d', '30d' or 'all'",
    };
    return;
  }

  // Respond from cache if we can.
  const cTopFeeBurners = topFeeBurnerCache.get(timeFrame);
  if (cTopFeeBurners !== undefined) {
    ctx.res.writeHead(200, {
      "Cache-Control": "max-age=5, stale-while-revalidate=18",
      "Content-Type": "application/json",
    });
    ctx.res.end(cTopFeeBurners);
    return;
  }

  const topTenFeeBurners = await BaseFeeBurn.getTopTenFeeBurners(timeFrame);

  // Cache the response
  const topTenFeeBurnersJson = JSON.stringify(topTenFeeBurners);
  topFeeBurnerCache.set(timeFrame, topTenFeeBurnersJson);

  ctx.res.writeHead(200, {
    "Cache-Control": "max-age=5, stale-while-revalidate=18",
    "Content-Type": "application/json",
  });
  ctx.res.end(topTenFeeBurnersJson);
};

const port = process.env.PORT || 8080;

const app = new Koa();

app.use(async (ctx, next) => {
  ctx.res.setHeader("Access-Control-Allow-Origin", "*");
  await next();
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

app.use(handleAnyRequest);

app.listen(port, () => {
  Log.info(`> listening on ${port}`);
});
