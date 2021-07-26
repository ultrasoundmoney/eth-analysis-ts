import * as Log from "./log.js";
import QuickLru from "quick-lru";
import Koa, { Middleware } from "koa";
import * as FeeUse from "./fee_use.js";

const topFeeUserCache = new QuickLru({ maxSize: 1, maxAge: 3600000 });
const topFeeUserCacheKey = "top-fee-users-key";

const handleAnyRequest: Middleware = async (ctx) => {
  // Respond from cache if we can.
  const cTopFeeUsers = topFeeUserCache.get(topFeeUserCacheKey);
  if (cTopFeeUsers !== undefined) {
    ctx.res.writeHead(200, {
      "Content-Type": "application/json",
    });
    ctx.res.end(cTopFeeUsers);
    return;
  }

  const topTenFeeUsers = await FeeUse.getTopTenFeeUsers();

  // Cache the response
  const topTenFeeUsersJson = JSON.stringify(topTenFeeUsers);
  topFeeUserCache.set(topFeeUserCacheKey, topTenFeeUsersJson);

  ctx.res.writeHead(200, { "Content-Type": "application/json" });
  ctx.res.end(topTenFeeUsersJson);
};

const port = process.env.PORT || 8080;

const app = new Koa();

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
