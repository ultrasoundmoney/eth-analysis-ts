import * as Log from "./log.js";
import QuickLru from "quick-lru";
import Koa, { Middleware } from "koa";
import * as GasUse from "./gas_use.js";

const topGasUserCache = new QuickLru({ maxSize: 1, maxAge: 3600000 });
const topGasUserCacheKey = "top-gas-users-key";

const handleAnyRequest: Middleware = async (ctx) => {
  // Respond from cache if we can.
  const cTopGasUsers = topGasUserCache.get(topGasUserCacheKey);
  if (cTopGasUsers !== undefined) {
    ctx.res.writeHead(200, {
      "Content-Type": "application/json",
    });
    ctx.res.end(cTopGasUsers);
    return;
  }

  const topTenGasUsers = await GasUse.getTopTenGasUsers();

  // Cache the response
  const topTenGasUsersJson = JSON.stringify(topTenGasUsers);
  topGasUserCache.set(topGasUserCacheKey, topTenGasUsersJson);

  ctx.res.writeHead(200, { "Content-Type": "application/json" });
  ctx.res.end(topTenGasUsersJson);
};

const port = process.env.PORT || 8080;

const app = new Koa();

app.use(handleAnyRequest);

app.listen(port, () => {
  Log.info(`> listening on ${port}`);
});
