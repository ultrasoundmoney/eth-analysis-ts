import Koa, { Middleware } from "koa";
import {
  requestHandler,
  Sentry,
  tracingMiddleWare,
} from "./serve_fees_sentry.js";
import * as Log from "./log.js";
import * as BaseFees from "./base_fees.js";
import Router from "@koa/router";
import { sql } from "./db.js";
import * as EthPrice from "./eth_price.js";
import conditional from "koa-conditional-get";
import etag from "koa-etag";
import { startWebSocketServer } from "./socket.js";

let number = 0;
let totalFeesBurned = 0;

const handleGetFeesBurned: Middleware = async (ctx) => {
  ctx.res.setHeader("Cache-Control", "max-age=4, stale-while-revalidate=8");
  ctx.res.setHeader("Content-Type", "application/json");
  ctx.body = { number, totalFeesBurned };
};

let feesBurnedPerInterval = {};
const handleGetFeesBurnedPerInterval: Middleware = async (ctx) => {
  ctx.res.setHeader("Cache-Control", "max-age=4, stale-while-revalidate=86400");
  ctx.res.setHeader("Content-Type", "application/json");
  ctx.body = { feesBurnedPerInterval, number };
};

const handleGetEthPrice: Middleware = async (ctx) => {
  const ethPrice = await EthPrice.getEthPrice();
  ctx.res.setHeader(
    "Cache-Control",
    "max-age=600, stale-while-revalidate=1800",
  );
  ctx.res.setHeader("Content-Type", "application/json");
  ctx.body = ethPrice;
};

let burnRates = { burnRate1h: 0, burnRate24h: 0 };

const handleGetBurnRate: Middleware = async (ctx) => {
  ctx.res.setHeader("Cache-Control", "max-age=4, stale-while-revalidate=8");
  ctx.res.setHeader("Content-Type", "application/json");
  ctx.body = { burnRates, number };
};

sql.listen("new-block", async (payload) => {
  const latestBlock: { number: number } = JSON.parse(payload!);

  number = latestBlock.number;

  const [newBurnRates, newTotalFeesBurned, newFeesBurnedPerInterval] =
    await Promise.all([
      BaseFees.getBurnRates(),
      BaseFees.getTotalFeesBurned(),
      BaseFees.getFeesBurnedPerInterval(),
    ]);

  burnRates = newBurnRates;
  totalFeesBurned = newTotalFeesBurned;
  feesBurnedPerInterval = newFeesBurnedPerInterval;
});

const port = process.env.PORT || 8080;

const app = new Koa();

app.use(async (ctx, next) => {
  ctx.res.setHeader("Access-Control-Allow-Origin", "*");
  await next();
});

app.use(requestHandler);
app.use(tracingMiddleWare);
app.use(conditional());
app.use(etag());

// usual error handler
app.on("error", (err, ctx) => {
  Sentry.withScope((scope) => {
    scope.addEventProcessor((event) => {
      return Sentry.Handlers.parseRequest(event, ctx.request);
    });
    Sentry.captureException(err);
  });
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

const router = new Router();

router.get("/fees/total-burned", handleGetFeesBurned);
router.get("/fees/burned-per-interval", handleGetFeesBurnedPerInterval);
router.get("/fees/eth-price", handleGetEthPrice);
router.get("/fees/burn-rate", handleGetBurnRate);

app.use(router.routes());
app.use(router.allowedMethods());

const server = app.listen(port, () => {
  Log.info(`listening on ${port}`);
});

startWebSocketServer(server);
