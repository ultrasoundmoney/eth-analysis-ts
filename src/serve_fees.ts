import Koa, { Middleware } from "koa";
import {
  requestHandler,
  Sentry,
  tracingMiddleWare,
} from "./serve_fees_sentry.js";
import * as Log from "./log.js";
import QuickLRU from "quick-lru";
import * as BaseFees from "./base_fees.js";
import Router from "@koa/router";
import WebSocket from "ws";
const { Server: WebSocketServer } = WebSocket;
import { sql } from "./db.js";
import * as EthPrice from "./eth_price.js";
import * as A from "fp-ts/lib/Array.js";
import { pipe } from "fp-ts/lib/function.js";
import { Socket } from "net";
import conditional from "koa-conditional-get";
import etag from "koa-etag";
import { startWebSocketServer } from "./socket.js";

const milisFromSeconds = (seconds: number) => seconds * 1000;

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

const totalFeesBurnedCache = new QuickLRU<string, string>({
  maxSize: 1,
  maxAge: milisFromSeconds(5),
});
const totalFeesBurnedKey = "total-fees-burned";

const handleGetFeesBurned: Middleware = async (ctx) => {
  const cTotalFeesBurned = totalFeesBurnedCache.get(totalFeesBurnedKey);

  if (cTotalFeesBurned !== undefined) {
    ctx.res.writeHead(200, {
      "Cache-Control": "max-age=5, stale-while-revalidate=18",
      "Content-Type": "application/json",
    });
    ctx.res.end(cTotalFeesBurned);
  }

  const totalFeesBurned = await BaseFees.getTotalFeesBurned();
  const totalFeesBurnedJson = JSON.stringify({ totalFeesBurned });

  totalFeesBurnedCache.set(totalFeesBurnedKey, totalFeesBurnedJson);

  ctx.res.writeHead(200, {
    "Cache-Control": "max-age=5, stale-while-revalidate=18",
    "Content-Type": "application/json",
  });
  ctx.res.end(totalFeesBurnedJson);
};

const handleGetFeesBurnedPerDay: Middleware = async (ctx) => {
  const feesBurnedPerDay = await BaseFees.getFeesBurnedPerDay();
  ctx.res.setHeader(
    "Cache-Control",
    "max-age=43200, stale-while-revalidate=86400",
  );
  ctx.res.setHeader("Content-Type", "application/json");
  ctx.body = { feesBurnedPerDay };
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

const router = new Router();

router.get("/fees/total-burned", handleGetFeesBurned);
router.get("/fees/burned-per-day", handleGetFeesBurnedPerDay);
router.get("/fees/eth-price", handleGetEthPrice);

app.use(router.routes());
app.use(router.allowedMethods());

const server = app.listen(port, () => {
  Log.info(`listening on ${port}`);
});

startWebSocketServer(server);
