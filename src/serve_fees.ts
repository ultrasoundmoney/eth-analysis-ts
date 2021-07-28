import * as Log from "./log.js";
import QuickLRU from "quick-lru";
import Koa, { Middleware } from "koa";
import * as BaseFees from "./base_fees.js";
import type { TimeFrame } from "./base_fees.js";
import Router from "@koa/router";
import ws from "ws";
import { sql } from "./db.js";
const { Server: WebSocketServer } = ws;
import debounce from "debounce-fn";

const milisFromSeconds = (seconds: number) => seconds * 1000;

const topFeeBurnerCache = new QuickLRU<string, string>({
  maxSize: 4,
  maxAge: milisFromSeconds(10),
});

const getIsTimeFrame = (raw: unknown): raw is TimeFrame =>
  raw === "24h" || raw === "7d" || raw === "30d" || raw === "all";

const handleGetTopBurners: Middleware = async (ctx) => {
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

  const topTenFeeBurners = await BaseFees.getTopTenFeeBurners(timeFrame);

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

const totalFeesBurnedCache = new QuickLRU<string, string>({
  maxSize: 1,
  maxAge: 300,
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
  ctx.res.writeHead(200, {
    "Cache-Control": "max-age=43200, stale-while-revalidate=86400",
    "Content-Type": "application/json",
  });
  ctx.res.end(JSON.stringify({ feesBurnedPerDay }));
};

const router = new Router();

router.get("/fees/leaderboard", handleGetTopBurners);
router.get("/fees/total-burned", handleGetFeesBurned);
router.get("/fees/burned-per-day", handleGetFeesBurnedPerDay);

app.use(router.routes());
app.use(router.allowedMethods());

const server = app.listen(port, () => {
  Log.info(`> listening on ${port}`);
});

const wss = new WebSocketServer({ noServer: true });

server.on("upgrade", function upgrade(request, socket, head) {
  if (request.url === "/fees/base-fee-feed") {
    wss.handleUpgrade(request, socket, head, function done(ws) {
      wss.emit("connection", ws, request);
    });
  } else {
    socket.destroy();
  }
});

type BaseFeeListener = (number: number, baseFeePerGas: number) => void;

const baseFeeListeners: Map<string, BaseFeeListener> = new Map();

const addBaseFeeListener = (id: string, fn: BaseFeeListener): void => {
  baseFeeListeners.set(id, fn);
};

const removeBaseFeeListener = (id: string) => {
  baseFeeListeners.delete(id);
};

const onBaseFeeUpdate = (payload: string | undefined) => {
  if (payload === undefined) {
    Log.warn("got undefined payload on base-fee-updates channel");
    return;
  }

  const { number, baseFeePerGas } = JSON.parse(payload);

  baseFeeListeners.forEach((fn) => {
    fn(number, baseFeePerGas);
  });
};

const dOnBaseFeeUpdate = debounce(onBaseFeeUpdate, {
  wait: 1000,
  maxWait: 4000,
});

sql.listen("base-fee-updates", dOnBaseFeeUpdate);

wss.on("connection", (ws) => {
  const id = req.socket.remoteAddress;

  addBaseFeeListener(id, (number: number, baseFeePerGas: number) => {
    ws.send(JSON.stringify({ number, baseFeePerGas }));
  });

  ws.on("close", () => {
    removeBaseFeeListener(id);
  });
});
