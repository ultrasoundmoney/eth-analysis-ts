import * as Log from "./log.js";
import QuickLRU from "quick-lru";
import Koa, { Middleware } from "koa";
import * as BaseFees from "./base_fees.js";
import Router from "@koa/router";
import ws from "ws";
import { sql } from "./db.js";
const { Server: WebSocketServer } = ws;
import debounce from "debounce-fn";
import * as EthPrice from "./eth_price.js";
import Config from "./config.js";

const milisFromSeconds = (seconds: number) => seconds * 1000;

// const topFeeBurnerCache = new QuickLRU<string, string>({
//   maxSize: 4,
//   maxAge: milisFromSeconds(10),
// });

// const getIsTimeFrame = (raw: unknown): raw is Timeframe =>
//   raw === "24h" || raw === "7d" || raw === "30d" || raw === "all";

// const handleGetTopBurners: Middleware = async (ctx) => {
//   const timeframe = ctx.request.query["timeframe"];

//   if (!getIsTimeFrame(timeframe)) {
//     ctx.status = 400;
//     ctx.body = {
//       msg: "missing 'timeframe' query param, one of '24h', '7d', '30d' or 'all'",
//     };
//     return;
//   }

//   // Respond from cache if we can.
//   const cTopFeeBurners = topFeeBurnerCache.get(timeframe);
//   if (cTopFeeBurners !== undefined) {
//     ctx.res.writeHead(200, {
//       "Cache-Control": "max-age=5, stale-while-revalidate=18",
//       "Content-Type": "application/json",
//     });
//     ctx.res.end(cTopFeeBurners);
//     return;
//   }

//   const topTenFeeBurners = await BaseFeeTotals.getTopTenFeeBurners(timeframe);

//   // Cache the response
//   const topTenFeeBurnersJson = JSON.stringify(topTenFeeBurners);
//   topFeeBurnerCache.set(timeframe, topTenFeeBurnersJson);

//   ctx.res.writeHead(200, {
//     "Cache-Control": "max-age=5, stale-while-revalidate=18",
//     "Content-Type": "application/json",
//   });
//   ctx.res.end(topTenFeeBurnersJson);
// };

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
  ctx.res.writeHead(200, {
    "Cache-Control": "max-age=43200, stale-while-revalidate=86400",
    "Content-Type": "application/json",
  });
  ctx.res.end(JSON.stringify({ feesBurnedPerDay }));
};

const handleGetEthPrice: Middleware = async (ctx) => {
  const ethPrice = await EthPrice.getEthPrice();
  ctx.res.writeHead(200, {
    "Cache-Control": "max-age=600, stale-while-revalidate=1800",
    "Content-Type": "application/json",
  });
  ctx.res.end(JSON.stringify(ethPrice));
};

const router = new Router();

const routeInfix = Config.chain === "ropsten" ? "-ropsten" : "";

// router.get(`/fees${routeInfix}/leaderboard`, handleGetTopBurners);
router.get(`/fees${routeInfix}/total-burned`, handleGetFeesBurned);
router.get(`/fees${routeInfix}/burned-per-day`, handleGetFeesBurnedPerDay);
router.get(`/fees${routeInfix}/eth-price`, handleGetEthPrice);

app.use(router.routes());
app.use(router.allowedMethods());

const server = app.listen(port, () => {
  Log.info(`> listening on ${port}`);
});

const wss = new WebSocketServer({ noServer: true });

server.on("upgrade", (request, socket, head) => {
  if (request.url === `/fees${routeInfix}/base-fee-feed`) {
    wss.handleUpgrade(request, socket, head, (ws) => {
      wss.emit("connection", ws, request);
    });
  } else {
    socket.destroy();
  }
});

// json, number: number, baseFeePerGas: number, totalFeesBurned: number
type BaseFeeListener = (blockFeeUpdate: string) => void;

const baseFeeListeners: Map<string, BaseFeeListener> = new Map();

const addBaseFeeListener = (id: string, fn: BaseFeeListener): void => {
  baseFeeListeners.set(id, fn);
};

const removeBaseFeeListener = (id: string) => {
  baseFeeListeners.delete(id);
};

let lastFeeUpdate: string | undefined = undefined;
let lastLeaderboardUpdate: string | undefined = undefined;

const onBaseFeeUpdate = (payload: string | undefined) => {
  if (payload === undefined) {
    Log.warn("got undefined payload on base-fee-updates channel");
    return;
  }

  baseFeeListeners.forEach((fn) => {
    fn(payload);
  });
};

const dOnBaseFeeUpdate = debounce(onBaseFeeUpdate, {
  wait: 1000,
  maxWait: 4000,
});

sql.listen("base-fee-updates", (payload) => {
  if (JSON.parse(payload!).type === "base-fee-update") {
    lastFeeUpdate = payload;
  }
  if (JSON.parse(payload!).type === "leaderboard-update") {
    lastLeaderboardUpdate = payload;
  }
  dOnBaseFeeUpdate(payload);
});

wss.on("error", (error) => Log.error("> wss error", { error }));

wss.on("connection", (ws, req) => {
  const id = req.socket.remoteAddress;

  if (id === undefined) {
    Log.error("socket has no remote address, can't id connection, dropping ws");
    return;
  }

  addBaseFeeListener(id, (payload) => ws.send(payload));

  // To make sure clients immediately have the last known state we send it on connect.
  if (typeof lastFeeUpdate === "string") {
    ws.send(lastFeeUpdate);
  }

  if (typeof lastLeaderboardUpdate === "string") {
    ws.send(lastLeaderboardUpdate);
  }

  ws.on("close", () => {
    removeBaseFeeListener(id);
  });
});
