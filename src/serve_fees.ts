import Koa, { Middleware } from "koa";
import * as Sentry from "@sentry/node";
import {
  extractTraceparentData,
  stripUrlQueryAndFragment,
} from "@sentry/tracing";
// eslint-disable-next-line node/no-deprecated-api
import domain from "domain";
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
import Config from "./config.js";
import { EventEmitter } from "events";

Sentry.init({
  dsn: "https://aa7ee1839c7b4ed4993023a300b438de@o920717.ingest.sentry.io/5896640",
  environment: Config.env,
});

const requestHandler: Middleware = (ctx, next) => {
  return new Promise((resolve) => {
    const local = domain.create();
    local.add(ctx as unknown as EventEmitter);
    local.on("error", (err) => {
      ctx.status = err.status || 500;
      ctx.body = err.message;
      ctx.app.emit("error", err, ctx);
    });
    local.run(async () => {
      Sentry.getCurrentHub().configureScope((scope) =>
        scope.addEventProcessor((event) =>
          Sentry.Handlers.parseRequest(event, ctx.request, { user: false }),
        ),
      );
      await next();
      resolve(undefined);
    });
  });
};

// this tracing middleware creates a transaction per request
const tracingMiddleWare: Middleware = async (ctx, next) => {
  const reqMethod = (ctx.method || "").toUpperCase();
  const reqUrl = ctx.url && stripUrlQueryAndFragment(ctx.url);

  // connect to trace of upstream app
  let traceparentData;
  if (ctx.request.get("sentry-trace")) {
    traceparentData = extractTraceparentData(ctx.request.get("sentry-trace"));
  }

  const transaction = Sentry.startTransaction({
    name: `${reqMethod} ${reqUrl}`,
    op: "http.server",
    ...traceparentData,
  });

  ctx.__sentry_transaction = transaction;

  // We put the transaction on the scope so users can attach children to it
  Sentry.getCurrentHub().configureScope((scope) => {
    scope.setSpan(transaction);
  });

  ctx.res.on("finish", () => {
    // Push `transaction.finish` to the next event loop so open spans have a chance to finish before the transaction closes
    setImmediate(() => {
      // if using koa router, a nicer way to capture transaction using the matched route
      if (ctx._matchedRoute) {
        const mountPath = ctx.mountPath || "";
        transaction.setName(`${reqMethod} ${mountPath}${ctx._matchedRoute}`);
      }
      transaction.setHttpStatus(ctx.status);
      transaction.finish();
    });
  });

  await next();
};

const milisFromSeconds = (seconds: number) => seconds * 1000;

const port = process.env.PORT || 8080;

const app = new Koa();

app.use(async (ctx, next) => {
  ctx.res.setHeader("Access-Control-Allow-Origin", "*");
  await next();
});

app.use(requestHandler);
app.use(tracingMiddleWare);

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

router.get("/fees/total-burned", handleGetFeesBurned);
router.get("/fees/burned-per-day", handleGetFeesBurnedPerDay);
router.get("/fees/eth-price", handleGetEthPrice);

app.use(router.routes());
app.use(router.allowedMethods());

const server = app.listen(port, () => {
  Log.info(`listening on ${port}`);
});

const wss = new WebSocketServer({ noServer: true });

server.on("upgrade", (request, socket, head) => {
  if (request.url === "/fees/base-fee-feed") {
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

let lastFeeUpdates: string[] = [];
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

// const dOnBaseFeeUpdate = debounce(onBaseFeeUpdate, {
//   wait: 1000,
//   maxWait: 4000,
// });

sql.listen("base-fee-updates", (payload) => {
  if (JSON.parse(payload!).type === "base-fee-update") {
    lastFeeUpdates.push(payload!);
    if (lastFeeUpdates.length > 7) {
      lastFeeUpdates = pipe(lastFeeUpdates, A.takeRight(7));
    }
  }
  if (JSON.parse(payload!).type === "leaderboard-update") {
    lastLeaderboardUpdate = payload;
  }
  onBaseFeeUpdate(payload);
});

wss.on("error", (error) => Log.error("wss error", { error }));

wss.on("connection", (ws, req) => {
  const id = req.socket.remoteAddress;

  if (id === undefined) {
    Log.error("socket has no remote address, can't id connection, dropping ws");
    return;
  }

  addBaseFeeListener(id, (payload) => ws.send(payload));

  // To make sure clients immediately have the last known state we send it on connect.
  lastFeeUpdates.forEach((blockUpdatePayload) => ws.send(blockUpdatePayload));

  if (typeof lastLeaderboardUpdate === "string") {
    ws.send(lastLeaderboardUpdate);
  }

  ws.on("close", () => {
    removeBaseFeeListener(id);
  });
});

// Cache total fees immediately.
BaseFees.getRealtimeTotalFeesBurned({
  contract_use_fees: {},
  contract_creation_fees: 0,
  transfers: 0,
}).then(() => {
  Log.info("done initializing total fees cache");
});
