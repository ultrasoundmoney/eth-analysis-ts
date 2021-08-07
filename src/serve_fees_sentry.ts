import * as SentryM from "@sentry/node";
import {
  extractTraceparentData,
  stripUrlQueryAndFragment,
} from "@sentry/tracing";
// eslint-disable-next-line node/no-deprecated-api
import domain from "domain";
import { EventEmitter } from "events";
import { Middleware } from "koa";
import Config from "./config.js";

export const Sentry = SentryM;

Sentry.init({
  dsn: "https://aa7ee1839c7b4ed4993023a300b438de@o920717.ingest.sentry.io/5896640",
  environment: Config.env,
});

export const requestHandler: Middleware = (ctx, next) => {
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
export const tracingMiddleWare: Middleware = async (ctx, next) => {
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
