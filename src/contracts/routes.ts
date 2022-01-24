import Router from "@koa/router";
import { Middleware } from "koa";
import * as Config from "../config.js";
import { pipe, T } from "../fp.js";
import * as Admin from "./admin.js";

export const handleSetContractTwitterHandle: Middleware = async (ctx) => {
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

  await Admin.setTwitterHandle(address, handle)();
  ctx.status = 200;
  return undefined;
};

export const handleSetContractName: Middleware = async (ctx) => {
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

  await Admin.setName(address, name)();
  ctx.status = 200;
  return undefined;
};

export const handleSetContractCategory: Middleware = async (ctx) => {
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

  await Admin.setCategory(address, category)();
  ctx.status = 200;
  return undefined;
};

export const handleSetContractLastManuallyVerified: Middleware = async (
  ctx,
) => {
  const address = ctx.query.address;

  if (typeof address !== "string") {
    ctx.status = 400;
    ctx.body = { msg: "missing address" };
    return undefined;
  }

  await Admin.setLastManuallyVerified(address)();
  ctx.status = 200;
  return undefined;
};

const getIsAddresses = (u: unknown): u is string[] =>
  Array.isArray(u) &&
  u.length > 0 &&
  !u.some((address) => typeof address !== "string");

export const handleGetMetadataFreshness: Middleware = async (ctx) => {
  const addresses = ctx.request.body?.addresses;

  if (!getIsAddresses(addresses)) {
    ctx.status = 400;
    ctx.body = {
      msg: "body must be json with 'addresses' a list of strings",
    };
    return;
  }

  await pipe(
    Admin.getMetadataFreshness(addresses),
    T.map((metadataFreshnessMap) => {
      ctx.status = 200;
      ctx.body = Object.fromEntries(metadataFreshnessMap.entries());
      return undefined;
    }),
  )();
};

const adminAuth: Middleware = (ctx, next) => {
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

  return next();
};

export const registerRoutes = (router: Router) => {
  router.use("/fees/contracts/admin", adminAuth);
  router.get(
    "/fees/contracts/admin/set-twitter-handle",
    handleSetContractTwitterHandle,
  );
  router.get("/fees/contracts/admin/set-name", handleSetContractName);
  router.get("/fees/contracts/admin/set-category", handleSetContractCategory);
  router.get(
    "/fees/contracts/admin/set-last-manually-verified",
    handleSetContractLastManuallyVerified,
  );
  router.post("/fees/contracts/metadata-freshness", handleGetMetadataFreshness);
};
