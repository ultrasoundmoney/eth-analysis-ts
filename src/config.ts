import * as Log from "./log.js";

type Env = "dev" | "prod" | "staging";

const parseEnv = (): Env => {
  const rawEnv = process.env.ENV;

  switch (rawEnv) {
    case "prod":
      return "prod";
    case "dev":
      return "dev";
    case "staging":
      return "staging";
    default:
      Log.warn("no ENV in env, defaulting to dev");
      return "dev";
  }
};

const env = parseEnv();

const parseShowProgress = (): boolean =>
  !(
    process.env.SHOW_PROGRESS === undefined ||
    process.env.SHOW_PROGRESS === "false"
  );

export type Config = {
  env: Env;
  famServiceUrl: string;
  gethUrl: string;
  name: string;
  showProgress: boolean;
};

export const config: Config = {
  env,
  gethUrl: process.env.GETH_URL || "ws://64.227.73.122:8546/",
  name: process.env.NAME || "unknown",
  showProgress: parseShowProgress(),
  famServiceUrl:
    env === "prod" || env === "staging"
      ? "http://serve-fam"
      : "https://api.ultrasound.money",
};

export const getEtherscanToken = (): string => {
  const rawToken = process.env.ETHERSCAN_TOKEN;

  if (typeof rawToken !== "string") {
    throw new Error("missing ETHERSCAN_TOKEN env var");
  }

  return rawToken;
};

export const getTwitterToken = (): string => {
  const rawToken = process.env.TWITTER_BEARER_TOKEN_USM;

  if (typeof rawToken !== "string") {
    throw new Error("missing TWITTER_BEARER_TOKEN_USM env var");
  }

  return rawToken;
};

export const getAdminToken = (): string => {
  const rawToken = process.env.ADMIN_TOKEN;

  if (typeof rawToken !== "string") {
    throw new Error("missing ADMIN_TOKEN env var");
  }

  return rawToken;
};
