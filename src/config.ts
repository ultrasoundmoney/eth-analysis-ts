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

const parseLocalNodeAvailable = (): boolean =>
  !(
    process.env.LOCAL_NODE_AVAILABLE === undefined ||
    process.env.LOCAL_NODE_AVAILABLE === "false"
  );

const parseShowProgress = (): boolean =>
  !(
    process.env.SHOW_PROGRESS === undefined ||
    process.env.SHOW_PROGRESS === "false"
  );

type Config = {
  env: Env;
  localNodeAvailable: boolean;
  name: string;
  showProgress: boolean;
};

const config: Config = {
  env: parseEnv(),
  localNodeAvailable: parseLocalNodeAvailable(),
  name: process.env.NAME || "unknown",
  showProgress: parseShowProgress(),
};

export default config;

export const getEtherscanToken = (): string => {
  const rawToken = process.env.ETHERSCAN_TOKEN;

  if (typeof rawToken !== "string") {
    throw new Error("missing ETHERSCAN_TOKEN env var");
  }

  return rawToken;
};

export const getTwitterToken = (): string => {
  const rawToken = process.env.TWITTER_TOKEN;

  if (typeof rawToken !== "string") {
    throw new Error("missing TWITTER_TOKEN env var");
  }

  return rawToken;
};
