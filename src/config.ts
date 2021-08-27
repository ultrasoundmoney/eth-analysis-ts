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

type Config = {
  env: Env;
  localNodeAvailable: boolean;
  name: string;
};

const config: Config = {
  env: parseEnv(),
  localNodeAvailable: parseLocalNodeAvailable(),
  name: process.env.NAME || "unknown",
};

export default config;
