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

type Config = {
  env: Env;
  localNodeAvailable: boolean;
};

const config: Config = {
  env: parseEnv(),
  localNodeAvailable: !(
    process.env.LOCAL_NODE_AVAILABLE === undefined ||
    process.env.LOCAL_NODE_AVAILABLE === "" ||
    process.env.LOCAL_NODE_AVAILABLE === "false"
  ),
};

export default config;
