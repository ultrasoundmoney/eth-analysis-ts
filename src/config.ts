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

const parseName = (): string => {
  const rawName = process.argv[1].split("/").pop();
  switch (rawName) {
    case "watch_base_fees.ts":
      return "watch-base-fees";
    case "watch_base_fees.js":
      return "watch-base-fees";
    case "watch_base_fee_totals.ts":
      return "watch-base-fee-totals";
    case "watch_base_fee_totals.js":
      return "watch-base-fee-totals";
    case "":
      return "unknown";
    default:
      return "unknown";
  }
};

type Config = {
  env: Env;
  localNodeAvailable: boolean;
  name: string;
};

const config: Config = {
  env: parseEnv(),
  localNodeAvailable: !(
    process.env.LOCAL_NODE_AVAILABLE === undefined ||
    process.env.LOCAL_NODE_AVAILABLE === "" ||
    process.env.LOCAL_NODE_AVAILABLE === "false"
  ),
  name: parseName(),
};

export default config;
