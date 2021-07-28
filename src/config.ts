import * as Log from "./log.js";

type Env = "dev" | "prod";

const parseEnv = (): Env => {
  const rawEnv = process.env.ENV;

  switch (rawEnv) {
    case "prod":
      return "prod";
    case "dev":
      return "dev";
    default:
      Log.warn("> no ENV in env, defaulting to dev");
      return "prod";
  }
};

type Chain = "ropsten" | "mainnet";

const parseChain = (): Chain => {
  switch (process.env.CHAIN) {
    case "mainnet":
      return "mainnet";
    case "ropsten":
      return "ropsten";
    default:
      Log.warn("> no CHAIN in env, defaulting to mainnet");
      return "mainnet";
  }
};

type Config = {
  env: Env;
  chain: Chain;
};

const config: Config = {
  env: parseEnv(),
  chain: parseChain(),
};

export default config;
