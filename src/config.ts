import { O, OAlt, pipe } from "./fp.js";
import * as Log from "./log.js";

const parseSimpleEnvVar = (name: string) =>
  pipe(process.env[name], O.fromNullable);

const parseSimpleEnvVarUnsafe = (name: string): string => {
  return pipe(
    parseSimpleEnvVar(name),
    OAlt.getOrThrow(`failed to parse ${name} env var`),
  );
};

const parseEnvBoolean = (key: string): boolean =>
  pipe(
    parseSimpleEnvVar(key),
    O.map((boolStr) => boolStr.toLowerCase() === "true"),
    O.getOrElseW(() => false),
  );

export type Env = "dev" | "prod" | "staging";

export const getEnv = (): Env => {
  const rawEnv = process.env.ENV;

  switch (rawEnv) {
    case "prod":
      return "prod";
    case "dev":
      return "dev";
    case "staging":
      return "staging";
    case "stag":
      return "staging";
    default:
      Log.debug("no ENV in env, defaulting to dev");
      return "dev";
  }
};

export const getName = () => process.env.NAME || "unknown";

const getUsePublicServiceUrl = () => parseEnvBoolean("USE_PUBLIC_SERVICE_URL");

export const getFamServiceUrl = () =>
  (getEnv() === "prod" || getEnv() === "staging") && !getUsePublicServiceUrl()
    ? // Socket hangups, cloud provider network issue?
      // Temporarily use external route.
      // ? "http://serve-fam"
      "https://ultrasound.money"
    : "https://ultrasound.money";

export const getEtherscanApiKey = (): string =>
  parseSimpleEnvVarUnsafe("ETHERSCAN_API_KEY");

export const getTwitterToken = (): string =>
  parseSimpleEnvVarUnsafe("TWITTER_BEARER_TOKEN_USM");

export const getAdminToken = (): string =>
  parseSimpleEnvVarUnsafe("ADMIN_TOKEN");

export const getGethUrl = (): string => parseSimpleEnvVarUnsafe("GETH_URL");

export const getGethFallbackUrl = (): string =>
  parseSimpleEnvVarUnsafe("GETH_FALLBACK_URL");

export const ensureCriticalBlockAnalysisConfig = (): void => {
  getGethUrl();
  getGethFallbackUrl();
};

export const getOpenseaApiKey = (): string =>
  parseSimpleEnvVarUnsafe("OPENSEA_API_KEY");

export const getGlassnodeApiKey = (): string =>
  parseSimpleEnvVarUnsafe("GLASSNODE_API_KEY");

export const getLogPerformance = (): boolean => parseEnvBoolean("LOG_PERF");

export const getUseNodeFallback = () => parseEnvBoolean("USE_NODE_FALLBACK");

export const getBeaconUrl = (): string => parseSimpleEnvVarUnsafe("BEACON_URL");

export const getYahooFinanceApiKey = (): string =>
  parseSimpleEnvVarUnsafe("YAHOO_FINANCE_API_KEY");

export const getOpsGenieApiKey = (): string =>
  parseSimpleEnvVarUnsafe("OPSGENIE_API_KEY");
