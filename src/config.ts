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
    default:
      Log.debug("no ENV in env, defaulting to dev");
      return "dev";
  }
};

export const getName = () => process.env.NAME || "unknown";

export const getFamServiceUrl = () =>
  getEnv() === "prod" || getEnv() === "staging"
    ? "http://serve-fam"
    : "https://api.ultrasound.money";

export const getEtherscanToken = (): string =>
  parseSimpleEnvVarUnsafe("ETHERSCAN_TOKEN");

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

export const getLogPerformance = (): boolean =>
  process.env["LOG_PERF"] === "true";

export const getUseNodeFallback = () =>
  pipe(
    parseSimpleEnvVar("USE_NODE_FALLBACK"),
    O.map((useNodeFallbackStr) => useNodeFallbackStr.toLowerCase() === "true"),
    O.getOrElse(() => false),
  );
