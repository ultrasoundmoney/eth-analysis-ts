import * as Log from "./log.js";

const parseSimpleEnvVar = (name: string): string => {
  const rawVar = process.env[name];
  if (typeof rawVar !== "string") {
    throw new Error(`missing ${name} env var`);
  }
  return rawVar;
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

export const getFamServiceUrl = (): string =>
  getEnv() === "prod" || getEnv() === "staging"
    ? "http://serve-fam"
    : "https://api.ultrasound.money";

export const getEtherscanToken = (): string =>
  parseSimpleEnvVar("ETHERSCAN_TOKEN");

export const getTwitterToken = (): string =>
  parseSimpleEnvVar("TWITTER_BEARER_TOKEN_USM");

export const getAdminToken = (): string => parseSimpleEnvVar("ADMIN_TOKEN");

export const getGethUrl = (): string => parseSimpleEnvVar("GETH_URL");

export const getGethFallbackUrl = (): string =>
  parseSimpleEnvVar("GETH_FALLBACK_URL");

export const ensureCriticalBlockAnalysisConfig = (): void => {
  getGethUrl();
  getGethFallbackUrl();
};

export const getOpenseaApiKey = (): string =>
  parseSimpleEnvVar("OPENSEA_API_KEY");

export const getGlassnodeApiKey = (): string =>
  parseSimpleEnvVar("GLASSNODE_API_KEY");
