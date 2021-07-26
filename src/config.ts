export default {
  network: "ropsten" as "mainnet" | "ropsten",
  env: process.env.ENV || ("prod" as "dev" | "prod"),
};
