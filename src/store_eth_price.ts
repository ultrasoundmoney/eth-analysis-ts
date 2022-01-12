import { setInterval } from "timers/promises";
import * as Duration from "./duration.js";
import * as EthPrices from "./eth-prices/eth_prices.js";
import { pipe, TEAlt } from "./fp.js";

process.on("unhandledRejection", (error) => {
  throw error;
});

const intervalIterator = setInterval(
  Duration.millisFromSeconds(10),
  Date.now(),
);

// eslint-disable-next-line @typescript-eslint/no-unused-vars
for await (const _ of intervalIterator) {
  await pipe(EthPrices.storeCurrentEthPrice(), TEAlt.getOrThrow)();
}
