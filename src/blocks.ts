import * as Sentry from "@sentry/node";
import * as EthNode from "./eth_node.js";
import { BlockLondon } from "./eth_node.js";
import * as Duration from "./duration.js";
import * as Log from "./log.js";
import { delay } from "./delay.js";

export const londonHardForkBlockNumber = 12965000;

export const getBlockRange = (from: number, toAndIncluding: number): number[] =>
  new Array(toAndIncluding - from + 1)
    .fill(undefined)
    .map((_, i) => toAndIncluding - i)
    .reverse();

export const getBlockWithRetry = async (
  blockNumber: number | "latest" | string,
): Promise<BlockLondon> => {
  const delayMilis = Duration.milisFromSeconds(3);
  const delaySeconds = delayMilis * 1000;
  let tries = 0;

  // Retry continuously
  // eslint-disable-next-line no-constant-condition
  while (true) {
    tries += tries + 1;

    const maybeBlock = await EthNode.getBlock(blockNumber);

    if (typeof maybeBlock?.hash === "string") {
      return maybeBlock;
    }

    if (tries === 10) {
      Sentry.captureException(
        new Error(
          `stuck fetching block, for more than ${tries * delaySeconds}s`,
        ),
      );
    }

    if (tries > 20) {
      throw new Error("failed to fetch block, stayed null");
    }

    Log.warn(
      `asked for block ${blockNumber}, got null, waiting ${delaySeconds}s and trying again`,
    );
    await delay(delayMilis);
  }
};
