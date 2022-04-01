import * as EthNode from "./eth_node.js";
import { pipe, T } from "./fp.js";

export const getCurrentBlockLag = (currentBlock: { number: number }) =>
  pipe(
    () => EthNode.getLatestBlockNumber(),
    T.map((latestBlockNumber) =>
      pipe(latestBlockNumber, (latestBlockNumber) =>
        latestBlockNumber - currentBlock.number < 0
          ? 1
          : latestBlockNumber - currentBlock.number,
      ),
    ),
  );
