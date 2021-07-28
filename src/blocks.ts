import { BlockTransactionString as BlockWeb3 } from "web3-eth/types/index";

export type BlockWeb3London = BlockWeb3 & {
  baseFeePerGas: string;
};
