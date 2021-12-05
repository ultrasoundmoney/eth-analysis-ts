import fs from "fs/promises";
import neatCsv from "neat-csv";
import { URL } from "url";
import { BlockDb } from "./blocks/blocks.js";

type RawBlock = {
  base_fee_per_gas: string;
  base_fee_sum: string;
  contract_creation_sum: string;
  eth_price: string;
  eth_transfer_sum: string;
  gas_used: string;
  hash: string;
  mined_at: string;
  number: string;
  tips: string;
};

const blockFromRawBlock = (rawBlock: RawBlock): BlockDb => ({
  baseFeePerGas: BigInt(Number(rawBlock.base_fee_per_gas)),
  baseFeeSum: BigInt(Number(rawBlock.base_fee_sum)),
  contractCreationSum: Number(rawBlock.contract_creation_sum),
  ethPrice: Number(rawBlock.eth_price),
  ethPriceCents: BigInt(
    BigInt(rawBlock.base_fee_per_gas) *
      BigInt(rawBlock.gas_used) *
      BigInt(Math.round(Number(rawBlock.eth_price)) * 100),
  ),
  ethTransferSum: Number(rawBlock.eth_transfer_sum),
  gasUsed: BigInt(Number(rawBlock.gas_used)),
  hash: rawBlock.hash,
  minedAt: new Date(rawBlock.mined_at),
  number: Number(rawBlock.number),
  tips: Number(rawBlock.tips),
});

const blocks5mPath = new URL("./blocks_5m.csv", import.meta.url).pathname;

export const getSingleBlock = async (): Promise<BlockDb> => {
  const file = await fs.readFile(blocks5mPath, "utf8");
  const rawBlocks = await neatCsv<RawBlock>(file);
  return blockFromRawBlock(rawBlocks[1]);
};
