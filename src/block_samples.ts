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

const blocksM5Path = new URL("./sample-data/blocks_5m.csv", import.meta.url)
  .pathname;
const blocksH1Path = new URL("./sample-data/blocks_1h.csv", import.meta.url)
  .pathname;
const blocksD1Path = new URL("./sample-data/blocks_1d.csv", import.meta.url)
  .pathname;

let m5Blocks: BlockDb[] | undefined = undefined;

export const getSingleBlock = async (): Promise<BlockDb> => {
  if (m5Blocks === undefined) {
    const file = await fs.readFile(blocksM5Path, "utf8");
    const rawBlocks = await neatCsv<RawBlock>(file);
    m5Blocks = rawBlocks.map(blockFromRawBlock);
  }

  return m5Blocks[1];
};

export const getM5Blocks = async (): Promise<BlockDb[]> => {
  if (m5Blocks === undefined) {
    const file = await fs.readFile(blocksM5Path, "utf8");
    const rawBlocks = await neatCsv<RawBlock>(file);
    m5Blocks = rawBlocks.map(blockFromRawBlock);
  }

  return m5Blocks;
};

let h1Blocks: BlockDb[] | undefined = undefined;

export const getH1Blocks = async (): Promise<BlockDb[]> => {
  if (h1Blocks === undefined) {
    const file = await fs.readFile(blocksH1Path, "utf8");
    const rawBlocks = await neatCsv<RawBlock>(file);
    h1Blocks = rawBlocks.map(blockFromRawBlock);
  }

  return h1Blocks;
};

let d1Blocks: BlockDb[] | undefined = undefined;

export const getD1Blocks = async (): Promise<BlockDb[]> => {
  if (d1Blocks === undefined) {
    const file = await fs.readFile(blocksD1Path, "utf8");
    const rawBlocks = await neatCsv<RawBlock>(file);
    d1Blocks = rawBlocks.map(blockFromRawBlock);
  }

  return d1Blocks;
};
