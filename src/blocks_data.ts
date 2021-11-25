import fs from "fs/promises";
import neatCsv from "neat-csv";
import { URL } from "url";
import { BlockDb } from "./blocks.js";
import { A, O, pipe, T } from "./fp.js";

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
  baseFeePerGas: Number(rawBlock.base_fee_per_gas),
  baseFeeSum: BigInt(rawBlock.base_fee_sum),
  contractCreationSum: Number(rawBlock.contract_creation_sum),
  ethPrice: Number(rawBlock.eth_price),
  ethTransferSum: Number(rawBlock.eth_transfer_sum),
  gasUsed: Number(rawBlock.gas_used),
  hash: rawBlock.hash,
  minedAt: new Date(rawBlock.mined_at),
  number: Number(rawBlock.number),
  tips: Number(rawBlock.tips),
});

const blocks5mPath = new URL("./blocks_5m.csv", import.meta.url).pathname;

export const getSingleBlock = (): T.Task<BlockDb> =>
  pipe(
    () => fs.readFile(blocks5mPath, "utf8"),
    T.chain((raw) => () => neatCsv<RawBlock>(raw)),
    T.map(A.lookup(1)),
    T.map(O.map(blockFromRawBlock)),
    T.map(
      O.getOrElseW(() => {
        throw new Error("get single block, no block");
      }),
    ),
  );
