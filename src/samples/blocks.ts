import fs from "fs/promises";
import neatCsv from "neat-csv";
import { URL } from "url";
import * as Blocks from "../blocks/blocks.js";
import { flow, NEA, O, OAlt, pipe, TE, TEAlt } from "../fp.js";

export type SupportedSample = "m5" | "h1";

const files: Record<SupportedSample, string> = {
  m5: new URL("./blocks_m5.csv", import.meta.url).pathname,
  h1: new URL("./blocks_h1.csv", import.meta.url).pathname,
};

const cache: Record<
  SupportedSample,
  O.Option<NEA.NonEmptyArray<Blocks.BlockV1>>
> = {
  m5: O.none,
  h1: O.none,
};

export type BlockCsv = {
  base_fee_per_gas: string;
  base_fee_sum: string;
  base_fee_sum_256: string;
  contract_creation_sum: string;
  eth_price: string;
  eth_transfer_sum: string;
  gas_used: string;
  hash: string;
  mined_at: string;
  number: string;
  tips: string;
};

const blockFromRaw = (rawBlock: BlockCsv): Blocks.BlockV1 => ({
  baseFeePerGas:
    rawBlock.base_fee_per_gas === "0.0"
      ? 0n
      : BigInt(rawBlock.base_fee_per_gas),
  baseFeeSum:
    rawBlock.base_fee_sum === "0.0" ? 0n : BigInt(rawBlock.base_fee_sum),
  contractCreationSum: Number(rawBlock.contract_creation_sum),
  ethPrice: Number(rawBlock.eth_price),
  ethTransferSum: Number(rawBlock.eth_transfer_sum),
  gasUsed: rawBlock.gas_used === "0.0" ? 0n : BigInt(rawBlock.gas_used),
  hash: rawBlock.hash,
  minedAt: new Date(rawBlock.mined_at),
  number: Number(rawBlock.number),
  tips: Number(rawBlock.tips),
});

export const getBlocksFromFile = (sample: SupportedSample) =>
  pipe(
    cache[sample],
    O.match(
      flow(
        TE.tryCatchK(
          () => fs.readFile(files[sample], "utf8"),
          TEAlt.decodeUnknownError,
        ),
        TE.chain(
          TE.tryCatchK(
            (file) => neatCsv<BlockCsv>(file),
            TEAlt.decodeUnknownError,
          ),
        ),
        TE.map(
          flow(
            NEA.fromArray,
            OAlt.getOrThrow("failed to read blocks from file, got empty list"),
            NEA.map(blockFromRaw),
          ),
        ),
        TE.chainFirstIOK((blocks) => () => {
          cache[sample] = O.some(blocks);
        }),
      ),
      TE.right,
    ),
    TEAlt.getOrThrow,
  );
