import fs from "fs/promises";
import neatCsv from "neat-csv";
import { URL } from "url";
import * as Blocks from "../blocks/blocks.js";
import * as Db from "../db.js";
import { A, O, pipe, T, TE, TEAlt } from "../fp.js";

type RawContractBaseFees = {
  block_number: string;
  contract_address: string;
  base_fees: string;
  transaction_count: string;
  base_fees_256: string;
};

const contractBaseFeesFromRaw = (
  rawRow: RawContractBaseFees,
): Blocks.ContractBaseFeesRow => ({
  block_number: Number(rawRow.block_number),
  contract_address: rawRow.contract_address,
  base_fees: Number(rawRow.base_fees),
  transaction_count: Number(rawRow.transaction_count),
  base_fees_256: rawRow.base_fees_256,
});

const contractBaseFeesM5 = new URL(
  "./contract_base_fees_m5.csv",
  import.meta.url,
).pathname;
// const contractBaseFeesH1 = new URL(
//   "./contract_base_fees_h1.csv",
//   import.meta.url,
// ).pathname;

let m5ContractBaseFees: O.Option<Blocks.ContractBaseFeesRow[]> = O.none;

export const getM5ContractBaseFees = () =>
  pipe(
    m5ContractBaseFees,
    O.match(
      () =>
        pipe(
          TE.tryCatch(
            () => fs.readFile(contractBaseFeesM5, "utf8"),
            TEAlt.errorFromUnknown,
          ),
          TE.chain((file) =>
            TE.tryCatch(
              () => neatCsv<RawContractBaseFees>(file),
              TEAlt.errorFromUnknown,
            ),
          ),
          TE.map(A.map(contractBaseFeesFromRaw)),
          TE.chainFirstIOK((contractBaseFees) => () => {
            m5ContractBaseFees = O.some(contractBaseFees);
          }),
        ),
      (contractBaseFees) => TE.right(contractBaseFees),
    ),
    TEAlt.getOrThrow,
  );
