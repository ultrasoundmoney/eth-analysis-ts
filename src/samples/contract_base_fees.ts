import fs from "fs/promises";
import neatCsv from "neat-csv";
import * as ContractBaseFees from "../contract_base_fees.js";
import { A, O, pipe, TE, TEAlt } from "../fp.js";

export type SupportedSample = "m5" | "h1";

const files: Record<SupportedSample, string> = {
  m5: new URL("./contract_base_fees_m5.csv", import.meta.url).pathname,
  h1: new URL("./contract_base_fees_h1.csv", import.meta.url).pathname,
};

const cache: Record<
  SupportedSample,
  O.Option<ContractBaseFees.ContractBaseFees[]>
> = {
  m5: O.none,
  h1: O.none,
};

type RawContractBaseFees = {
  base_fees: string;
  base_fees_256: string;
  block_number: string;
  contract_address: string;
  gas_used?: string;
  transaction_count: string;
};

const contractBaseFeesFromRaw = (
  rawRow: RawContractBaseFees,
): ContractBaseFees.ContractBaseFees => ({
  baseFees256: BigInt(rawRow.base_fees_256),
  baseFees: Number(rawRow.base_fees),
  blockNumber: Number(rawRow.block_number),
  contractAddress: rawRow.contract_address,
  gasUsed: BigInt(rawRow.gas_used ?? "0"),
  transactionCount: Number(rawRow.transaction_count),
});

export const getContractBaseFeesFromFile = (sample: SupportedSample) =>
  pipe(
    cache[sample],
    O.match(
      () =>
        pipe(
          TE.tryCatch(
            () => fs.readFile(files[sample], "utf8"),
            TEAlt.decodeUnknownError,
          ),
          TE.chain((file) =>
            TE.tryCatch(
              () => neatCsv<RawContractBaseFees>(file),
              TEAlt.decodeUnknownError,
            ),
          ),
          TE.map(A.map(contractBaseFeesFromRaw)),
          TE.chainFirstIOK((contractBaseFees) => () => {
            cache[sample] = O.some(contractBaseFees);
          }),
        ),
      TE.right,
    ),
    TEAlt.getOrThrow,
  );
