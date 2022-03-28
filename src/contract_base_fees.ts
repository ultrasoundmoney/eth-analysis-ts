import { FeeSegments } from "./base_fees.js";
import { BlockNodeV2, ContractBaseFeesInsertable } from "./blocks/blocks.js";
import { A, flow, NEA, O, OAlt, pipe, TO } from "./fp.js";
import * as Blocks from "./blocks/blocks.js";
import * as Db from "./db.js";

// TODO: rename to BlockContractFees
export type ContractBaseFees = {
  baseFees256: bigint;
  baseFees: number;
  blockNumber: number;
  contractAddress: string;
  transactionCount: number;
};

export const insertableFromContractBaseFees = (
  contractBaseFees: ContractBaseFees,
): ContractBaseFeesInsertable => ({
  base_fees: contractBaseFees.baseFees,
  base_fees_256: String(contractBaseFees.baseFees256),
  block_number: contractBaseFees.blockNumber,
  contract_address: contractBaseFees.contractAddress,
  transaction_count: contractBaseFees.transactionCount,
});

export const contractBaseFeesFromAnalysis = (
  block: BlockNodeV2,
  feeSegments: FeeSegments,
  transactionCounts: Map<string, number>,
  address: string,
  baseFees: number,
): ContractBaseFees => ({
  baseFees: baseFees,
  baseFees256: pipe(
    feeSegments.contractSumsEthBI.get(address),
    O.fromNullable,
    OAlt.getOrThrow(
      "when storing contract base fees, bigint counterparts were missing",
    ),
  ),
  blockNumber: block.number,
  contractAddress: address,
  transactionCount: transactionCounts.get(address) ?? 0,
});

export const storeContractBaseFees = (
  block: Blocks.BlockNodeV2,
  feeSegments: FeeSegments,
  transactionCounts: Map<string, number>,
) =>
  pipe(
    Array.from(feeSegments.contractSumsEth.entries()),
    NEA.fromArray,
    TO.fromOption,
    TO.chainTaskK(
      flow(
        A.map(([address, baseFees]) =>
          contractBaseFeesFromAnalysis(
            block,
            feeSegments,
            transactionCounts,
            address,
            baseFees,
          ),
        ),
        A.map(insertableFromContractBaseFees),
        (insertables) =>
          Db.sqlTVoid`
            INSERT INTO contract_base_fees ${Db.values(insertables)}
          `,
      ),
    ),
  );

export const deleteContractBaseFees = (blockNumber: number) =>
  Db.sqlTVoid`
    DELETE FROM contract_base_fees
    WHERE block_number = ${blockNumber}
  `;
