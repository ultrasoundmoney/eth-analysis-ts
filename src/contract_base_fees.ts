import { FeeSegments } from "./base_fees.js";
import { BlockV1, ContractBaseFeesInsertable } from "./blocks/blocks.js";
import { O, OAlt, pipe } from "./fp.js";

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
  block: BlockV1,
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
