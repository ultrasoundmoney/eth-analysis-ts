import { sql } from "./db.js";
import type { TxRWeb3London } from "./transactions";
import A from "fp-ts/lib/Array.js";
import { pipe } from "fp-ts/lib/function.js";
import * as Log from "./log.js";

export type FeesPaid = {
  // fees paid for simple transfers.
  transfers: number;
  // fees paid for use of contracts.
  contract_use_fees: Record<string, number>;
  // fees paid for the creation of contracts.
  contract_creation_fees: number;
};

export const getLatestGasUseBlockNumber = (): Promise<number | undefined> =>
  sql`
    SELECT max(number) AS number FROM gas_use_per_block
  `.then((result) => result[0]?.number || undefined);

export const storeFeesPaidForBlock = (
  hash: string,
  number: number,
  feesPaid: FeesPaid,
): Promise<void> =>
  sql`
    INSERT INTO gas_use_per_block
      (hash, number, fees_paid)
    VALUES (${hash}, ${number}, ${sql.json(feesPaid)})
`.then(() => undefined);

const hexToNumber = (hex: string) => Number.parseInt(hex, 16);

const calculateFee = (txr: TxRWeb3London): number =>
  (txr.gasUsed * hexToNumber(txr.effectiveGasPrice)) / 10 ** 18;

export const calcTxrFees = (txrs: TxRWeb3London[]): number =>
  pipe(
    txrs,
    A.reduce(0, (feesPaid, txr) => feesPaid + calculateFee(txr)),
  );

type FeePerContractMap = Record<string, number>;

export const calcContractUseFees = (txrs: TxRWeb3London[]): FeePerContractMap =>
  pipe(
    txrs,
    A.reduce({} as FeePerContractMap, (aggFee, txr) => {
      // Contract creation
      if (txr.to === null) {
        return aggFee;
      }

      const feesPaid = aggFee[txr.to] || 0;
      aggFee[txr.to] = feesPaid + calculateFee(txr);

      return aggFee;
    }),
  );

// Name is undefined because we don't always know the name for a contract. Image is undefined because we don't always have an image for a contract. Address is undefined because fees paid for ETH transfers are shared between many addresses.
export type FeeUser = {
  name: string | undefined;
  address: string | undefined;
  image: string | undefined;
  fees: number;
};

// Length is less than or equal to ten.
export type TopGasUsers = FeeUser[];

// ~6.88 days
const weekOfBlocksCount = 45000;

export const getTopTenFeeUsers = async (): Promise<TopGasUsers> => {
  const feesPaidForBlocks = await sql<{ feesPaid: FeesPaid }[]>`
      SELECT fees_paid
      FROM gas_use_per_block
      LIMIT ${weekOfBlocksCount}
  `.then((result) => {
    if (result.length === 0) {
      Log.warn("tried to determine top gas users but found no analyzed blocks");
      return [];
    }

    return result.map((row) => row.feesPaid);
  });

  const ethTransferFeesPaid = feesPaidForBlocks.map(
    (feesPaid) => feesPaid.transfers,
  );
  const contractCreationFeesPaid = feesPaidForBlocks.map(
    (feesPaid) => feesPaid.contract_creation_fees,
  );
  const contractUseFeesPaid = feesPaidForBlocks.map(
    (feesPaid) => feesPaid.contract_use_fees,
  );

  const ethTransferFeesTotal = ethTransferFeesPaid.reduce(
    (sum, fee) => sum + fee,
    0,
  );
  const contractCreationFeesPaidTotal = contractCreationFeesPaid.reduce(
    (sum, fee) => sum + fee,
    0,
  );

  const contractUseFeesPaidTotal: FeeUser[] = pipe(
    contractUseFeesPaid,
    A.reduce({} as Record<string, number>, (agg, feesPaidPerContract) => {
      Object.entries(feesPaidPerContract).forEach(([address, fee]) => {
        const sum = agg[address] || 0;
        agg[address] = sum + fee;
      });
      return agg;
    }),
    Object.entries,
    A.map(([address, fees]) => ({
      image: undefined,
      name: undefined,
      address,
      fees,
    })),
  );

  return pipe(
    [
      {
        image: undefined,
        name: "eth transfer fees",
        fees: ethTransferFeesTotal,
        address: undefined,
      },
      {
        image: undefined,
        name: "contract creation fees",
        fees: contractCreationFeesPaidTotal,
        address: undefined,
      },
      ...contractUseFeesPaidTotal,
    ],
    A.sort<FeeUser>({
      compare: (first, second) =>
        first.fees === second.fees ? 0 : first.fees > second.fees ? -1 : 1,
      equals: (first, second) => first.fees === second.fees,
    }),
    A.takeLeft(10),
  );
};
