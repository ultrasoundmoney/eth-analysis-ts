import { deepStrictEqual } from "assert";
import makeEta from "simple-eta";
import { isDeepStrictEqual } from "util";
import * as BaseFees from "../base_fees.js";
import * as Blocks from "../blocks/blocks.js";
import * as ContractBaseFees from "../contract_base_fees.js";
import * as Db from "../db.js";
import { sql } from "../db.js";
import * as Duration from "../duration.js";
import { EthPrice, getEthPrice } from "../eth-prices/index.js";
import { A, O, pipe, T, TEAlt, TOAlt } from "../fp.js";
import * as Log from "../log.js";
import * as Transactions from "../transactions.js";

const lastStoredBlock = await Blocks.getLastStoredBlock()();

const lastCheckedBlock = await Db.sql<{ value: number | null }[]>`
  SELECT "value" FROM key_value_store
  WHERE "key" = 'last-checked-transaction-block'
`.then((rows) => rows[0]?.value);

const blocksToCheck = Blocks.getBlockRange(
  lastCheckedBlock ?? Blocks.londonHardForkBlockNumber,
  lastStoredBlock.number,
);

const storeLastAdded = (blockNumber: number) => Db.sql`
  INSERT INTO key_value_store
    (key, value)
  VALUES
    ('last-checked-transaction-block', ${sql.json(blockNumber)})
  ON CONFLICT (key) DO UPDATE SET
    value = excluded.value
`;

const heal = (
  block: Blocks.BlockNodeV2,
  transactionReceipts: Transactions.TransactionReceiptV1[],
  ethPrice: EthPrice,
) =>
  pipe(
    Db.sqlT`
      DELETE FROM contract_base_fees
      WHERE block_number = ${block.number}
    `,
    T.chain(() => {
      const transactionSegments =
        Transactions.segmentTransactions(transactionReceipts);

      const feeSegments = BaseFees.sumFeeSegments(
        block,
        transactionSegments,
        ethPrice.ethusd,
      );

      const transactionCounts = Blocks.countTransactionsPerContract(
        transactionSegments.other,
      );

      return ContractBaseFees.storeContractBaseFees(
        block,
        feeSegments,
        transactionCounts,
      );
    }),
  );

const eta = makeEta({
  max: blocksToCheck.length,
});
let blocksDone = 0;

for (const blockNumber of blocksToCheck) {
  await pipe(
    T.Do,
    T.apS(
      "block",
      pipe(
        Blocks.getBlockSafe(blockNumber),
        TOAlt.expect("expected to get block ${blockNumber} from node"),
      ),
    ),
    T.bind("transactionReceipts", ({ block }) =>
      pipe(
        Transactions.getTransactionReceiptsSafe(block),
        TOAlt.expect(`transactions for ${blockNumber} came back null`),
      ),
    ),
    T.bind("segments", ({ transactionReceipts }) =>
      pipe(Transactions.segmentTransactions(transactionReceipts), T.of),
    ),
    T.bind("ethPrice", ({ block }) =>
      pipe(
        getEthPrice(block.timestamp, Duration.millisFromMinutes(10)),
        TEAlt.getOrThrow,
      ),
    ),
    T.bind("feeSums", ({ block, segments, ethPrice }) =>
      pipe(BaseFees.sumFeeSegments(block, segments, ethPrice.ethusd), T.of),
    ),
    T.apS(
      "storedContractFees",
      pipe(
        Db.sqlT<
          {
            contractAddress: string;
            baseFees_256: string | null;
          }[]
        >`
          SELECT
            contract_address,
            base_fees_256
          FROM contract_base_fees
          WHERE block_number = ${blockNumber}
        `,
        T.map(
          A.map((row) => ({
            baseFees256: pipe(row.baseFees_256, O.fromNullable, O.map(BigInt)),
            contractAddress: row.contractAddress,
          })),
        ),
      ),
    ),
    T.chain(
      ({
        block,
        ethPrice,
        feeSums,
        storedContractFees,
        transactionReceipts,
      }) => {
        const storedFees = pipe(
          storedContractFees,
          A.map(
            (entry) =>
              [
                entry.contractAddress,
                {
                  // baseFees: entry.baseFees,
                  baseFees256: O.toNullable(entry.baseFees256),
                },
              ] as [string, { baseFees: number; baseFees256: bigint | null }],
          ),
          Object.fromEntries,
        );

        const expectedFees = pipe(
          Array.from(feeSums.contractSumsEth.keys()),
          A.map((address) => [
            address,
            {
              // baseFees: feeSums.contractSumsEth.get(address)!,
              baseFees256: feeSums.contractSumsEthBI.get(address)!,
            },
          ]),
          Object.fromEntries,
        );

        if (!isDeepStrictEqual(storedFees, expectedFees)) {
          try {
            deepStrictEqual(storedFees, expectedFees);
          } catch (error) {
            Log.warn(`${blockNumber} actual not equal to expected`, error);
            return heal(block, transactionReceipts, ethPrice);
          }
        }

        return T.of(undefined);
      },
    ),
  )();

  if (blockNumber % 100 === 0 && blocksDone !== 0) {
    Log.debug(
      `blocks done: ${blocksDone}, eta: ${eta.estimate().toFixed(0)}s left`,
    );
    await storeLastAdded(blockNumber);
  }

  blocksDone++;
  eta.report(blocksDone);
}
