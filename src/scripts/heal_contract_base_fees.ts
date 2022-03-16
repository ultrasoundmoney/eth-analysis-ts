import makeEta from "simple-eta";
import * as BaseFees from "../base_fees.js";
import * as Blocks from "../blocks/blocks.js";
import * as ContractBaseFees from "../contract_base_fees.js";
import { sql, sqlT } from "../db.js";
import * as Duration from "../duration.js";
import { getEthPrice } from "../eth-prices/eth_prices.js";
import { A, O, pipe, T, TEAlt, TOAlt } from "../fp.js";
import * as Log from "../log.js";
import * as Transactions from "../transactions.js";

const lastStoredBlock = await Blocks.getLastStoredBlock()();

const lastCheckedBlock = await sql<{ value: number | null }[]>`
  SELECT "value" FROM key_value_store
  WHERE "key" = 'last-checked-transaction-block'
`.then((rows) => rows[0]?.value);

const blocksToCheck = Blocks.getBlockRange(
  lastCheckedBlock ?? Blocks.londonHardForkBlockNumber,
  lastStoredBlock.number,
);

const storeLastAdded = (blockNumber: number) => sql`
  INSERT INTO key_value_store
    (key, value)
  VALUES
    ('last-checked-transaction-block', ${sql.json(blockNumber)})
  ON CONFLICT (key) DO UPDATE SET
    value = excluded.value
`;

const healBlock = async (
  block: Blocks.BlockNodeV2,
  transactionReceipts: Transactions.TransactionReceiptV1[],
) => {
  const ethPrice = await pipe(
    getEthPrice(block.timestamp, Duration.millisFromMinutes(10)),
    TEAlt.getOrThrow,
  )();

  await sql`
    DELETE FROM contract_base_fees
    WHERE block_number = ${block.number}
  `;

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
  await ContractBaseFees.storeContractBaseFees(
    block,
    feeSegments,
    transactionCounts,
  )();
};

const eta = makeEta({
  max: blocksToCheck.length,
});
let blocksDone = 0;

for (const blockNumber of blocksToCheck) {
  const block = await Blocks.getBlockSafe(blockNumber)();
  if (O.isNone(block)) {
    throw new Error(`failed to get block ${blockNumber}`);
  }
  const transactionReceipts = await pipe(
    Transactions.getTransactionReceiptsSafe(block.value),
    TOAlt.getOrThrow(`transactions for ${blockNumber} came back null`),
  )();
  const otherReceipts =
    Transactions.segmentTransactions(transactionReceipts).other;

  const contractOtherAddresses = pipe(
    otherReceipts,
    A.map((receipt) => receipt.to),
    A.compact,
    (addresses) => new Set(addresses),
  );
  const storedContractAddresses = await pipe(
    sqlT<{ contractAddress: string }[]>`
      SELECT contract_address FROM contract_base_fees
      WHERE block_number = ${blockNumber}
    `,
    T.map((rows) => rows.map((row) => row.contractAddress)),
    T.map((addresses) => new Set(addresses)),
  )();
  const missing = new Array(...contractOtherAddresses).filter(
    (address) => !storedContractAddresses.has(address),
  );
  const wrong = new Array(...storedContractAddresses).filter(
    (address) => !contractOtherAddresses.has(address),
  );

  if (missing.length !== 0) {
    Log.debug(`block ${blockNumber}, ${missing.length} missing addresses`);
    await healBlock(block.value, transactionReceipts);
  } else {
    if (wrong.length !== 0) {
      Log.debug(`block ${blockNumber}, ${wrong.length} bad addresses stored`);
      await healBlock(block.value, transactionReceipts);
    }
  }

  if (blockNumber % 100 === 0 && blocksDone !== 0) {
    Log.debug(
      `blocks done: ${blocksDone}, eta: ${eta.estimate().toFixed(0)}s left`,
    );
    await storeLastAdded(blockNumber);
  }

  blocksDone++;
  eta.report(blocksDone);
}
