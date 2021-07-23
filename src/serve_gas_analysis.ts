import * as Blocks from "./blocks";
import * as Transactions from "./transactions";
import type { TransactionReceipt } from "./transactions";
import * as Log from "./log";
import { pipe } from "fp-ts/lib/function";
import A from "fp-ts/lib/Array";
import QuickLru from "quick-lru";
import Koa, { Middleware } from "koa";

type AggregatedGasUse = Record<string, number>;

type GasUser = {
  name: string;
  fee: number;
  image: undefined;
};

const getAggregateContractGasUse = (
  transactionReceipts: TransactionReceipt[],
): GasUser[] => {
  const table: AggregatedGasUse = {};

  transactionReceipts.forEach((transactionReceipt) => {
    if (transactionReceipt.to === null) {
      return;
    }
    if (
      transactionReceipt.to === "0x11f09fe012f66881245eaaf83a4f614e6cc39a1f"
    ) {
    }
    const aggregateUse = table[transactionReceipt.to] || 0;
    table[transactionReceipt.to] =
      aggregateUse +
      (transactionReceipt.gasUsed * transactionReceipt.effectiveGasPrice) /
        10 ** 18;
  });

  return Object.entries(table).map(([address, gasUsed]) => ({
    name: address,
    fee: gasUsed,
    image: undefined,
  }));
};

type KnownContracts = Record<string, string>;

const decodeContracts = (
  knownContracts: KnownContracts,
  aggregatedGasUse: AggregatedGasUse,
): Record<string, number> =>
  pipe(
    aggregatedGasUse,
    Object.entries,
    A.map(([key, value]) => [knownContracts[key] || key, value]),
    Object.fromEntries,
  );

// TODO: build
// Get transactions of last 45,000 blocks.
// Retrieve from network, cache in DB.
// Aggregate gas usage by address.
// Identify transactions.
// Identify burn leader addresses.
// Cache warmer

// ~6.88 days
const weekOfBlocksCount = 45000;

// ~ 1 day
const dayOfBlocksCount = 6545;

// ~ 1 h
const hourOfBlocksCount = 272;

const getAggregateEthTransferGasUse = (
  transactionsReceipts: TransactionReceipt[],
): number =>
  pipe(
    transactionsReceipts,
    A.reduce(
      0,
      (sum, transactionsReceipt) =>
        sum +
        (transactionsReceipt.gasUsed * transactionsReceipt.effectiveGasPrice) /
          10 ** 18,
    ),
  );

const topGasUserCache = new QuickLru({ maxSize: 1, maxAge: 3600000 });
const topGasUserCacheKey = "top-gas-users-key";

const handleAnyRequest: Middleware = async (ctx) => {
  // Respond from cache if we can.
  const cTopGasUsers = topGasUserCache.get(topGasUserCacheKey);
  if (cTopGasUsers !== undefined) {
    ctx.res.writeHead(200, {
      "Content-Type": "application/json",
    });
    ctx.res.end(cTopGasUsers);
    return;
  }

  const transactionHashes = await Blocks.getLastNBlocksTransactionHashes(
    weekOfBlocksCount,
  );

  const transactionReceipts = await Transactions.getTransactionReceipts(
    transactionHashes,
  );

  // Filter out transactions we don't want to count
  // 1. to `null` which create new contracts
  const transactionsReceiptsWithoutContractCreations =
    transactionReceipts.filter((txr) => txr.to !== null);

  // Segment transaction receipts
  const { left: contractTransactions, right: ethTransferTransactions } = pipe(
    transactionsReceiptsWithoutContractCreations,
    A.partition((transactionReceipt) => transactionReceipt.gasUsed === 21000),
  );

  const aggregateEthTransferUse = getAggregateEthTransferGasUse(
    ethTransferTransactions,
  );
  const contractsGasUsed = getAggregateContractGasUse(contractTransactions);

  const topTenGasUsers = pipe(
    [
      ...contractsGasUsed,
      {
        image: undefined,
        name: "eth transfer",
        fee: aggregateEthTransferUse,
      },
    ],
    A.sort<GasUser>({
      compare: (first, second) =>
        first.fee === second.fee ? 0 : first.fee > second.fee ? -1 : 1,
      equals: (first: GasUser, second: GasUser) => first.fee === second.fee,
    }),
    A.takeLeft(10),
  );

  // Cache the response
  const topTenGasUsersJson = JSON.stringify(topTenGasUsers);
  topGasUserCache.set(topGasUserCacheKey, topTenGasUsersJson);

  ctx.res.writeHead(200, { "Content-Type": "application/json" });
  ctx.res.end(topTenGasUsersJson);
};

const port = process.env.PORT || 8080;

const app = new Koa();

app.use(handleAnyRequest);

app.listen(port, () => {
  Log.info(`> listening on ${port}`);
});
