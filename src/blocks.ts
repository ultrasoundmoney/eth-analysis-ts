import { Row, RowList } from "postgres";
import { sql } from "./db";
import { eth } from "./web3";
import * as Log from "./log";
import * as Transactions from "./transactions";
import type { Block as BlockWeb3 } from "web3-eth/types/index";

export type Block = {
  number: number;
  hash: string;
  parentHash: string;
  transactions: string[];
};

const storeBlock = (block: BlockWeb3): Promise<RowList<Row[]>> => sql`
    INSERT INTO blocks (hash, number, json)
    VALUES (${block.hash}, ${block.number}, ${sql.json(block)})
    ON CONFLICT DO NOTHING`;

let cachedLatestBlock: Block | undefined = undefined;
// Blocks could be found faster, we accept a worst-case 10s delay.
const ethAverageBlockTime = 13000;

export const getLatestBlock = async (): Promise<Block> => {
  // Serve from cache if we can.
  if (cachedLatestBlock !== undefined) {
    return cachedLatestBlock;
  }

  Log.debug(`> fetching latest block`);
  const block = await eth.getBlock("latest");

  // Cache if we cannot
  cachedLatestBlock = block;
  setTimeout(() => {
    cachedLatestBlock = undefined;
  }, ethAverageBlockTime);

  storeBlock(block);

  return block;
};

const getLatestStoredBlock = (): Promise<Block | undefined> =>
  sql`
    SELECT * FROM blocks
    ORDER BY number DESC
    LIMIT 1
  `.then((result) => result[0]?.json as Block | undefined);

export const syncBlocks = async (): Promise<void> => {
  const [latestBlock, latestStoredBlock] = await Promise.all([
    getLatestBlock(),
    getLatestStoredBlock(),
  ]);

  // Use 0 if we've never stored blocks before.
  const lastStoredBlockNumber = latestStoredBlock?.number ?? 0;

  const blocksMissingCount = latestBlock.number - lastStoredBlockNumber;

  // Don't fetch more than a day worth of blocks
  const blocksToFetchCount =
    blocksMissingCount > 7000 ? 7000 : blocksMissingCount;

  Log.info(`> latest is ${latestBlock.number}`);
  Log.info(`> latest stored is ${latestStoredBlock?.number}`);
  Log.info(`> missing ${blocksToFetchCount} blocks, fetching ...`);

  const blockNumbersToFetch = new Array(blocksToFetchCount)
    .fill(undefined)
    .map((_, i) => latestBlock.number - i);

  for (const blockNumberToFetch of blockNumbersToFetch) {
    const block = await eth.getBlock(blockNumberToFetch);
    await Transactions.syncTransactionReceipts(block.transactions);
    // Store the block after the receipts so we refetch the receipts if we fail halfway.
    await storeBlock(block);
  }

  Log.info("> done syncing blocks");
};

export const getLastNBlocksTransactionHashes = (
  count: number,
): Promise<string[]> =>
  sql`
    SELECT
      number,
      json -> 'transactions' AS transactions
    FROM blocks
    ORDER BY number DESC
    LIMIT ${count}
  `
    .then((result) => {
      if (result.length < count) {
        Log.warn(`> asked for ${count} blocks, got ${result.length}`);
      }
      return result;
    })
    .then((result) => result.flatMap((row) => row.transactions));
