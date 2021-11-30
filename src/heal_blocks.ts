import * as DateFns from "date-fns";
import fs from "fs/promises";
import PQueue from "p-queue";
import * as Blocks from "./blocks.js";
import * as Config from "./config.js";
import { sql } from "./db.js";
import * as EthNode from "./eth_node.js";
import * as EthPrices from "./eth_prices.js";
import { pipe, T, TE } from "./fp.js";
import * as Log from "./log.js";
import * as Transactions from "./transactions.js";

type HashBlock = {
  number: number;
  hash: string;
  ethPrice: number | null;
  minedAt: Date;
  gasUsed: BigInt;
};

const concurrency = 8;

const healBlockQueue = new PQueue({ concurrency });

type FsError = {
  errno: number;
  code: "ENOENT" | string;
  syscall: string;
  path: string;
};

const lastHealedBlock: number | undefined = await pipe(
  TE.tryCatch(
    () => fs.readFile(`./last_healed_block_${Config.getEnv()}.json`, "utf8"),
    (error) => {
      const fsError = error as FsError;
      return fsError;
    },
  ),
  TE.map(JSON.parse),
  TE.match(
    (error) => {
      if (error.code === "ENOENT") {
        return undefined;
      }

      throw error;
    },
    (v) => v,
  ),
)();

const healBlock = async (hashBlock: HashBlock) => {
  const block = await Blocks.getBlockWithRetry(hashBlock.number);
  if (hashBlock.hash === block.hash) {
    return undefined;
  }

  const minedAtIso = DateFns.formatISO(hashBlock.minedAt);
  Log.warn(
    `block: ${block.number}, mined at: ${minedAtIso} ago, hash mismatch, gas used old:  ${hashBlock.gasUsed}, new: ${block.gasUsed} healing block`,
  );
  const txrs = await Transactions.getTxrsWithRetry(block);

  const ethPrice =
    hashBlock.ethPrice !== null
      ? hashBlock.ethPrice
      : await pipe(
          EthPrices.getPriceForOldBlock(block),
          T.map((ethPrice) => ethPrice.ethusd),
        )();

  await Blocks.updateBlock(block, txrs, ethPrice)();

  return undefined;
};

try {
  const processChunk = async (blockChunk: HashBlock[]): Promise<void> => {
    Log.info(
      `checking block: ${blockChunk[blockChunk.length - 1].number}, to block: ${
        blockChunk[0].number
      }`,
    );

    await healBlockQueue.addAll(
      blockChunk.map((hashBlock) => () => healBlock(hashBlock)),
    );

    await fs.writeFile(
      `./last_healed_block_${Config.getEnv()}.json`,
      JSON.stringify(blockChunk[blockChunk.length - 1].number),
    );

    return undefined;
  };

  Log.info("healing blocks");

  const latestBlockNumber = await sql<{ number: number }[]>`
      SELECT number FROM blocks ORDER BY number DESC LIMIT 1
    `.then((rows) => rows[0]?.number);

  let offset =
    lastHealedBlock !== undefined ? latestBlockNumber - lastHealedBlock : 0;

  const getBlocksWithLimit = () =>
    sql<HashBlock[]>`
      SELECT number, hash, eth_price, mined_at, gas_used FROM blocks
      ORDER BY number DESC
      OFFSET ${offset}
      LIMIT 10000
    `;

  let blocks = await getBlocksWithLimit();
  while (blocks.length !== 0) {
    await processChunk(blocks);
    offset = offset + 10000;
    blocks = await getBlocksWithLimit();
  }

  Log.info(`done healing blocks, queue length: ${healBlockQueue.size}`);
  await healBlockQueue.onIdle();
  Log.info(`done healing blocks, queue length: ${healBlockQueue.size}`);

  EthNode.closeConnection();
  await sql.end();
} catch (error) {
  Log.error(error);
  throw error;
}
