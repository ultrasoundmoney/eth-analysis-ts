import PQueue from "p-queue";
import * as Config from "./config.js";
import fs from "fs/promises";
import * as DateFns from "date-fns";
import * as Blocks from "./blocks.js";
import * as BaseFees from "./base_fees.js";
import { sql } from "./db.js";
import * as Log from "./log.js";
import * as Transactions from "./transactions.js";
import * as Leaderboards from "./leaderboards.js";
import * as EthPrices from "./eth_prices.js";
import * as LeaderboardsAll from "./leaderboards_all.js";
import { A, pipe, T, TE } from "./fp.js";

type HashBlock = {
  number: number;
  hash: string;
  ethPrice: number | null;
  minedAt: Date;
  gasUsed: BigInt;
};

const concurrency = 1;

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
  const sumsToRollback = await Leaderboards.getRangeBaseFees(
    block.number,
    block.number,
  )();
  await LeaderboardsAll.removeContractBaseFeeSums("eth", sumsToRollback.eth)();
  await LeaderboardsAll.removeContractBaseFeeSums("usd", sumsToRollback.usd)();

  const ethPrice =
    hashBlock.ethPrice !== null
      ? hashBlock.ethPrice
      : await pipe(
          EthPrices.getPriceForOldBlock(block),
          T.map((ethPrice) => ethPrice.ethusd),
        )();

  Log.debug(`updating block: ${block.number}`);
  await Blocks.updateBlock(block, txrs)();
  Log.debug(`done updating block: ${block.number}`);

  const contractBaseFees = BaseFees.calcBlockFeeBreakdown(
    block,
    txrs,
    ethPrice,
  );

  await LeaderboardsAll.addBlock(
    block.number,
    contractBaseFees.contract_use_fees,
    contractBaseFees.contract_use_fees_usd,
  )();
  return undefined;
};

try {
  const hashBlocks = await (lastHealedBlock === undefined
    ? sql<HashBlock[]>`
        SELECT number, hash, eth_price, mined_at, gas_used FROM blocks
        ORDER BY number DESC
      `
    : sql<HashBlock[]>`
        SELECT number, hash, eth_price, mined_at, gas_used FROM blocks
        WHERE number < ${lastHealedBlock}
        ORDER BY number DESC
      `);

  for (const hashBlockChunk of A.chunksOf(8)(hashBlocks)) {
    Log.info(
      `checking block: ${
        hashBlockChunk[hashBlockChunk.length - 1].number
      }, to block: ${hashBlockChunk[0].number}`,
    );
    await healBlockQueue.addAll(
      hashBlockChunk.map((hashBlock) => () => healBlock(hashBlock)),
    );
    await fs.writeFile(
      `./last_healed_block_${Config.getEnv()}`,
      JSON.stringify(hashBlockChunk[hashBlockChunk.length - 1].number),
    );
  }
} catch (error) {
  Log.error(error);
  throw error;
}
