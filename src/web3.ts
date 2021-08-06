import WebSocket from "ws";
import Config from "./config.js";
import { hexToNumber, numberToHex } from "./numbers.js";
import { TxRWeb3London } from "./transactions.js";
// eslint-disable-next-line node/no-unpublished-import
import type { Log as LogWeb3 } from "web3-core";
import PQueue from "p-queue";
import ProgressBar from "progress";
import * as Blocks from "./blocks.jsx";

const mainnetNode = Config.localNodeAvailable
  ? "ws://localhost:8546/"
  : "ws://3.15.217.72:8546/";

const ws = new WebSocket(mainnetNode);

type MessageErr = { code: number; message: string };

let randomInts: number[] = [];

const getNewMessageId = (): number => {
  if (randomInts.length === 0) {
    randomInts = new Array(10000).fill(undefined).map((_, i) => i);
  }

  // We fill the array when it's empty above. pop() can't return undefined.
  return randomInts.pop()!;
};

const messageListners = new Map();

const registerMessageListener = <A>(): [number, Promise<A>] => {
  const id = getNewMessageId();
  const messageP: Promise<A> = new Promise((resolve, reject) => {
    messageListners.set(id, (err: MessageErr, data: A) => {
      messageListners.delete(id);
      if (err !== null) {
        reject(err);
      } else {
        resolve(data);
      }
    });
  });

  return [id, messageP];
};

const send = (message: Record<string, unknown>) => {
  ws.send(JSON.stringify(message));
};

type RawBlock = {
  baseFeePerGas: string;
  gasUsed: string;
  difficulty: string;
  extraData: string;
  gasLimit: string;
  hash: string;
  logsBloom: string;
  miner: string;
  mixHash: string;
  nonce: string;
  number: string;
  parentHash: string;
  receiptsRoot: string;
  sha3Uncles: string;
  size: string;
  stateRoot: string;
  timestamp: string;
  totalDifficulty: string;
  transactions: string[];
  transactionsRoot: string;
  uncles: string[];
};

export type BlockLondon = {
  baseFeePerGas: string;
  difficulty: string;
  extraData: string;
  gasLimit: number;
  gasUsed: number;
  hash: string;
  logsBloom: string;
  miner: string;
  mixHash: string;
  nonce: string;
  number: number;
  parentHash: string;
  receiptsRoot: string;
  sha3Uncles: string;
  size: number;
  stateRoot: string;
  timestamp: number;
  totalDifficulty: string;
  transactions: string[];
  transactionsRoot: string;
  uncles: string[];
};

// Mimics web3.js translation of fields.
const translateBlock = (rawBlock: RawBlock): BlockLondon => ({
  ...rawBlock,
  baseFeePerGas: rawBlock.baseFeePerGas,
  gasUsed: hexToNumber(rawBlock.gasUsed),
  gasLimit: hexToNumber(rawBlock.gasLimit),
  number: hexToNumber(rawBlock.number),
  size: hexToNumber(rawBlock.number),
  timestamp: hexToNumber(rawBlock.timestamp),
});

export const getBlock = async (
  number: number | "latest" | string,
): Promise<BlockLondon> => {
  const [id, messageP] = registerMessageListener<RawBlock>();

  const numberAsHex =
    number === "latest"
      ? "latest"
      : typeof number === "string"
      ? number
      : numberToHex(number);

  send({
    method: "eth_getBlockByNumber",
    params: [numberAsHex, false],
    id,
    jsonrpc: "2.0",
  });

  const rawBlock = await messageP;
  return translateBlock(rawBlock);
};

type RawTxr = {
  blockHash: string;
  blockNumber: string;
  contractAddress: string | null;
  cumulativeGasUsed: string;
  effectiveGasPrice: string;
  from: string;
  gasUsed: string;
  logs: LogWeb3[];
  logsBloom: string;
  status: string;
  to: string;
  transactionHash: string;
  transactionIndex: string;
  type: string;
};

const statusToNumber = (rawStatus: string): boolean => {
  if (rawStatus === "0") {
    return false;
  }

  if (rawStatus !== "1") {
    return true;
  }

  throw new Error(`unexpected status string: ${rawStatus}`);
};

const translateTxr = (rawTrx: RawTxr): TxRWeb3London => ({
  ...rawTrx,
  status: statusToNumber(rawTrx.status),
  transactionIndex: hexToNumber(rawTrx.transactionIndex),
  blockNumber: hexToNumber(rawTrx.blockNumber),
  contractAddress: rawTrx.contractAddress || undefined,
  cumulativeGasUsed: hexToNumber(rawTrx.cumulativeGasUsed),
  gasUsed: hexToNumber(rawTrx.gasUsed),
});

export const getTransactionReceipt = async (
  hash: string,
): Promise<TxRWeb3London | null> => {
  const [id, messageP] = registerMessageListener<RawTxr>();

  send({
    method: "eth_getTransactionReceipt",
    params: [hash],
    id,
    jsonrpc: "2.0",
  });

  const rawTxr = await messageP;

  if (rawTxr === null) {
    return rawTxr;
  }

  return translateTxr(rawTxr);
};

ws.on("message", (event) => {
  const message: {
    id: number;
    result: unknown;
    error: { code: number; message: string };
  } = JSON.parse(event.toString());
  const cb = messageListners.get(message.id);
  if (cb !== undefined) {
    if ("error" in message) {
      cb(message.error);
    } else {
      cb(null, message.result);
    }
  }
});

export const closeWeb3Ws = () => {
  ws.close();
};

export const webSocketOpen = new Promise((resolve) => {
  ws.on("open", resolve);
});

export const benchmarkTxrFetch = async () => {
  const blockQueue = new PQueue({ concurrency: 4 });
  const txrsQueue = new PQueue({ concurrency: 200 });
  await webSocketOpen;
  console.log("connected!");
  const block = await getBlock("latest");

  const blockRange = Blocks.getBlockRange(block.number - 1000, block.number);

  const blocks = await Promise.all(blockRange.map(getBlock));

  const bar = new ProgressBar(">> [:bar] :rate/s :percent :etas", {
    total: blocks.length,
  });

  await blockQueue.addAll(
    blocks.map((block) => async () => {
      await txrsQueue.addAll(
        block.transactions.map(
          (hash) => () =>
            getTransactionReceipt(hash).then((txr) => {
              return txr;
            }),
        ),
      );
      bar.tick();
    }),
  );
};
