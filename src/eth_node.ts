import WebSocket from "ws";
import { hexToNumber, numberToHex } from "./hexadecimal.js";
import { TxRWeb3London } from "./transactions.js";
import type { Log as LogWeb3 } from "web3-core";
import PQueue from "p-queue";
import ProgressBar from "progress";
import * as Blocks from "./blocks.js";
import * as Log from "./log.js";

const mainnetNode = `ws://${process.env.NODE_IP}:8546/`;

let ws: WebSocket | undefined = undefined;

export const connect = async () => {
  ws = new WebSocket(mainnetNode);
  return new Promise((resolve) => {
    ws!.on("open", resolve);
  });
};

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
  ws!.send(JSON.stringify(message));
};

export const benchmarkTxrFetch = async () => {
  const blockQueue = new PQueue({ concurrency: 4 });
  const txrsQueue = new PQueue({ concurrency: 200 });
  await connect();
  console.log("connected!");
  const block = await getBlock("latest");

  const blockRange = Blocks.getBlockRange(block!.number - 1000, block!.number);

  const blocks = await Promise.all(blockRange.map(getBlock));

  const bar = new ProgressBar(">> [:bar] :rate/s :percent :etas", {
    total: blocks.length,
  });

  await blockQueue.addAll(
    blocks.map((block) => async () => {
      await txrsQueue.addAll(
        block!.transactions.map(
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
// NOTE: this is not safe. We lose precision here. Convert these to big int at some point.
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
): Promise<BlockLondon | undefined> => {
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

  // NOTE: Some blocks come back as null. Unclear why.
  if (rawBlock === null) {
    return undefined;
  }

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
): Promise<TxRWeb3London | undefined> => {
  const [id, messageP] = registerMessageListener<RawTxr>();

  send({
    method: "eth_getTransactionReceipt",
    params: [hash],
    id,
    jsonrpc: "2.0",
  });

  const rawTxr = await messageP;

  // NOTE: Some txrs come back as null. Unclear why.
  if (rawTxr === null) {
    return undefined;
  }

  return translateTxr(rawTxr);
};

ws!.on("message", (event) => {
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

export const closeConnection = () => {
  ws!.close();
};

const translateHead = (rawHead: RawHead): Head => ({
  ...rawHead,
  number: hexToNumber(rawHead.number),
});

export const subscribeNewHeads = (
  handleNewHead: (head: Head) => Promise<void>,
) => {
  const headsWs = new WebSocket(mainnetNode);
  let gotSubscription = false;

  headsWs.on("close", () => {
    Log.warn("heads ws closed, reconnecting!");
    subscribeNewHeads(handleNewHead);
  });

  headsWs.on("error", (error) => {
    Log.error("heads ws error", { error });
  });

  headsWs.on("message", (data) => {
    if (!gotSubscription) {
      gotSubscription = true;
      return;
    }

    const rawHead: RawHead = JSON.parse(data.toString()).params.result;
    const head = translateHead(rawHead);

    handleNewHead(head);
  });

  headsWs.on("open", () => {
    headsWs.send(
      JSON.stringify({ id: 1, method: "eth_subscribe", params: ["newHeads"] }),
    );
  });
};

type RawHead = {
  parentHash: string;
  sha3Uncles: string;
  miner: string;
  stateRoot: string;
  transactionsRoot: string;
  receiptsRoot: string;
  logsBloom: string;
  difficulty: string;
  number: string;
  gasLimit: string;
  gasUsed: string;
  timestamp: string;
  extraData: string;
  mixHash: string;
  nonce: string;
  baseFeePerGas: string;
  hash: string;
};

type Head = {
  parentHash: string;
  sha3Uncles: string;
  miner: string;
  stateRoot: string;
  transactionsRoot: string;
  receiptsRoot: string;
  logsBloom: string;
  difficulty: string;
  number: number;
  gasLimit: string;
  gasUsed: string;
  timestamp: string;
  extraData: string;
  mixHash: string;
  nonce: string;
  baseFeePerGas: string;
  hash: string;
};
