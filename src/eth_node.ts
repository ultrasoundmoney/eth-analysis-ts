import PQueue from "p-queue";
import ProgressBar from "progress";
import Web3 from "web3";
import { Log as LogWeb3 } from "web3-core";
import { Contract } from "web3-eth-contract";
import { AbiItem } from "web3-utils";
import WebSocket from "ws";
import * as Blocks from "./blocks/blocks.js";
import * as Config from "./config.js";
import * as Duration from "./duration.js";
import { pipe, T } from "./fp.js";
import { hexToNumber, numberToHex } from "./hexadecimal.js";
import * as Log from "./log.js";
import { TransactionReceiptV1 } from "./transactions.js";

let managedWeb3Obj: Web3 | undefined = undefined;

let managedGethWs: WebSocket | undefined = undefined;

const messageListners = new Map();

let wsAttempt = 0;

export const connect = async (): Promise<WebSocket> => {
  // Try our own node three times then try our third part fallback
  const ws =
    wsAttempt < 3
      ? new WebSocket(Config.getGethUrl())
      : new WebSocket(Config.getGethFallbackUrl());

  if (wsAttempt < 3) {
    wsAttempt = wsAttempt + 1;
  } else {
    Log.error("failed to connect to geth node, using fallback");
    wsAttempt = 0;
  }

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

  ws.on("close", () => {
    Log.info("geth node websocket closed, immediately reconnecting");
    getGethWs();
  });

  return new Promise((resolve) => {
    ws.on("open", () => {
      Log.debug("connected to eth node");
      resolve(ws);
    });
  });
};

type MessageErr = { code: number; message: string };

let nextId = 0;
const inUseIds = new Set<number>();

const getNewMessageId = (): number => {
  if (nextId === 1024) {
    nextId = 0;
  }

  // If more than 1024 messages are still waiting for a response, this loop starts eating CPU until an id frees up. We assume this is never the case, if it does start happening, increase the id pool.
  while (inUseIds.has(nextId)) {
    nextId = nextId + 1;
  }

  inUseIds.add(nextId);
  return nextId;
};

const registerMessageListener = <A>(): [number, Promise<A>] => {
  const id = getNewMessageId();
  const messageP: Promise<A> = new Promise((resolve, reject) => {
    messageListners.set(id, (err: MessageErr, data: A) => {
      messageListners.delete(id);
      inUseIds.delete(id);
      if (err !== null) {
        reject(err);
      } else {
        resolve(data);
      }
    });
  });

  return [id, messageP];
};

let web3Attempt = 0;

export const getWeb3 = (): Web3 => {
  if (managedWeb3Obj !== undefined) {
    return managedWeb3Obj;
  }

  const providerOptions = {
    reconnect: {
      auto: true,
      delay: Duration.millisFromSeconds(5),
      maxAttempts: 5,
    },
  };
  // Try our own node three times then try our third party fallback.
  const provider =
    web3Attempt < 3
      ? new Web3.providers.WebsocketProvider(
          Config.getGethUrl(),
          providerOptions,
        )
      : new Web3.providers.WebsocketProvider(
          Config.getGethFallbackUrl(),
          providerOptions,
        );

  if (web3Attempt < 3) {
    web3Attempt = wsAttempt + 1;
  } else {
    Log.error("failed to connect to geth node, using fallback");
    web3Attempt = 0;
  }

  managedWeb3Obj = new Web3(provider);
  return managedWeb3Obj;
};

const connectionQueue = new PQueue({ concurrency: 1 });

export const getGethWs = () => connectionQueue.add(getOpenSocketOrReconnect);

const getOpenSocketOrReconnect = async (): Promise<WebSocket> => {
  if (
    managedGethWs !== undefined &&
    managedGethWs.readyState === WebSocket.OPEN
  ) {
    return managedGethWs;
  }

  if (managedGethWs === undefined) {
    Log.debug("websocket undefined, initializing");
  } else {
    const isConnecting = managedGethWs.readyState === WebSocket.CONNECTING;
    const isClosing = managedGethWs.readyState === WebSocket.CLOSING;
    const isClosed = managedGethWs.readyState === WebSocket.CLOSED;
    Log.warn(
      `ws initialized but not open, not initial connect, closing=${isClosing}, closed=${isClosed}, connecting=${isConnecting}`,
    );
  }

  const ws = await connect();
  managedGethWs = ws;
  return ws;
};

const send = async (message: Record<string, unknown>) => {
  const connection = await getGethWs();
  connection.send(JSON.stringify(message));
};

export const benchmarkTxrFetch = async () => {
  const blockQueue = new PQueue({ concurrency: 4 });
  const txrsQueue = new PQueue({ concurrency: 200 });
  await connect();
  Log.info("connected!");
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
  baseFeePerGasBI: bigint;
  difficulty: string;
  extraData: string;
  gasLimit: number;
  gasUsed: number;
  gasUsedBI: bigint;
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

// NOTE: this is not safe. We lose precision here. Convert these to big int at some point.
const translateBlock = (rawBlock: RawBlock): BlockLondon => ({
  ...rawBlock,
  baseFeePerGas: rawBlock.baseFeePerGas,
  baseFeePerGasBI: BigInt(rawBlock.baseFeePerGas),
  gasUsed: hexToNumber(rawBlock.gasUsed),
  gasUsedBI: BigInt(rawBlock.gasUsed),
  gasLimit: hexToNumber(rawBlock.gasLimit),
  number: hexToNumber(rawBlock.number),
  size: hexToNumber(rawBlock.size),
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

export const getBlockByHash = async (
  hash: string,
): Promise<BlockLondon | undefined> => {
  const [id, messageP] = registerMessageListener<RawBlock>();

  send({
    method: "eth_getBlockByHash",
    params: [hash, false],
    id,
    jsonrpc: "2.0",
  });

  const rawBlock = await messageP;

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

const translateTxr = (rawTrx: RawTxr): TransactionReceiptV1 => ({
  ...rawTrx,
  status: statusToNumber(rawTrx.status),
  transactionIndex: hexToNumber(rawTrx.transactionIndex),
  blockNumber: hexToNumber(rawTrx.blockNumber),
  contractAddress: rawTrx.contractAddress || undefined,
  cumulativeGasUsed: hexToNumber(rawTrx.cumulativeGasUsed),
  gasUsed: hexToNumber(rawTrx.gasUsed),
  gasUsedBI: BigInt(rawTrx.gasUsed),
});

export const getTransactionReceipt = async (
  hash: string,
): Promise<TransactionReceiptV1 | undefined> => {
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

export const closeConnection = () => {
  if (managedGethWs !== undefined) {
    managedGethWs.close();
  }
};

const translateHead = (rawHead: RawHead): Head => ({
  ...rawHead,
  gasLimit: hexToNumber(rawHead.gasLimit),
  gasUsed: hexToNumber(rawHead.gasUsed),
  number: hexToNumber(rawHead.number),
  timestamp: hexToNumber(rawHead.timestamp),
});

let subscribeHeadsAttempt = 0;

export const subscribeNewHeads = (
  handleNewHead: (head: Head) => Promise<void>,
) => {
  const headsWs =
    subscribeHeadsAttempt < 3
      ? new WebSocket(Config.getGethUrl())
      : new WebSocket(Config.getGethFallbackUrl());

  if (!(subscribeHeadsAttempt < 3)) {
    Log.error(
      "failed to subscribe to geth node heads three times, using fallback",
    );
    subscribeHeadsAttempt = 0;
  }

  let gotSubscription = false;

  headsWs.on("close", () => {
    Log.warn("heads ws closed, reconnecting!");
    subscribeHeadsAttempt = subscribeHeadsAttempt + 1;
    subscribeNewHeads(handleNewHead);
  });

  headsWs.on("error", (error) => {
    Log.error("heads ws error", { error });
  });

  headsWs.on("message", (data) => {
    if (!gotSubscription) {
      Log.debug("got acknowledgement of new heads subscription");
      gotSubscription = true;
      return undefined;
    }

    const rawHead: RawHead = JSON.parse(data.toString()).params.result;
    const head = translateHead(rawHead);

    // const receivedAt = new Date().toISOString();
    // const minedAt = new Date(Number(head.timestamp) * 1000).toISOString();
    // Log.debug("new head", {
    //   number: head.number,
    //   hash: head.hash,
    //   parentHash: head.parentHash,
    //   receivedAt,
    //   minedAt,
    // });
    Log.debug(
      `new head, number: ${head.number}, hash: ${head.hash}, parent: ${head.parentHash}`,
    );
    handleNewHead(head);
    return undefined;
  });

  headsWs.on("open", () => {
    headsWs.send(
      JSON.stringify({ id: 1, method: "eth_subscribe", params: ["newHeads"] }),
    );
  });
};

export type RawHead = {
  baseFeePerGas: string;
  difficulty: string;
  extraData: string;
  gasLimit: string;
  gasUsed: string;
  hash: string;
  logsBloom: string;
  miner: string;
  mixHash: string;
  nonce: string;
  number: string;
  parentHash: string;
  receiptsRoot: string;
  sha3Uncles: string;
  stateRoot: string;
  timestamp: string;
  transactionsRoot: string;
};

export type Head = {
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
  stateRoot: string;
  timestamp: number;
  transactionsRoot: string;
};

// Doesn't seem to do anything.
export const raiseLogLevel = async () => {
  // {"method": "debug_vmodule", "params": [number]}
  const [id] = registerMessageListener();

  send({
    method: "debug_vmodule",
    params: [6],
    id,
    jsonrpc: "2.0",
  });

  inUseIds.delete(id);
};

export const getLatestBlockNumber = async (): Promise<number> => {
  const [id, messageP] = registerMessageListener<string>();

  send({
    method: "eth_blockNumber",
    id,
    jsonrpc: "2.0",
  });

  const rawBlockNumber = await messageP;

  return hexToNumber(rawBlockNumber);
};

export const makeContract = (address: string, abi: AbiItem[]): Contract => {
  const Contract = getWeb3().eth.Contract;
  const contract = new Contract(abi, address);
  // NOTE: possible workaround as web3 leaks memory.
  // See: https://github.com/ChainSafe/web3.js/issues/3042#issuecomment-663622882
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  (contract as any).setProvider(getWeb3().currentProvider);
  return contract;
};

export const getBalance = (address: string) =>
  pipe(() => getWeb3().eth.getBalance(address), T.map(BigInt));
