import PQueue from "p-queue";
import * as DateFns from "date-fns";
import Web3 from "web3";
import web3Core from "web3-core";
import { Contract } from "web3-eth-contract";
import { AbiItem } from "web3-utils";
import WebSocket from "ws";
import * as Config from "./config.js";
import * as Duration from "./duration.js";
import { pipe, T } from "./fp.js";
import * as Hexadecimal from "./hexadecimal.js";
import * as Log from "./log.js";

// TODO: restart whole service when websocket fails.
// Try own node three times, then switch to fallback node.

type LogWeb3 = web3Core.Log;
const WebsocketProvider = web3Core.WebsocketProvider;

let managedWeb3Obj: Web3 | undefined = undefined;

let managedGethWs: WebSocket | undefined = undefined;

const messageListners = new Map();

let wsAttempt = 0;

const connect = async (): Promise<WebSocket> => {
  if (Config.getUseNodeFallback()) {
    Log.warn("using node fallback");
  }

  // Try our own node three times then try our third party fallback
  const ws =
    wsAttempt < 3 && !Config.getUseNodeFallback()
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

const getGethWs = () => connectionQueue.add(getOpenSocketOrReconnect);

export const closeConnections = async () => {
  if (connectionQueue.size !== 0) {
    await connectionQueue.onEmpty();
  }

  if (managedGethWs !== undefined) {
    managedGethWs.close();
  }

  if (
    managedWeb3Obj !== undefined &&
    managedWeb3Obj.currentProvider !== null &&
    managedWeb3Obj.currentProvider instanceof WebsocketProvider
  ) {
    managedWeb3Obj.currentProvider.disconnect(0, "exiting");
  }
};

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

/**
 * A block as it comes in from an eth node.
 */
export type BlockNodeV1 = {
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

// NOTE: Some blocks get dropped, queries come back null.
export const getBlock = async (
  query: number | "latest" | string,
): Promise<BlockNodeV1 | undefined> => {
  const [id, messageP] = registerMessageListener<BlockNodeV1>();

  const numberAsHex =
    query === "latest"
      ? "latest"
      : typeof query === "string"
      ? query
      : Hexadecimal.hexFromNumber(query);

  send({
    method: "eth_getBlockByNumber",
    params: [numberAsHex, false],
    id,
    jsonrpc: "2.0",
  });

  const rawBlock = await messageP;

  return rawBlock;
};

export const getRawBlockByHash = async (
  hash: string,
): Promise<BlockNodeV1 | null> => {
  const [id, messageP] = registerMessageListener<BlockNodeV1>();

  send({
    method: "eth_getBlockByHash",
    params: [hash, false],
    id,
    jsonrpc: "2.0",
  });

  const rawBlock = await messageP;

  return rawBlock;
};

export type RawTxr = {
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

// NOTE: Some blocks get dropped, receipts get dropped, queries come back null.
export const getTransactionReceipt = async (
  hash: string,
): Promise<RawTxr | null> => {
  const [id, messageP] = registerMessageListener<RawTxr>();

  send({
    method: "eth_getTransactionReceipt",
    params: [hash],
    id,
    jsonrpc: "2.0",
  });

  const rawTxr = await messageP;

  return rawTxr;
};

const translateHead = (rawHead: RawHead): Head => ({
  hash: rawHead.hash,
  number: Hexadecimal.numberFromHex(rawHead.number),
  parentHash: rawHead.parentHash,
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

    const formatter = new Intl.NumberFormat("en", { maximumFractionDigits: 2 });
    Log.debug(
      `miner to api block staleness ${formatter.format(
        DateFns.differenceInSeconds(
          new Date(),
          DateFns.fromUnixTime(Number(rawHead.timestamp)),
        ),
      )}s, timestamp: ${new Date().toISOString()}`,
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
  hash: string;
  number: number;
  parentHash: string;
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

  return Hexadecimal.numberFromHex(rawBlockNumber);
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
