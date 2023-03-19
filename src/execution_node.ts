import * as DateFns from "date-fns";
import PQueue from "p-queue";
import Web3 from "web3";
import web3Core from "web3-core";
import { Contract } from "web3-eth-contract";
import { AbiItem } from "web3-utils";
import WebSocket from "ws";
import * as Config from "./config.js";
import * as Hexadecimal from "./hexadecimal.js";
import * as Log from "./log.js";

// TODO: restart whole service when websocket fails.
// Try own node three times, then switch to fallback node.

type MessageErr = { code: number; message: string };

let nextId = 0;
const inUseIds = new Set<number>();

const getNewMessageId = (): number => {
  if (nextId === 1024) {
    nextId = 0;
  }

  if (inUseIds.size === 1024) {
    Log.alert("message id pool hit limit, increase pool size or reduce wait!");
  }

  while (inUseIds.has(nextId)) {
    nextId = nextId + 1;
  }

  inUseIds.add(nextId);
  return nextId;
};

const registerMessageListener = <A>(): [number, Promise<A>] => {
  const id = getNewMessageId();
  const messageP: Promise<A> = new Promise((resolve, reject) => {
    const messageCallback = (err: MessageErr, data: A) => {
      messageCallbackMap.delete(id);
      inUseIds.delete(id);
      if (err !== null) {
        reject(err);
      } else {
        resolve(data);
      }
    };
    messageCallbackMap.set(id, messageCallback);
  });

  return [id, messageP];
};

let managedWeb3Obj: Web3 | undefined = undefined;

let managedGethWs: WebSocket | undefined = undefined;

const messageCallbackMap = new Map();

const makeGethWs = (url: string) => {
  const ws = new WebSocket(url);

  return new Promise<WebSocket>((resolve, reject) => {
    ws.on("close", () => {
      Log.info("geth node websocket closed");
    });

    ws.on("error", (error) => {
      reject(error);
    });

    ws.on("open", () => {
      Log.debug("connected to eth node");
      resolve(ws);
    });
  });
};

const connectWithFallback = async () => {
  if (Config.getUseNodeFallback()) {
    Log.warn("using geth node fallback");
    const ws = await makeGethWs(Config.getGethFallbackUrl());
    return ws;
  }

  try {
    const ws = await makeGethWs(Config.getGethUrl());
    return ws;
  } catch (error) {
    Log.alert("failed to connect to own node ws, using fallback", error);
    const ws = await makeGethWs(Config.getGethFallbackUrl());
    return ws;
  }
};

type Message =
  | {
      id: number;
      result: unknown;
    }
  | {
      id: number;
      error: { code: number; message: string };
    };

const connectGeth = async (): Promise<WebSocket> => {
  const ws = await connectWithFallback();
  ws.on("message", (event) => {
    const message = JSON.parse(event.toString()) as Message;

    const cb = messageCallbackMap.get(message.id);

    if (cb === undefined) {
      Log.error("got message without matching callback in callback map");
      return;
    }

    if ("error" in message) {
      cb(message.error);
    } else {
      cb(null, message.result);
    }
  });

  return ws;
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
      `ws initialized but not open, closing=${isClosing}, closed=${isClosed}, connecting=${isConnecting}`,
    );
  }

  const ws = await connectGeth();
  managedGethWs = ws;
  return ws;
};

const gethWsSeqQueue = new PQueue({ concurrency: 1 });

const getGethWs = () => gethWsSeqQueue.add(getOpenSocketOrReconnect);

const connectWeb3 = (url: string) =>
  new Promise<web3Core.WebsocketProvider>((resolve, reject) => {
    const provider = new Web3.providers.WebsocketProvider(url, {
      timeout: 8000,
      reconnect: {
        auto: true,
        delay: 3000,
        maxAttempts: 3,
      },
    });

    provider.on("error", ((error: unknown) => {
      reject(error);
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
    }) as any);

    provider.on("connect", () => {
      resolve(provider);
    });

    provider.connect();
  });

const connectWeb3WithFallback = async () => {
  if (Config.getUseNodeFallback()) {
    const provider = await connectWeb3(Config.getGethFallbackUrl());
    return new Web3(provider);
  }

  try {
    const provider = await connectWeb3(Config.getGethUrl());
    return new Web3(provider);
  } catch (error) {
    Log.alert(
      "failed to connect to own node for web3js, using fallback",
      error,
    );
    const provider = await connectWeb3(Config.getGethFallbackUrl());
    return new Web3(provider);
  }
};

export const getExistingWeb3OrReconnect = async (): Promise<Web3> => {
  if (
    managedWeb3Obj !== undefined &&
    (managedWeb3Obj.currentProvider as { connected: boolean }).connected
  ) {
    return Promise.resolve(managedWeb3Obj);
  }

  if (managedWeb3Obj === undefined) {
    Log.debug("web3 client undefined, initializing");
  } else {
    const readyState = (
      managedWeb3Obj.currentProvider as { connection: { _readyState?: number } }
    ).connection._readyState;
    const isConnecting = readyState === WebSocket.CONNECTING;
    const isClosing = readyState === WebSocket.CLOSING;
    const isClosed = readyState === WebSocket.CLOSED;
    const isOpen = readyState === WebSocket.OPEN;
    // It seems we get the "connected" web3 client before the connection open event has fired. In other words, it may show "connecting" here, although we may treat it as "open".
    Log.warn(
      `web3 client initialized but not open, closing=${isClosing}, closed=${isClosed}, connecting=${isConnecting}, isOpen=${isOpen}`,
    );
  }

  const web3 = await connectWeb3WithFallback();
  managedWeb3Obj = web3;
  return managedWeb3Obj;
};

const web3SeqQueue = new PQueue({ concurrency: 1 });

export const getWeb3 = () => web3SeqQueue.add(getExistingWeb3OrReconnect);

export const closeConnections = async () => {
  if (gethWsSeqQueue.size !== 0) {
    await gethWsSeqQueue.onEmpty();
  }

  if (managedGethWs !== undefined) {
    managedGethWs.close();
  }

  if (
    managedWeb3Obj !== undefined &&
    managedWeb3Obj.currentProvider !== null &&
    managedWeb3Obj.currentProvider instanceof web3Core.WebsocketProvider
  ) {
    managedWeb3Obj.currentProvider.disconnect(0, "exiting");
  }
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
    id,
    jsonrpc: "2.0",
    method: "eth_getBlockByNumber",
    params: [numberAsHex, false],
  });

  const rawBlock = await messageP;

  return rawBlock;
};

export const getRawBlockByHash = async (
  hash: string,
): Promise<BlockNodeV1 | null> => {
  const [id, messageP] = registerMessageListener<BlockNodeV1>();

  send({
    id,
    jsonrpc: "2.0",
    method: "eth_getBlockByHash",
    params: [hash, false],
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
  logs: web3Core.Log[];
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
    id,
    jsonrpc: "2.0",
    method: "eth_getTransactionReceipt",
    params: [hash],
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
    subscribeHeadsAttempt < 3 && !Config.getUseNodeFallback()
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

    Log.debug(
      `new head, number: ${head.number}, hash: ${head.hash}, parent: ${head.parentHash}`,
    );

    handleNewHead(head);
    return undefined;
  });

  headsWs.on("open", () => {
    headsWs.send(
      JSON.stringify({
        id: 1,
        jsonrpc: "2.0",
        method: "eth_subscribe",
        params: ["newHeads"],
      }),
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
    id,
    jsonrpc: "2.0",
    method: "debug_vmodule",
    params: [6],
  });

  inUseIds.delete(id);
};

export const getLatestBlockNumber = async (): Promise<number> => {
  const [id, messageP] = registerMessageListener<string>();

  send({
    id,
    jsonrpc: "2.0",
    method: "eth_blockNumber",
  });

  const rawBlockNumber = await messageP;

  return Hexadecimal.numberFromHex(rawBlockNumber);
};

export const makeContract = async (address: string, abi: AbiItem[]) => {
  const web3 = await getWeb3();
  const Contract = web3.eth.Contract;
  const contract = new Contract(abi, address);
  // NOTE: possible workaround as web3.js leaks memory.
  // See: https://github.com/ChainSafe/web3.js/issues/3042#issuecomment-663622882
  type ContractWithProvider = Contract & {
    setProvider: (provider: web3Core.provider) => void;
  };
  (contract as ContractWithProvider).setProvider(web3.currentProvider);
  return contract;
};

export const getBalance = async (address: string) => {
  const web3 = await getWeb3();
  const balanceStr = await web3.eth.getBalance(address);
  return BigInt(balanceStr);
};

export const checkHealth = async () => {
  await getLatestBlockNumber();
};
