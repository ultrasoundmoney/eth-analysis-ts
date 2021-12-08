import * as DateFns from "date-fns";
import PQueue from "p-queue";
import WebSocket from "ws";
import * as Config from "./config.js";
import { sql } from "./db.js";
import * as Log from "./log.js";

type SubscriptionConfirmationEnvelope = {
  jsonrpc: "2.0";
  id: number;
  // Unique ID later referenced in the subscription messages
  result: string;
};

type HexStr = string;

type RawHead = {
  parentHash: HexStr;
  number: HexStr;
  timestamp: HexStr;
  hash: HexStr;
};

type HeadEnvelope = {
  jsonrpc: "2.0";
  method: "eth_subscription";
  params: {
    subscription: string;
    result: RawHead;
  };
};

type HeadInsertable = {
  hash: string;
  number: number;
  parent_hash: string;
  mined_at: Date;
  received_at: Date;
  is_duplicate_number: boolean;
  is_jumping_ahead: boolean;
};

const headsQueue = new PQueue({ concurrency: 1 });

const getIsKnownNumber = async (number: number): Promise<boolean> => {
  const rows = await sql<{ exists: boolean }[]>`
    SELECT EXISTS(SELECT number FROM heads_log WHERE number = ${number})
  `;

  return rows[0]?.exists ?? false;
};

const getIsKnownHash = async (hash: string): Promise<boolean> => {
  const rows = await sql<{ exists: boolean }[]>`
    SELECT EXISTS(SELECT hash FROM heads_log WHERE hash = ${hash})
  `;

  return rows[0]?.exists ?? false;
};

const getIsSubscriptionConfirmation = (
  envelope: unknown,
): envelope is SubscriptionConfirmationEnvelope =>
  (envelope as SubscriptionConfirmationEnvelope)?.id ===
  headSubscriptionMessageId;

const getIsHeadEnvelope = (envelope: unknown): envelope is HeadEnvelope =>
  (envelope as HeadEnvelope)?.method === "eth_subscription";

const logHead = async (
  receivedAt: Date,
  isFirstLog: boolean,
  rawHead: RawHead,
): Promise<void> => {
  const head = {
    hash: rawHead.hash,
    number: Number(rawHead.number),
    parent_hash: rawHead.parentHash,
    mined_at: DateFns.fromUnixTime(Number(rawHead.timestamp)),
    received_at: receivedAt,
  };

  const [isKnownNumber, isParentKnown] = await Promise.all([
    getIsKnownNumber(head.number),
    getIsKnownHash(head.parent_hash),
  ]);

  const insertable: HeadInsertable = {
    ...head,
    is_duplicate_number: isKnownNumber,
    is_jumping_ahead: isFirstLog ? false : !isParentKnown,
  };

  Log.debug(`logging: number, duplicate, jumping, hash`);
  Log.debug(
    `${head.number}, ${isKnownNumber}, ${!isParentKnown}, ${head.hash}`,
  );

  await sql`
    INSERT INTO heads_log
      ${sql(insertable)}
  `;
};

let headsSubscriptionId: string | undefined = undefined;

let isFirstCall = true;
const getIsFirstCall = (): boolean => {
  if (isFirstCall) {
    isFirstCall = false;
    return true;
  }
  return false;
};

const onMessage = (buffer: Buffer) => {
  if (!Buffer.isBuffer(buffer)) {
    console.log("expected buffer, got: ", buffer);
    throw new Error(`unexpected message, not a buffer`);
  }

  const envelopeJson = buffer.toString();
  const envelope: unknown = JSON.parse(envelopeJson);

  if (getIsSubscriptionConfirmation(envelope)) {
    headsSubscriptionId = envelope.result;
    Log.debug("received head subscription confirmation");
    return;
  }

  if (getIsHeadEnvelope(envelope)) {
    if (headsSubscriptionId !== envelope.params.subscription) {
      console.log("unexpected envelope", envelope);
      throw new Error(
        "got unexpected new head envelope, subscription id does not match",
      );
    }

    Log.debug(
      `received new head: ${Number(envelope.params.result.number)}, hash: ${
        envelope.params.result.hash
      }`,
    );

    const isFirstCall = getIsFirstCall();

    headsQueue.add(() =>
      logHead(new Date(), isFirstCall, envelope.params.result),
    );
    return;
  }

  console.log("unexpected envelope", envelope);
  throw new Error(
    `unexpected message, not a subscription confirmation or new head`,
  );
};

Log.info("log heads start");
const ws = new WebSocket(Config.getGethUrl());

ws.on("error", (event) => {
  throw new Error(String(event));
});

ws.on("close", (event) => {
  throw new Error(String(event));
});

ws.on("message", onMessage);

const connected = new Promise((resolve) => {
  ws.on("open", resolve);
});

await connected;

Log.debug("connected to geth");

const send = (message: string) =>
  new Promise((resolve, reject) => {
    ws.send(message, (err) => {
      if (err !== undefined) {
        reject(err);
        return;
      }

      resolve(undefined);
    });
  });

// We clear the log on start because we can't be sure whether something was a jump or just the process restarting otherwise.
await sql`TRUNCATE TABLE heads_log`;
Log.debug("cleared heads_log");

// Unique-ish id we use to identify the subscription confirmation.
const headSubscriptionMessageId = 63601;

const subscribeMessage = JSON.stringify({
  id: headSubscriptionMessageId,
  method: "eth_subscribe",
  params: ["newHeads"],
});

await send(subscribeMessage);
