import * as Fs from "fs/promises";
import { pipe } from "./fp.js";
import * as DateFns from "date-fns";
import PQueue from "p-queue";
import * as Config from "./config.js";
import WebSocket from "ws";
import { promisify } from "util";
import { env } from "process";
import { sql } from "./db.js";
import { insert } from "fp-ts/lib/ReadonlySet";
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
    SELECT EXISTS(SELECT number FROM blocks WHERE number = ${number})
  `;

  return rows[0]?.exists ?? false;
};

const getIsKnownHash = async (hash: string): Promise<boolean> => {
  const rows = await sql<{ exists: boolean }[]>`
    SELECT EXISTS(SELECT hash FROM blocks WHERE hash = ${hash})
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

const logHead = async (receivedAt: Date, rawHead: RawHead) => {
  const head = {
    hash: rawHead.hash,
    number: Number(rawHead.number),
    parent_hash: rawHead.parentHash,
    mined_at: DateFns.fromUnixTime(Number(rawHead.timestamp)),
    received_at: receivedAt,
  };

  Log.debug(`logging: ${head.number}, ${head.hash}`);

  const [isKnownNumber, isParentKnown] = await Promise.all([
    getIsKnownNumber(head.number),
    getIsKnownHash(head.parent_hash),
  ]);

  const insertable: HeadInsertable = {
    ...head,
    is_duplicate_number: isKnownNumber,
    is_jumping_ahead: !isParentKnown,
  };

  await sql`
    INSERT INTO heads_log
      ${sql(insertable)}
  `;

  Log.debug(`done logging: ${head.number}, ${head.hash}`);
};

let headsSubscriptionId: string | undefined = undefined;

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

    headsQueue.add(() => logHead(new Date(), envelope.params.result));
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

// Unique-ish id we use to identify the subscription confirmation.
const headSubscriptionMessageId = 63601;

const subscribeMessage = JSON.stringify({
  id: headSubscriptionMessageId,
  method: "eth_subscribe",
  params: ["newHeads"],
});

await send(subscribeMessage);
