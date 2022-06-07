import fetch from "node-fetch";
import { setTimeout } from "timers/promises";
import { releaseCanary, resetCanary } from "./canary.js";
import * as Log from "./log.js";

Log.info("releasing canary, triggers after ? seconds");
releaseCanary();

let lastSeenBlockNumber = undefined;

// eslint-disable-next-line no-constant-condition
while (true) {
  const res = await fetch("https://api.ultrasound.money/fees/all");
  const body = (await res.json()) as { number: number };

  if (lastSeenBlockNumber !== body.number) {
    resetCanary();
    Log.debug(
      `lastSeenBlockNumber: ${lastSeenBlockNumber}, new block number: ${body.number}, resetting canary`,
    );
    lastSeenBlockNumber = body.number;
  }

  await setTimeout(10000);
}
