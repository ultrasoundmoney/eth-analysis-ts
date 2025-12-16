import * as DateFns from "date-fns";
import { createRequire } from "module";

const require = createRequire(import.meta.url);
const blobScheduleJson = require("./data/blobs/blobschedule.json");

type BlobScheduleEntry = [timestamp: number, fraction: number];

/**
 * Parses the blob schedule JSON into an array of [timestamp, fraction] tuples.
 *
 * JSON format:
 * {
 *   "blobSchedule": {
 *     "cancun": { "baseFeeUpdateFraction": 3338477 },
 *     "prague": { "baseFeeUpdateFraction": 5007716 },
 *     ...
 *   },
 *   "cancunTime": 1710338135,
 *   "pragueTime": 1746612311,
 *   ...
 * }
 *
 * Each fork in blobSchedule must have a corresponding {forkName}Time field.
 * New forks can be added to the JSON without code changes.
 */
function parseBlobSchedule(json: Record<string, unknown>): BlobScheduleEntry[] {
  const blobSchedule = json.blobSchedule;
  if (typeof blobSchedule !== "object" || blobSchedule === null) {
    throw new Error("blobSchedule should be an object");
  }

  const entries: BlobScheduleEntry[] = [];

  for (const forkName of Object.keys(blobSchedule)) {
    // Convention: each fork "foo" in blobSchedule must have a "fooTime" field
    const timeKey = `${forkName}Time`;
    const time = json[timeKey];
    if (typeof time !== "number") {
      throw new Error(`missing or invalid ${timeKey}`);
    }

    const forkParams = (blobSchedule as Record<string, unknown>)[forkName];
    const fraction =
      typeof forkParams === "object" && forkParams !== null
        ? (forkParams as Record<string, unknown>).baseFeeUpdateFraction
        : undefined;
    if (typeof fraction !== "number") {
      throw new Error(`missing baseFeeUpdateFraction for ${forkName}`);
    }

    entries.push([time, fraction]);
  }

  // Sort descending by timestamp so we don't rely on the order in the JSON
  entries.sort((a, b) => b[0] - a[0]);
  return entries;
}

const BLOB_SCHEDULE = parseBlobSchedule(blobScheduleJson);

const MIN_BLOB_BASE_FEE = 1;

function blobUpdateFractionFromTimestamp(timestamp: number): number | undefined {
  return BLOB_SCHEDULE.find(([ts]) => timestamp >= ts)?.[1];
}

function fakeExponential(
  factor: number,
  numerator: number,
  denominator: number,
): number {
  let i = 1;
  let output = 0;
  let numeratorAccum = factor * denominator;

  while (numeratorAccum > 0) {
    output += numeratorAccum;
    numeratorAccum = Math.floor((numeratorAccum * numerator) / (denominator * i));
    i += 1;
  }

  return Math.floor(output / denominator);
}

export function calcBlobBaseFee(
  excessBlobGas: number,
  timestamp: Date,
): number | undefined {
  if (excessBlobGas < 0) {
    throw new Error("excessBlobGas should not be negative");
  }

  const fraction = blobUpdateFractionFromTimestamp(DateFns.getUnixTime(timestamp));
  if (fraction === undefined) {
    return undefined;
  }

  return fakeExponential(MIN_BLOB_BASE_FEE, excessBlobGas, fraction);
}
