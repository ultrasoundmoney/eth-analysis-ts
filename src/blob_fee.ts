/**
 * Blob fee calculation based on EIP-4844.
 *
 * To update the blob schedule for future forks, replace the blob schedule JSON with the new
 * version. Blob schedule can be found in the chainspec:
 * https://github.com/eth-clients/mainnet/blob/main/metadata/chainspec.json
 */
import * as DateFns from "date-fns";
import { createRequire } from "module";
import { numberFromHex } from "./hexadecimal.js";

const require = createRequire(import.meta.url);

const MIN_BLOB_BASE_FEE = 1;

type BlobScheduleJson = {
  blobSchedule: BlobScheduleEntryJson[];
};

type BlobScheduleEntryJson = {
  timestamp: string;
  baseFeeUpdateFraction: string;
};

type BlobScheduleEntry = [timestamp: number, fraction: number];

const blobScheduleJson: BlobScheduleJson = require("./data/blobs/blobschedule.json");

const BLOB_SCHEDULE: BlobScheduleEntry[] = blobScheduleJson.blobSchedule
  .map(
    (entry): BlobScheduleEntry => [
      numberFromHex(entry.timestamp),
      numberFromHex(entry.baseFeeUpdateFraction),
    ],
  )
  .sort((a, b) => b[0] - a[0]);

function blobUpdateFractionFromTimestamp(
  timestamp: number,
): number | undefined {
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
    numeratorAccum = Math.floor(
      (numeratorAccum * numerator) / (denominator * i),
    );
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

  const fraction = blobUpdateFractionFromTimestamp(
    DateFns.getUnixTime(timestamp),
  );
  if (fraction === undefined) {
    return undefined;
  }

  return fakeExponential(MIN_BLOB_BASE_FEE, excessBlobGas, fraction);
}
