import blobScheduleJson from "../data/blobs/blobschedule.json";

type BlobScheduleFile = {
  blobSchedule: {
    cancun: ForkParams;
    prague: ForkParams;
    osaka: ForkParams;
    bpo1: ForkParams;
    bpo2: ForkParams;
  };
  cancunTime: number;
  pragueTime: number;
  osakaTime: number;
  bpo1Time: number;
  bpo2Time: number;
};

type ForkParams = {
  baseFeeUpdateFraction: number;
};

const schedule = blobScheduleJson as BlobScheduleFile;

const BLOB_SCHEDULE: [number, number][] = [
  [schedule.bpo2Time, schedule.blobSchedule.bpo2.baseFeeUpdateFraction],
  [schedule.bpo1Time, schedule.blobSchedule.bpo1.baseFeeUpdateFraction],
  [schedule.osakaTime, schedule.blobSchedule.osaka.baseFeeUpdateFraction],
  [schedule.pragueTime, schedule.blobSchedule.prague.baseFeeUpdateFraction],
  [schedule.cancunTime, schedule.blobSchedule.cancun.baseFeeUpdateFraction],
];

function blobUpdateFractionFromTimestamp(timestamp: number): number {
  const entry = BLOB_SCHEDULE.find(([ts]) => timestamp >= ts);
  if (entry == null) {
    throw new Error(`no matching blob schedule entry for timestamp ${timestamp}`);
  }
  return entry[1];
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
): number {
  const MIN_BLOB_BASE_FEE = 1;
  const fraction = blobUpdateFractionFromTimestamp(
    Math.floor(timestamp.getTime() / 1000),
  );

  return fakeExponential(MIN_BLOB_BASE_FEE, excessBlobGas, fraction);
}
