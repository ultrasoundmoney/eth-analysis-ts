import * as Blocks from "../blocks/blocks.js";
import * as SamplesBlocks from "../samples/blocks.js";
import * as Db from "../db.js";
import { A, flow, NEA, OAlt, pipe, T, TAlt } from "../fp.js";
import * as SamplesContractBaseFees from "../samples/contract_base_fees.js";
import * as DateFns from "date-fns";
import * as Duration from "../duration.js";
import * as ContractBaseFees from "../contract_base_fees.js";

// eslint-disable-next-line @typescript-eslint/no-explicit-any
(BigInt.prototype as any).toJSON = function () {
  return this.toString() + "n";
};

export const resetTables = () =>
  pipe(
    TAlt.seqTSeq(
      Db.sqlTVoid`DELETE FROM contract_base_fees`,
      Db.sqlTVoid`DELETE FROM contracts`,
      Db.sqlTVoid`DELETE FROM burn_records`,
      Db.sqlTVoid`DELETE FROM deflationary_streaks`,
      Db.sqlTVoid`DELETE FROM blocks`,
      Db.sqlTVoid`DELETE FROM analysis_state`,
      Db.sqlTVoid`DELETE FROM key_value_store`,
    ),
    TAlt.concatAllVoid,
  );

const getMostRecentBlockDistanceToNow = (blocks: Blocks.BlockDb[]) =>
  pipe(
    blocks,
    A.last,
    OAlt.getOrThrow("tried to find most recent block on empty list"),
    (block) => DateFns.differenceInMilliseconds(new Date(), block.minedAt),
  );

const setBlocksToNow = (blocks: NEA.NonEmptyArray<Blocks.BlockDb>) =>
  pipe(getMostRecentBlockDistanceToNow(blocks), (distanceToNow) =>
    pipe(
      blocks,
      NEA.map((block) => ({
        ...block,
        minedAt: DateFns.addMilliseconds(
          new Date(block.minedAt),
          distanceToNow,
        ),
      })),
    ),
  );

const shiftMap: Record<SamplesBlocks.SupportedSample, number> = {
  m5: Duration.millisFromMinutes(2.5),
  h1: Duration.millisFromMinutes(30),
};

const setBlocksHalfOutOfFrame = (
  sample: SamplesBlocks.SupportedSample,
  blocks: NEA.NonEmptyArray<Blocks.BlockDb>,
) =>
  pipe(
    blocks,
    NEA.map((block) => ({
      ...block,
      minedAt: DateFns.subMilliseconds(block.minedAt, shiftMap[sample]),
    })),
  );

export const getSeedBlocks = (
  sample: SamplesBlocks.SupportedSample,
  fromNow = true,
  shiftHalfOutOfFrame = false,
) =>
  pipe(
    SamplesBlocks.getBlocksFromFile(sample),
    T.map(
      flow(
        (blocks) => (fromNow ? setBlocksToNow(blocks) : blocks),
        (blocks) =>
          shiftHalfOutOfFrame
            ? setBlocksHalfOutOfFrame(sample, blocks)
            : blocks,
      ),
    ),
  );

export const seedBlocks = (
  sample: SamplesBlocks.SupportedSample &
    SamplesContractBaseFees.SupportedSample,
  fromNow = true,
  shiftHalfOutOfFrame = false,
) =>
  pipe(
    getSeedBlocks(sample, fromNow, shiftHalfOutOfFrame),
    T.map(NEA.map(Blocks.insertableFromBlock)),
    T.chain(
      (blocks) => Db.sqlTVoid`
        INSERT INTO blocks ${Db.sql(blocks)}
      `,
    ),
  );

const seedContracts = (sample: SamplesContractBaseFees.SupportedSample) =>
  pipe(
    SamplesContractBaseFees.getContractBaseFeesFromFile(sample),
    T.map(
      flow(
        A.map((contractBaseFees) => contractBaseFees.contractAddress),
        (rows) => new Set(rows),
        (set) => Array.from(set.values()),
        A.map((address) => ({ address })),
      ),
    ),
    T.chain(
      (addresses) => Db.sqlTVoid`
        INSERT INTO contracts ${Db.sql(addresses)}
      `,
    ),
  );

export const seedContractBaseFees = (
  sample: SamplesContractBaseFees.SupportedSample,
  fromNow = true,
  shiftHalfOutOfFrame = false,
) =>
  pipe(
    T.Do,
    T.apS("seedBlocks", seedBlocks(sample, fromNow, shiftHalfOutOfFrame)),
    T.apS("seedContracts", seedContracts(sample)),
    T.bind("contractBaseFees", () =>
      SamplesContractBaseFees.getContractBaseFeesFromFile("m5"),
    ),
    T.map(({ contractBaseFees }) =>
      pipe(
        contractBaseFees,
        A.map(ContractBaseFees.insertableFromContractBaseFees),
      ),
    ),
    T.chain(
      (contractBaseFees) => Db.sqlTVoid`
        INSERT INTO contract_base_fees ${Db.sql(contractBaseFees)}
      `,
    ),
  );
