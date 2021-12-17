import { sql } from "../db.js";
import { O, pipe } from "../fp.js";

export type Scarcity = {
  engines: {
    burned: {
      amount: bigint;
      name: string;
      startedOn: Date;
    };
    locked: {
      amount: number;
      name: string;
      startedOn: Date;
    };
    staked: {
      amount: bigint;
      name: string;
      startedOn: Date;
    };
  };
  ethSupply: bigint;
  number: number;
};

let lastScarcity: Scarcity | undefined = undefined;

export const updateScarcity = (scarcity: Scarcity) => {
  lastScarcity = scarcity;
};

export const getLastScarcity = (): O.Option<Scarcity> =>
  pipe(lastScarcity, O.fromNullable);

export type ScarcityJson = string;

export const getLastStoredScarcity = async (): Promise<ScarcityJson> => {
  const rows = await sql<{ scarcity: string }[]>`
    SELECT scarcity FROM derived_block_stats
    WHERE block_number = (
      SELECT MAX(block_number) FROM derived_block_stats
      WHERE scarcity IS NOT NULL
    )
  `;

  console.log(rows[0]?.scarcity);

  return rows[0]?.scarcity;
};
