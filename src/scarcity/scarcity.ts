import { sql } from "../db.js";
import { O, pipe } from "../fp.js";

export type ScarcityT = {
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

let lastScarcity: ScarcityT | undefined = undefined;

export const updateScarcity = (scarcity: ScarcityT) => {
  lastScarcity = scarcity;
};

export const getLastScarcity = (): O.Option<ScarcityT> =>
  pipe(lastScarcity, O.fromNullable);

export const getLastStoredScarcity = async (): Promise<ScarcityT> => {
  const rows = await sql<{ scarcity: ScarcityT }[]>`
    SELECT scarcity FROM derived_block_stats
    WHERE block_number = (
      SELECT MAX(block_number) FROM derived_block_stats
      WHERE scarcity IS NOT NULL
    )
  `;

  return rows[0]?.scarcity;
};
