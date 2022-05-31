import * as Db from "./db.js";

export const getIssuanceByDay = () =>
  Db.sqlT<{ timestamp: Date; gwei: string }[]>`
    SELECT timestamp, gwei FROM beacon_issuance
  `;
