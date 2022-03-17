// amount in ETH, from: https://etherscan.io/chart/blockreward
export const powIssuancePerDay = 13500;

// amount in ETH, from: https://beaconscan.com/stat/validatortotaldailyincome
export const posIssuancePerDay = 1352;

// average blocks per day: https://etherscan.io/chart/blocks
export const blocksPerDay = 6450;

export const issuancePerBlockPreMerge =
  (powIssuancePerDay + posIssuancePerDay) / blocksPerDay;

export const issuancePerBlockPostMerge = posIssuancePerDay / blocksPerDay;
