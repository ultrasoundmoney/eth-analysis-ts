export const denominations = ["eth", "usd"] as const;
export type Denomination = typeof denominations[number];
