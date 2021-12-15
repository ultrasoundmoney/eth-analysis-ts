export const serializeBigInt = (_key: unknown, value: unknown) =>
  typeof value === "bigint" ? value.toString() + "n" : value;

export const deserializeBigInt = (_key: string, value: unknown) => {
  if (typeof value === "string" && /^\d+n$/.test(value)) {
    return BigInt(value.slice(0, -1));
  }
  return value;
};
