(BigInt.prototype as any).toJSON = function () {
  return this.toString();
};

export const serialize = (_key: unknown, value: unknown) =>
  typeof value === "bigint" ? value.toString() + "n" : value;

export const deserialize = (_key: string, value: unknown) => {
  if (typeof value === "string" && /^\d+n$/.test(value)) {
    return BigInt(value.slice(0, -1));
  }
  return value;
};
