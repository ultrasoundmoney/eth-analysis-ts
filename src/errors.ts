export const errorFromUnknown = (e: unknown) => {
  if (e instanceof Error) {
    return e;
  }

  return new Error(String(e));
};
