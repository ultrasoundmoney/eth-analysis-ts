export const make2 = <A, B>(as: readonly A[], bs: readonly B[]): [A, B][] => {
  const products = [] as [A, B][];
  for (const a of as) {
    for (const b of bs) {
      products.push([a, b]);
    }
  }
  return products;
};

export const make3 = <A, B, C>(
  as: readonly A[],
  bs: readonly B[],
  cs: readonly C[],
): [A, B, C][] => {
  const products = [] as [A, B, C][];
  for (const a of as) {
    for (const b of bs) {
      for (const c of cs) {
        products.push([a, b, c]);
      }
    }
  }
  return products;
};
