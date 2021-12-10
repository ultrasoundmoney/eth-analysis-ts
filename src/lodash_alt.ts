export const insertAt = <A>(insertAt: number, item: A, arr: A[]): A[] => [
  ...arr.slice(0, insertAt),
  item,
  ...arr.slice(insertAt),
];
