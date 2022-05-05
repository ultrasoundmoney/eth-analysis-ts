import { E, T, TE } from "./fp.js";

export const traverseGenSeq =
  <E1, E2, A, B>(
    gen: AsyncGenerator<E.Either<E1, A>, void>,
    fn: (a: A) => TE.TaskEither<E2, B>,
  ): TE.TaskEither<E1 | E2, readonly B[]> =>
  async () => {
    const bs = [];
    for await (const a of gen) {
      if (E.isLeft(a)) {
        return a;
      }

      const b = await fn(a.right)();

      if (E.isLeft(b)) {
        return b;
      }

      bs.push(b);
    }
    return E.sequenceArray<E1 | E2, B>(bs);
  };

export const traverseGenSeqUnsafe =
  <A, B>(
    gen: AsyncGenerator<A, void>,
    fn: (a: A) => T.Task<B>,
  ): T.Task<readonly B[]> =>
  async () => {
    const bs = [];
    for await (const a of gen) {
      const b = await fn(a)();
      bs.push(b);
    }

    return bs;
  };
