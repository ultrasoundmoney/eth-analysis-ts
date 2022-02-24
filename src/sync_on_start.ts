import * as Blocks from "./blocks/blocks.js";
import * as DeflationaryStreaks from "./deflationary_streaks.js";
import { flow, NEA, OAlt, pipe, T } from "./fp.js";

// Unify syncing of blocks here. Only retrieve blocks to sync once.
export const sync = (_from: number, upToIncluding: number) =>
  pipe(
    T.Do,
    T.apS(
      "deflationaryStreakNextBlockToAdd",
      DeflationaryStreaks.getNextBlockToAdd(),
    ),
    // Sync deflationary streaks
    T.chain(({ deflationaryStreakNextBlockToAdd }) =>
      pipe(
        () => Blocks.getBlocks(deflationaryStreakNextBlockToAdd, upToIncluding),
        T.map(
          flow(
            NEA.fromArray,
            OAlt.getOrThrow(
              `failed to retrieve blocks ${deflationaryStreakNextBlockToAdd} to ${upToIncluding} to sync deflationary streaks, expected one or more blocks`,
            ),
          ),
        ),
        T.chain((blocks) => DeflationaryStreaks.analyzeNewBlocks(blocks)),
      ),
    ),
  );
