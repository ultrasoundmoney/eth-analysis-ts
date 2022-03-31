import * as Blocks from "./blocks/blocks.js";
import * as Performance from "./performance.js";
import * as DeflationaryStreaks from "./deflationary_streaks.js";
import { flow, NEA, OAlt, pipe, T, TO } from "./fp.js";

// Unify syncing of blocks here. Only retrieve blocks to sync once.
export const sync = (_from: number, upToIncluding: number) =>
  pipe(
    T.Do,
    T.apS(
      "deflationaryStreakNextBlockToAdd",
      Performance.measureTaskPerf(
        "  get deflationary streak next block to add",
        DeflationaryStreaks.getNextBlockToAdd(),
      ),
    ),
    // Sync deflationary streaks
    T.chain(({ deflationaryStreakNextBlockToAdd }) =>
      Performance.measureTaskPerf(
        "  sync deflationary streak",
        pipe(
          deflationaryStreakNextBlockToAdd,
          TO.fromOption,
          TO.chainTaskK((deflationaryStreakNextBlockToAdd) =>
            Blocks.getBlocks(deflationaryStreakNextBlockToAdd, upToIncluding),
          ),
          TO.map(
            flow(
              NEA.fromArray,
              OAlt.getOrThrow(
                `failed to retrieve blocks ${deflationaryStreakNextBlockToAdd} to ${upToIncluding} to sync deflationary streaks, expected one or more blocks`,
              ),
            ),
          ),
          TO.chainTaskK((blocks) =>
            DeflationaryStreaks.analyzeNewBlocks(blocks),
          ),
        ),
      ),
    ),
  );
