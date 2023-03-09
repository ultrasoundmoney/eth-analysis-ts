import * as Blocks from "./blocks/blocks.js";
import * as DeflationaryStreaks from "./deflationary_streaks.js";
import { flow, NEA, OAlt, pipe, T, TO } from "./fp.js";
import * as Log from "./log.js";
import * as Performance from "./performance.js";

// Unify syncing of blocks here. Only retrieve blocks to sync once.
export const sync = (_from: number, upToIncluding: number) =>
  pipe(
    T.Do,
    T.apS(
      "deflationaryStreakNextBlockToAdd",
      pipe(
        DeflationaryStreaks.getNextBlockToAdd(),
        Performance.measureTaskPerf(
          "  get deflationary streak next block to add",
        ),
      ),
    ),
    // Sync deflationary streaks
    T.chain(({ deflationaryStreakNextBlockToAdd }) =>
      pipe(
        deflationaryStreakNextBlockToAdd,
        TO.fromOption,
        TO.chainTaskK((deflationaryStreakNextBlockToAdd) => {
          const blocksToSyncCount =
            upToIncluding - deflationaryStreakNextBlockToAdd;

          Log.debug(
            `getting ${blocksToSyncCount} blocks to sync deflationary streaks`,
          );

          return Blocks.getBlocks(
            deflationaryStreakNextBlockToAdd,
            upToIncluding,
          );
        }),
        TO.map(
          flow(
            NEA.fromArray,
            OAlt.getOrThrow(
              `failed to retrieve blocks ${deflationaryStreakNextBlockToAdd} to ${upToIncluding} to sync deflationary streaks, expected one or more blocks`,
            ),
          ),
        ),
        TO.chainTaskK((blocks) => DeflationaryStreaks.analyzeNewBlocks(blocks)),
        Performance.measureTaskPerf("  sync deflationary streak"),
      ),
    ),
  );
