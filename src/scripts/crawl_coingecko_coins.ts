import * as Coingecko from "../coingecko.js";
import * as MetadataCoingecko from "../contracts/metadata/coingecko.js";
import { A, pipe, T, TE, TEAlt } from "../fp.js";
import * as TotalValueSecured from "../total-value-secured/total_value_secured.js";
import * as Log from "../log.js";
import { addTwitterMetadataMaybe } from "../contracts/metadata/twitter.js";

let seenLastCrawled = true;
const lastCrawled = "circuits-of-value";

await pipe(
  Coingecko.getCoinList(),
  TE.map(A.filter(TotalValueSecured.getIsCoinOnEth)),
  TE.map(
    A.filter((coin) => {
      if (seenLastCrawled) {
        return true;
      }

      if (coin.id === lastCrawled) {
        seenLastCrawled = true;
        return false;
      }

      return false;
    }),
  ),
  TEAlt.chainFirstLogDebug((list) => `${list.length} coins to crawl`),
  TE.chain(
    TE.traverseSeqArray((coin) =>
      pipe(
        MetadataCoingecko.checkForMetadata(coin.platforms.ethereum),
        T.chain(() => addTwitterMetadataMaybe(coin.platforms.ethereum)),
        T.chainIOK(() => Log.debugIO(`added metadata for coin ${coin.id}`)),
        TE.fromTask,
      ),
    ),
  ),
  TE.match(
    (e) => Log.error("failed to crawl CoinGecko metadata", e),
    () => Log.debug("succcessfully crawled CoinGecko metadata"),
  ),
)();
