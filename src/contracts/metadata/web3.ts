import * as DateFns from "date-fns";
import PQueue from "p-queue";
import * as Duration from "../../duration.js";
import * as Etherscan from "../../etherscan.js";
import { O, pipe, T, TAlt, TE } from "../../fp.js";
import * as Log from "../../log.js";
import * as Queues from "../../queues.js";
import * as Contracts from "../contracts.js";
import * as ContractsWeb3 from "../web3.js";

export const web3Queue = new PQueue({
  concurrency: 4,
  throwOnTimeout: true,
  timeout: Duration.millisFromSeconds(60),
});
const web3LastAttemptMap: Record<string, Date | undefined> = {};

const getIsBackoffPast = (
  attemptMap: Partial<Record<string, Date>>,
  address: string,
) =>
  pipe(
    attemptMap[address],
    O.fromNullable,
    O.map(
      (lastAttempted) =>
        DateFns.differenceInHours(new Date(), lastAttempted) < 6,
    ),
    O.getOrElse(() => false),
  );

const handleGetSupportedInterfaceError = (
  address: string,
  interfaceName: string,
  e: ContractsWeb3.UnsupportedMethodError | Error,
) =>
  e instanceof ContractsWeb3.UnsupportedMethodError
    ? // Not all contracts will implement this method, do nothing.
      T.of(undefined)
    : T.fromIO(() => {
        Log.error(
          `failed to check contract supports interface ${interfaceName} for ${address}`,
          e,
        );
      });

const addWeb3Metadata = (address: string) =>
  pipe(
    ContractsWeb3.getContract(address),
    Queues.queueOnQueueWithTimeoutThrown(web3Queue),
    TE.chainFirstIOK(() => () => {
      web3LastAttemptMap[address] = new Date();
    }),
    TE.chainTaskK((contract) => {
      const storeErc721Task = pipe(
        ContractsWeb3.getSupportedInterface(contract, "ERC721"),
        TE.matchE(
          (e) => handleGetSupportedInterfaceError(address, "erc721", e),
          (supportsErc_721) =>
            Contracts.setSimpleBooleanColumn(
              "supports_erc_721",
              address,
              supportsErc_721,
            ),
        ),
      );

      const storeErc1155Task = pipe(
        ContractsWeb3.getSupportedInterface(contract, "ERC1155"),
        TE.matchE(
          (e) => handleGetSupportedInterfaceError(address, "erc1155", e),
          (supportsErc_1155) =>
            Contracts.setSimpleBooleanColumn(
              "supports_erc_1155",
              address,
              supportsErc_1155,
            ),
        ),
      );

      const storeNameTask = pipe(
        ContractsWeb3.getName(contract),
        // Contracts may have a NUL byte in their name, which is not safe to store in postgres. We should find a way to store this safely.
        TE.map((name) => name.replaceAll("\x00", "")),
        TE.matchE(
          (e) => {
            if (e instanceof ContractsWeb3.NoNameMethodError) {
              // Not all contracts will implement a name method.
              return Contracts.setSimpleTextColumn("web3_name", address, null);
            }

            Log.error("failed to get web3 contract name", e);
            return T.of(undefined);
          },
          (safeName) =>
            Contracts.setSimpleTextColumn("web3_name", address, safeName),
        ),
      );

      return pipe(
        TAlt.seqTPar(storeErc721Task, storeErc1155Task, storeNameTask),
        T.chain(() => Contracts.updatePreferredMetadata(address)),
      );
    }),
    TE.match(
      (e) => {
        if (e instanceof Queues.TimeoutError) {
          // Timeouts are expected.
          Log.debug(`web3 metadata fetch timed out for ${address}`);
          return undefined;
        }

        if (e instanceof Etherscan.AbiNotVerifiedError) {
          // Not all contracts we see are verified, that's okay.
          Log.debug(`contract ABI not known to Etherscan ${address}`);
          return undefined;
        }

        // Everything else is an error we should look at although we don't want to hold up the entire crawling process and don't have proper error handling higher up yet.
        Log.error("failed to fetch web3 contract", e);
        return undefined;
      },
      (v) => v,
    ),
  );

export const refreshWeb3Metadata = (address: string, forceRefetch = false) =>
  pipe(
    forceRefetch || getIsBackoffPast(web3LastAttemptMap, address),
    (shouldAttempt) => TAlt.when(shouldAttempt, addWeb3Metadata(address)),
  );
