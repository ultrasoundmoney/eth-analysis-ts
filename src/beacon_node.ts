import EventSource from "eventsource";
import { formatUrl } from "url-sub";
import * as Config from "./config.js";
import { decodeWithError } from "./decoding.js";
import * as Fetch from "./fetch.js";
import { D, E, O, pipe, T, TE } from "./fp.js";
import * as Log from "./log.js";

const GweiAmount = pipe(
  D.string,
  D.parse((s) => {
    try {
      return D.success(BigInt(s));
    } catch (error) {
      if (error instanceof Error) {
        return D.failure(
          s,
          `failed to parse deposit amount as BigInt, ${error.message}`,
        );
      }

      return D.failure(
        s,
        `failed to parse deposits amount as BigInt, ${error}`,
      );
    }
  }),
);

type GweiAmount = D.TypeOf<typeof GweiAmount>;

// JS MAX_SAFE_INTEGER is big enough to hold the slot billions of years into the future.
const Slot = pipe(
  D.string,
  D.parse((s) => D.success(Number(s))),
);

export const BeaconBlock = D.struct({
  body: D.struct({
    deposits: D.array(
      D.struct({
        data: D.struct({
          amount: GweiAmount,
        }),
      }),
    ),
  }),
  parent_root: D.string,
  slot: Slot,
  state_root: D.string,
});

export type BeaconBlock = D.TypeOf<typeof BeaconBlock>;

export const BeaconBlockEnvelope = D.struct({
  data: D.struct({
    message: BeaconBlock,
  }),
});

export type BeaconBlockEnvelope = D.TypeOf<typeof BeaconBlockEnvelope>;

const StateRootEnvelope = D.struct({
  data: D.struct({
    root: D.string,
  }),
});

type StateRootEnvelope = D.TypeOf<typeof StateRootEnvelope>;

const makeBlocksUrl = (blockId: "head" | "finalized" | "genesis" | string) =>
  formatUrl(Config.getBeaconUrl(), "/eth/v1/beacon/blocks/:block_id", {
    block_id: blockId,
  });

export const getLastFinalizedBlock = () =>
  pipe(
    Fetch.fetchJson(makeBlocksUrl("finalized")),
    TE.chainEitherKW(decodeWithError(BeaconBlockEnvelope)),
    TE.map((envelope) => envelope.data.message),
  );

const makeStateRootUrl = (slot: number) =>
  formatUrl(Config.getBeaconUrl(), "/eth/v1/beacon/states/:state_id/root", {
    state_id: slot,
  });

export const getStateRootBySlot = (slot: number) =>
  pipe(
    Fetch.fetchJson(makeStateRootUrl(slot)),
    TE.chainEitherKW(decodeWithError(StateRootEnvelope)),
    TE.map((envelope) => envelope.data.root),
  );

export const getBlockByRoot = (blockRoot: string) =>
  pipe(
    Fetch.fetchJson(makeBlocksUrl(blockRoot)),
    TE.chainEitherKW(decodeWithError(BeaconBlockEnvelope)),
    TE.map((envelope) => envelope.data.message),
  );

export const FinalizedCheckpoint = pipe(
  D.string,
  D.parse((s) =>
    pipe(
      s,
      JSON.parse,
      D.struct({
        block: D.string,
        state: D.string,
        epoch: pipe(
          D.string,
          D.parse((s) => D.success(Number(s))),
        ),
      }).decode,
    ),
  ),
);

export type FinalizedCheckpoint = D.TypeOf<typeof FinalizedCheckpoint>;

const FinalizedCheckpointMessageEvent = D.struct({
  type: D.literal("finalized_checkpoint"),
  data: FinalizedCheckpoint,
});

let es: EventSource | undefined = undefined;

export const subscribeNewFinalizedCheckpoints = async (
  cb: (finalizedCheckpoint: {
    block: string;
    state: string;
    epoch: number;
  }) => void,
) => {
  if (es !== undefined) {
    throw new Error("tried to subscribe to beacon SSE but already subscribed");
  }

  es = new EventSource(
    formatUrl(Config.getBeaconUrl(), "/eth/v1/events", {
      topics: "finalized_checkpoint",
    }),
  );

  es.onopen = Log.debugIO("subscribed to beacon finalized checkpoints SSE");

  es.onerror = (
    event: MessageEvent & { status?: number; message?: string },
  ) => {
    Log.error("beacon SSE closed", event);
    throw new Error("beacon SSE closed");
  };

  es.addEventListener("finalized_checkpoint", (event) => {
    pipe(
      event,
      decodeWithError(FinalizedCheckpointMessageEvent),
      E.match(
        (e) => {
          throw e;
        },
        (envelope) => cb(envelope.data),
      ),
    );
  });
};

export const closeConnection = () => {
  if (es === undefined) {
    Log.warn("tried to close beacon node connection but nothing is open");
    return;
  }

  es.close();
};

const makeBlockBySlotUrl = (slot: number) =>
  formatUrl(Config.getBeaconUrl(), "/eth/v2/beacon/blocks/:block_id", {
    block_id: slot,
  });

export const getBlockBySlot = (slot: number) =>
  pipe(
    Fetch.fetchJson(makeBlockBySlotUrl(slot)),
    T.map(
      E.matchW(
        (e): E.Either<typeof e, O.Option<BeaconBlock>> =>
          e instanceof Fetch.BadResponseError && e.status === 404
            ? // No block at this slot, either this slot doesn't exist or there is no header at this slot. We assume the slot exists but is headerless.
              E.right(O.none)
            : E.left(e),
        (u) =>
          pipe(
            u,
            decodeWithError(BeaconBlockEnvelope),
            E.map((envelope) => O.some(envelope.data.message)),
          ),
      ),
    ),
  );

export const getSlotFromEpoch = (epoch: number) => epoch * 32;

const BeaconHeader = D.struct({
  /** block_root */
  root: D.string,
  header: D.struct({
    message: D.struct({
      slot: Slot,
      parent_root: D.string,
      state_root: D.string,
    }),
  }),
});

export type BeaconHeader = D.TypeOf<typeof BeaconHeader>;

const BeaconHeaderEnvelope = D.struct({
  data: BeaconHeader,
});

const makeHeaderBySlotUrl = (slot: number) =>
  formatUrl(Config.getBeaconUrl(), "/eth/v1/beacon/headers/:block_id", {
    block_id: slot,
  });

export const getHeaderBySlot = (slot: number) =>
  pipe(
    Fetch.fetchJson(makeHeaderBySlotUrl(slot)),
    T.map(
      E.matchW(
        (e): E.Either<typeof e, O.Option<BeaconHeader>> =>
          e instanceof Fetch.BadResponseError && e.status === 404
            ? // No header at this slot, either this slot doesn't exist or there is no header at this slot. We assume the slot exists but is headerless.
              E.right(O.none)
            : E.left(e),
        (u) =>
          pipe(
            u,
            decodeWithError(BeaconHeaderEnvelope),
            E.map((envelope) => O.some(envelope.data)),
          ),
      ),
    ),
  );

const makeHeaderByBlockRootUrl = (blockRoot: string) =>
  formatUrl(Config.getBeaconUrl(), "/eth/v1/beacon/headers/:block_id", {
    block_id: blockRoot,
  });

export const getHeaderByRoot = (blockRoot: string) =>
  pipe(
    Fetch.fetchJson(makeHeaderByBlockRootUrl(blockRoot)),
    T.map(
      E.matchW(
        (e): E.Either<typeof e, O.Option<BeaconHeader>> =>
          e instanceof Fetch.BadResponseError && e.status === 404
            ? // No header at this slot, either this slot doesn't exist or there is no header at this slot. We assume the slot exists but is headerless.
              E.right(O.none)
            : E.left(e),
        (u) =>
          pipe(
            u,
            decodeWithError(BeaconHeaderEnvelope),
            E.map((envelope) => O.some(envelope.data)),
          ),
      ),
    ),
  );

const makeValidatorBalancesByStateUrl = (stateRoot: string) =>
  formatUrl(
    Config.getBeaconUrl(),
    "/eth/v1/beacon/states/:state_id/validator_balances",
    { state_id: stateRoot },
  );

const ValidatorBalance = D.struct({
  index: D.string,
  balance: GweiAmount,
});

export type ValidatorBalance = D.TypeOf<typeof ValidatorBalance>;

const ValidatorBalances = D.struct({
  data: D.array(ValidatorBalance),
});

export const getValidatorBalances = (stateRoot: string) =>
  pipe(
    Fetch.fetchJson(makeValidatorBalancesByStateUrl(stateRoot)),
    TE.chainEitherKW(decodeWithError(ValidatorBalances)),
    TE.map((envelope) => envelope.data),
  );
