export async function up(client) {
  await client`
    CREATE TABLE beacon_states (
      block_root text NULL,
      deposit_sum numeric NULL,
      deposit_sum_aggregated numeric NULL,
      parent_root text NULL,
      slot integer NOT NULL,
      state_root text NOT NULL,
      validator_balance_sum numeric NULL,
      CONSTRAINT beacon_states_pk PRIMARY KEY (state_root)
    )
  `;

  await client`
    CREATE UNIQUE INDEX beacon_states_slot_idx ON public.beacon_states (slot)
  `;
}

export async function down(client) {
  await client`
    DROP TABLE beacon_states
  `;
}
