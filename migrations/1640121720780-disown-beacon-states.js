export async function up(client) {
  await client`DROP TABLE beacon_issuance`;
  await client`DROP TABLE eth_in_validators`;
  await client`DROP TABLE beacon_states CASCADE`;
}

export async function down(client) {
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
    CREATE TABLE beacon_issuance (
      issuance numeric NOT NULL,
      timestamp timestamptz NOT NULL,
      CONSTRAINT beacon_issuance_pk PRIMARY KEY ("timestamp")
    );
  `;

  await client`
    CREATE TABLE eth_in_validators (
      gwei numeric NOT NULL,
      timestamp timestamptz NOT NULL,
      CONSTRAINT eth_in_validators_pk PRIMARY KEY ("timestamp")
    );
  `;
}
