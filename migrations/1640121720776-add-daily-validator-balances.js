export async function up(client) {
  await client`
    CREATE TABLE eth_in_validators (
      gwei numeric NOT NULL,
      timestamp timestamptz NOT NULL,
      CONSTRAINT eth_in_validators_pk PRIMARY KEY ("timestamp")
    );
  `;
}

export async function down(client) {
  await client`
    DROP TABLE eth_in_validators
  `;
}
