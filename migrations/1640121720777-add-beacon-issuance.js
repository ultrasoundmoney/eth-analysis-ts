export async function up(client) {
  await client`
    CREATE TABLE beacon_issuance (
      issuance numeric NOT NULL,
      timestamp timestamptz NOT NULL,
      CONSTRAINT beacon_issuance_pk PRIMARY KEY ("timestamp")
    );
  `;
}

export async function down(client) {
  await client`
    DROP TABLE beacon_issuance
  `;
}
