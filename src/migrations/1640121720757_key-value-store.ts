export async function up(DB) {
  await DB`
    CREATE TABLE public.key_value_store (
      "key" text NOT NULL,
      value json NULL,
      CONSTRAINT key_value_store_pkey PRIMARY KEY (key)
    );
  `;
}
