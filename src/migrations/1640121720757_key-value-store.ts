// eslint-disable-next-line @typescript-eslint/no-explicit-any
export async function up(DB: any) {
  await DB`
    CREATE TABLE public.key_value_store (
      "key" text NOT NULL,
      value json NULL,
      CONSTRAINT key_value_store_pkey PRIMARY KEY (key)
    );
  `;
}
