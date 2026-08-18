/**
 * r2-writer.ts — Writes event batches to R2 as JSON Lines files.
 *
 * Path convention (Hive-style partitioning for Databricks Auto Loader):
 *   {eventType}/year=YYYY/month=MM/day=DD/batch_{epoch}.jsonl
 */

export interface WriteResult {
  path: string;
  count: number;
  bytes: number;
}

/**
 * Write an array of events as a single JSONL file to R2.
 * Each invocation is atomic — one file, one put.
 */
export async function writeEvents(
  bucket: R2Bucket,
  eventType: string,
  events: unknown[],
): Promise<WriteResult> {
  if (events.length === 0) {
    return { path: "", count: 0, bytes: 0 };
  }

  const now = new Date();
  const year = now.getUTCFullYear();
  const month = String(now.getUTCMonth() + 1).padStart(2, "0");
  const day = String(now.getUTCDate()).padStart(2, "0");
  const epoch = now.getTime();

  const path = `${eventType}/year=${year}/month=${month}/day=${day}/batch_${epoch}.jsonl`;
  const body = events.map((e) => JSON.stringify(e)).join("\n") + "\n";

  await bucket.put(path, body, {
    httpMetadata: { contentType: "application/x-ndjson" },
    customMetadata: {
      event_type: eventType,
      record_count: String(events.length),
      generated_at: now.toISOString(),
    },
  });

  return { path, count: events.length, bytes: body.length };
}
