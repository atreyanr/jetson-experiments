/**
 * state.ts — Persistent generator state stored in R2.
 *
 * Tracks created Stripe customer IDs and recent PaymentIntent IDs so
 * subsequent cron runs can create charges for existing customers and
 * refunds for recent payments.
 */

const STATE_KEY = "_state/generator-state.json";

export interface GeneratorState {
  customer_ids: string[];
  recent_payment_intents: string[];
  total_customers: number;
  total_transactions: number;
  total_refunds: number;
  total_activity_events: number;
  last_run: string;
}

function emptyState(): GeneratorState {
  return {
    customer_ids: [],
    recent_payment_intents: [],
    total_customers: 0,
    total_transactions: 0,
    total_refunds: 0,
    total_activity_events: 0,
    last_run: new Date().toISOString(),
  };
}

/** Load state from R2. Returns a fresh state if none exists yet. */
export async function loadState(bucket: R2Bucket): Promise<GeneratorState> {
  const obj = await bucket.get(STATE_KEY);
  if (!obj) return emptyState();

  try {
    const text = await obj.text();
    return JSON.parse(text) as GeneratorState;
  } catch {
    console.warn("Corrupt state file — starting fresh");
    return emptyState();
  }
}

/** Persist state back to R2. Caps lists to avoid unbounded growth. */
export async function saveState(
  bucket: R2Bucket,
  state: GeneratorState,
): Promise<void> {
  // Keep at most 2 000 customer IDs and 200 recent payment intents
  if (state.customer_ids.length > 2000) {
    state.customer_ids = state.customer_ids.slice(-2000);
  }
  if (state.recent_payment_intents.length > 200) {
    state.recent_payment_intents = state.recent_payment_intents.slice(-200);
  }

  state.last_run = new Date().toISOString();
  await bucket.put(STATE_KEY, JSON.stringify(state, null, 2));
}
