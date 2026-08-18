/**
 * index.ts — Cloudflare Worker: Fintech Data Source
 *
 * Generates realistic fintech events on a cron schedule (every 5 min)
 * or on-demand via HTTP. Uses Stripe TEST MODE for payment data and
 * writes all events as JSON Lines to an R2 bucket that Databricks
 * Auto Loader can ingest.
 *
 * ⚠️  This worker uses Stripe test mode exclusively.
 *     No real money is ever moved. All customer and payment data is fake.
 */

import Stripe from "stripe";
import { createCustomer, createCharge, createRefund } from "./stripe-ops";
import { generateActivityEvents } from "./activity-generator";
import { writeEvents, type WriteResult } from "./r2-writer";
import { loadState, saveState, type GeneratorState } from "./state";

// ── Worker environment bindings ─────────────────────────────────

interface Env {
  EVENTS_BUCKET: R2Bucket;
  STRIPE_SECRET_KEY: string;
  BATCH_SIZE: string;
  CUSTOMER_BATCH: string;
  REFUND_BATCH: string;
  ACTIVITY_BATCH: string;
}

// ── Pipeline summary ────────────────────────────────────────────

interface RunSummary {
  ok: boolean;
  timestamp: string;
  customers_created: number;
  transactions_created: number;
  refunds_created: number;
  activity_events_created: number;
  files_written: WriteResult[];
  errors: string[];
  state_totals: {
    total_customers: number;
    total_transactions: number;
    total_refunds: number;
    total_activity_events: number;
  };
}

// ── Core pipeline ───────────────────────────────────────────────

async function runPipeline(env: Env): Promise<RunSummary> {
  const errors: string[] = [];
  const filesWritten: WriteResult[] = [];

  // Stripe client — TEST MODE ONLY
  const stripe = new Stripe(env.STRIPE_SECRET_KEY, {
    apiVersion: "2024-12-18.acacia",
    typescript: true,
  });

  const customerBatch = parseInt(env.CUSTOMER_BATCH || "3", 10);
  const txnBatch = parseInt(env.BATCH_SIZE || "25", 10);
  const refundBatch = parseInt(env.REFUND_BATCH || "2", 10);
  const activityBatch = parseInt(env.ACTIVITY_BATCH || "40", 10);

  // Load persistent state (customer IDs, recent PIs)
  const state = await loadState(env.EVENTS_BUCKET);

  // ── Step 1: Create new customers ────────────────────────────
  const newCustomers: Record<string, unknown>[] = [];
  for (let i = 0; i < customerBatch; i++) {
    try {
      const customer = await createCustomer(stripe);
      state.customer_ids.push(customer.id);
      newCustomers.push({
        event_type: "customer_created",
        event_id: crypto.randomUUID(),
        timestamp: new Date().toISOString(),
        stripe_customer_id: customer.id,
        name: customer.name,
        email: customer.email,
        phone: customer.phone,
        address: customer.address,
        metadata: customer.metadata,
      });
    } catch (err) {
      errors.push(`customer creation failed: ${err}`);
    }
  }

  if (newCustomers.length > 0) {
    const result = await writeEvents(env.EVENTS_BUCKET, "customers", newCustomers);
    filesWritten.push(result);
  }
  state.total_customers += newCustomers.length;

  // ── Step 2: Create transactions (charges) ───────────────────
  const newTransactions: Record<string, unknown>[] = [];
  if (state.customer_ids.length > 0) {
    for (let i = 0; i < txnBatch; i++) {
      try {
        const custId =
          state.customer_ids[
            Math.floor(Math.random() * state.customer_ids.length)
          ];
        const pi = await createCharge(stripe, custId);
        state.recent_payment_intents.push(pi.id);
        newTransactions.push({
          event_type: "payment_completed",
          event_id: crypto.randomUUID(),
          timestamp: new Date().toISOString(),
          stripe_payment_intent_id: pi.id,
          stripe_customer_id: pi.customer,
          amount: pi.amount,
          currency: pi.currency,
          status: pi.status,
          payment_method_type:
            typeof pi.payment_method === "string"
              ? pi.payment_method
              : pi.payment_method?.type ?? "card",
          description: pi.description,
          metadata: pi.metadata,
        });
      } catch (err) {
        errors.push(`charge failed: ${err}`);
      }
    }
  } else {
    errors.push("no customers in state — skipping transactions");
  }

  if (newTransactions.length > 0) {
    const result = await writeEvents(
      env.EVENTS_BUCKET,
      "transactions",
      newTransactions,
    );
    filesWritten.push(result);
  }
  state.total_transactions += newTransactions.length;

  // ── Step 3: Create refunds ──────────────────────────────────
  const newRefunds: Record<string, unknown>[] = [];
  if (state.recent_payment_intents.length > 0) {
    for (let i = 0; i < refundBatch; i++) {
      try {
        const piId =
          state.recent_payment_intents[
            Math.floor(Math.random() * state.recent_payment_intents.length)
          ];
        const refund = await createRefund(stripe, piId);
        newRefunds.push({
          event_type: "payment_refunded",
          event_id: crypto.randomUUID(),
          timestamp: new Date().toISOString(),
          stripe_refund_id: refund.id,
          stripe_payment_intent_id: piId,
          amount: refund.amount,
          currency: refund.currency,
          status: refund.status,
          reason: refund.reason,
          metadata: refund.metadata,
        });
        // Remove refunded PI from candidates to avoid double-refund
        const idx = state.recent_payment_intents.indexOf(piId);
        if (idx > -1) state.recent_payment_intents.splice(idx, 1);
      } catch (err) {
        // Refunds can fail if PI already fully refunded — expected
        errors.push(`refund failed: ${err}`);
      }
    }
  }

  if (newRefunds.length > 0) {
    const result = await writeEvents(env.EVENTS_BUCKET, "refunds", newRefunds);
    filesWritten.push(result);
  }
  state.total_refunds += newRefunds.length;

  // ── Step 4: Generate activity events ────────────────────────
  const activityEvents = generateActivityEvents(
    state.customer_ids,
    activityBatch,
  );

  if (activityEvents.length > 0) {
    const result = await writeEvents(
      env.EVENTS_BUCKET,
      "activity",
      activityEvents,
    );
    filesWritten.push(result);
  }
  state.total_activity_events += activityEvents.length;

  // ── Persist state ───────────────────────────────────────────
  await saveState(env.EVENTS_BUCKET, state);

  return {
    ok: errors.length === 0,
    timestamp: new Date().toISOString(),
    customers_created: newCustomers.length,
    transactions_created: newTransactions.length,
    refunds_created: newRefunds.length,
    activity_events_created: activityEvents.length,
    files_written: filesWritten,
    errors,
    state_totals: {
      total_customers: state.total_customers,
      total_transactions: state.total_transactions,
      total_refunds: state.total_refunds,
      total_activity_events: state.total_activity_events,
    },
  };
}

// ── Worker entry points ─────────────────────────────────────────

export default {
  /** HTTP handler — trigger pipeline on-demand and return summary. */
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);

    // Health check
    if (url.pathname === "/health") {
      return new Response(JSON.stringify({ status: "ok" }), {
        headers: { "content-type": "application/json" },
      });
    }

    // Status — show state without running the pipeline
    if (url.pathname === "/status") {
      const state = await loadState(env.EVENTS_BUCKET);
      return new Response(JSON.stringify(state, null, 2), {
        headers: { "content-type": "application/json" },
      });
    }

    // Default: run the pipeline
    console.log("HTTP trigger — running pipeline");
    const summary = await runPipeline(env);
    console.log(
      `Pipeline complete: ${summary.customers_created} customers, ` +
        `${summary.transactions_created} txns, ${summary.refunds_created} refunds, ` +
        `${summary.activity_events_created} activity events`,
    );

    return new Response(JSON.stringify(summary, null, 2), {
      status: summary.ok ? 200 : 207,
      headers: { "content-type": "application/json" },
    });
  },

  /** Cron handler — runs every 5 minutes. */
  async scheduled(
    _event: ScheduledEvent,
    env: Env,
    ctx: ExecutionContext,
  ): Promise<void> {
    console.log("Cron trigger — running pipeline");
    const summary = await runPipeline(env);
    console.log(
      `Pipeline complete: ${summary.customers_created} customers, ` +
        `${summary.transactions_created} txns, ${summary.refunds_created} refunds, ` +
        `${summary.activity_events_created} activity events` +
        (summary.errors.length > 0
          ? ` (${summary.errors.length} errors)`
          : ""),
    );
  },
};
