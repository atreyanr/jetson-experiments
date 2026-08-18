/**
 * activity-generator.ts — Synthetic user activity event generator.
 *
 * Produces realistic browsing sessions: login → browse → search →
 * add_to_cart → checkout. Drop-off rates model a real conversion funnel.
 * No Stripe calls — these are pure application-level events.
 */

// ── Types ───────────────────────────────────────────────────────

export interface ActivityEvent {
  event_id: string;
  event_type: string;
  customer_id: string;
  timestamp: string;
  session_id: string;
  page_url: string;
  device_type: string;
  ip_address: string;
  user_agent: string;
  metadata: Record<string, string>;
}

// ── Reference data ──────────────────────────────────────────────

const PAGES = [
  "/dashboard",
  "/accounts",
  "/accounts/checking",
  "/accounts/savings",
  "/accounts/credit-card",
  "/transfer",
  "/transfer/domestic",
  "/transfer/international",
  "/payments",
  "/payments/history",
  "/payments/scheduled",
  "/bills",
  "/bills/upcoming",
  "/investments",
  "/investments/portfolio",
  "/settings",
  "/settings/security",
  "/settings/notifications",
  "/support",
  "/rewards",
];

const SEARCH_TERMS = [
  "transfer money",
  "check balance",
  "pay bill",
  "credit card statement",
  "interest rate",
  "wire transfer fee",
  "exchange rate",
  "savings account",
  "fraud alert",
  "dispute transaction",
  "update address",
  "account limit",
  "investment options",
  "mortgage rate",
  "loan application",
];

const USER_AGENTS = [
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
  "Mozilla/5.0 (iPhone; CPU iPhone OS 18_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1 Mobile/15E148 Safari/604.1",
  "Mozilla/5.0 (Linux; Android 15; Pixel 9) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Mobile Safari/537.36",
  "Mozilla/5.0 (iPad; CPU OS 18_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1 Mobile/15E148 Safari/604.1",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1 Safari/605.1.15",
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0",
];

const DEVICES = ["desktop", "mobile", "tablet"] as const;

// ── Helpers ─────────────────────────────────────────────────────

function pick<T>(arr: readonly T[]): T {
  return arr[Math.floor(Math.random() * arr.length)];
}

function randInt(min: number, max: number): number {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

/** Generate a fake but realistic-looking IPv4 address. */
function fakeIp(): string {
  // Avoid reserved ranges
  const a = randInt(11, 223);
  return `${a}.${randInt(0, 255)}.${randInt(0, 255)}.${randInt(1, 254)}`;
}

// ── Session generator ───────────────────────────────────────────

/**
 * Generate one user session (3–10 events) following a realistic funnel:
 *   login → browse (1-5 pages) → maybe search → maybe add_to_cart → maybe checkout
 *
 * Drop-off probabilities (cumulative):
 *   100% login, 100% browse, 30% search, 20% add_to_cart, 10% checkout
 */
function generateSession(
  customerId: string,
  baseTime: Date,
): ActivityEvent[] {
  const events: ActivityEvent[] = [];
  const sessionId = crypto.randomUUID();
  const device = pick(DEVICES);
  const ua = pick(USER_AGENTS);
  const ip = fakeIp();
  let cursor = new Date(baseTime);

  function pushEvent(
    eventType: string,
    pageUrl: string,
    meta: Record<string, string> = {},
  ): void {
    events.push({
      event_id: crypto.randomUUID(),
      event_type: eventType,
      customer_id: customerId,
      timestamp: cursor.toISOString(),
      session_id: sessionId,
      page_url: pageUrl,
      device_type: device,
      ip_address: ip,
      user_agent: ua,
      metadata: meta,
    });
    // Advance clock 10–120 seconds between events
    cursor = new Date(cursor.getTime() + randInt(10, 120) * 1000);
  }

  // Step 1: Login (always)
  pushEvent("login", "/login", { auth_method: pick(["password", "biometric", "sso"]) });

  // Step 2: Browse 1–5 pages (always)
  const browseCount = randInt(1, 5);
  for (let i = 0; i < browseCount; i++) {
    pushEvent("browse", pick(PAGES), { referrer: i === 0 ? "/login" : pick(PAGES) });
  }

  // Step 3: Search (30% chance)
  if (Math.random() < 0.3) {
    const term = pick(SEARCH_TERMS);
    pushEvent("search", "/search", {
      query: term,
      results_count: String(randInt(0, 25)),
    });
  }

  // Step 4: Add to cart / initiate transfer (20% chance)
  if (Math.random() < 0.2) {
    const amount = randInt(500, 500000); // cents
    pushEvent("add_to_cart", "/transfer", {
      amount_cents: String(amount),
      currency: pick(["usd", "eur", "gbp"]),
      transfer_type: pick(["domestic", "international", "bill_pay"]),
    });

    // Step 5: Checkout (50% of those who add to cart → 10% overall)
    if (Math.random() < 0.5) {
      pushEvent("checkout_started", "/transfer/confirm", {
        amount_cents: String(amount),
      });

      // Complete or abandon (70% complete of those who start checkout)
      if (Math.random() < 0.7) {
        pushEvent("checkout_completed", "/transfer/success", {
          amount_cents: String(amount),
          confirmation_id: crypto.randomUUID().slice(0, 8).toUpperCase(),
        });
      } else {
        pushEvent("checkout_abandoned", "/transfer/confirm", {
          reason: pick(["changed_mind", "error", "session_timeout", "price_check"]),
        });
      }
    }
  }

  return events;
}

// ── Public API ──────────────────────────────────────────────────

/**
 * Generate `count` activity events spread across multiple user sessions.
 * Each session belongs to a random customer from the provided list.
 */
export function generateActivityEvents(
  customerIds: string[],
  count: number,
): ActivityEvent[] {
  if (customerIds.length === 0) return [];

  const allEvents: ActivityEvent[] = [];
  const now = new Date();

  // Generate sessions until we have enough events
  while (allEvents.length < count) {
    const customerId = pick(customerIds);
    // Spread session start times over the last 5 minutes
    const offset = randInt(0, 5 * 60) * 1000;
    const sessionStart = new Date(now.getTime() - offset);
    const session = generateSession(customerId, sessionStart);
    allEvents.push(...session);
  }

  // Trim to exact count and sort by timestamp
  return allEvents
    .slice(0, count)
    .sort((a, b) => a.timestamp.localeCompare(b.timestamp));
}
