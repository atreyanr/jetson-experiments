/**
 * stripe-ops.ts — Stripe TEST MODE operations for fintech data generation.
 *
 * ⚠️  ALL calls go to Stripe's test environment (sk_test_... key).
 *     No real money is moved. Test card tokens are used for payments.
 *     https://docs.stripe.com/testing#cards
 */

import Stripe from "stripe";

// ── Reference data ──────────────────────────────────────────────

const FIRST_NAMES = [
  "James", "Mary", "Robert", "Patricia", "John", "Jennifer", "Michael",
  "Linda", "David", "Elizabeth", "William", "Barbara", "Richard", "Susan",
  "Joseph", "Jessica", "Thomas", "Sarah", "Christopher", "Karen",
  "Charles", "Lisa", "Daniel", "Nancy", "Matthew", "Betty", "Anthony",
  "Margaret", "Mark", "Sandra", "Donald", "Ashley", "Steven", "Kimberly",
  "Paul", "Emily", "Andrew", "Donna", "Joshua", "Michelle", "Kenneth",
  "Carol", "Kevin", "Amanda", "Brian", "Dorothy", "George", "Melissa",
  "Timothy", "Deborah", "Ronald", "Stephanie", "Edward", "Rebecca",
  "Jason", "Sharon", "Jeffrey", "Laura", "Ryan", "Cynthia",
];

const LAST_NAMES = [
  "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
  "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez",
  "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin",
  "Lee", "Perez", "Thompson", "White", "Harris", "Sanchez", "Clark",
  "Ramirez", "Lewis", "Robinson", "Walker", "Young", "Allen", "King",
  "Wright", "Scott", "Torres", "Nguyen", "Hill", "Flores", "Green",
  "Adams", "Nelson", "Baker", "Hall", "Rivera", "Campbell", "Mitchell",
  "Carter", "Roberts", "Gomez", "Phillips", "Evans", "Turner", "Diaz",
  "Parker", "Cruz", "Edwards", "Collins", "Reyes",
];

const CITIES: Array<{ city: string; state: string; zip: string }> = [
  { city: "New York", state: "NY", zip: "10001" },
  { city: "Los Angeles", state: "CA", zip: "90001" },
  { city: "Chicago", state: "IL", zip: "60601" },
  { city: "Houston", state: "TX", zip: "77001" },
  { city: "Phoenix", state: "AZ", zip: "85001" },
  { city: "Philadelphia", state: "PA", zip: "19101" },
  { city: "San Antonio", state: "TX", zip: "78201" },
  { city: "San Diego", state: "CA", zip: "92101" },
  { city: "Dallas", state: "TX", zip: "75201" },
  { city: "Austin", state: "TX", zip: "78701" },
  { city: "Jacksonville", state: "FL", zip: "32099" },
  { city: "San Francisco", state: "CA", zip: "94102" },
  { city: "Columbus", state: "OH", zip: "43085" },
  { city: "Indianapolis", state: "IN", zip: "46201" },
  { city: "Charlotte", state: "NC", zip: "28201" },
  { city: "Seattle", state: "WA", zip: "98101" },
  { city: "Denver", state: "CO", zip: "80201" },
  { city: "Boston", state: "MA", zip: "02101" },
  { city: "Nashville", state: "TN", zip: "37201" },
  { city: "Portland", state: "OR", zip: "97201" },
  { city: "Miami", state: "FL", zip: "33101" },
  { city: "Atlanta", state: "GA", zip: "30301" },
  { city: "Minneapolis", state: "MN", zip: "55401" },
  { city: "Detroit", state: "MI", zip: "48201" },
  { city: "Raleigh", state: "NC", zip: "27601" },
];

/** Spending categories with amount ranges in cents */
const CATEGORIES: Array<{
  name: string;
  min: number;
  max: number;
  weight: number;
}> = [
  { name: "coffee",        min: 300,     max: 750,     weight: 25 },
  { name: "groceries",     min: 2000,    max: 20000,   weight: 20 },
  { name: "gas",           min: 3000,    max: 8000,    weight: 15 },
  { name: "restaurants",   min: 1500,    max: 12000,   weight: 15 },
  { name: "subscription",  min: 500,     max: 5000,    weight: 10 },
  { name: "electronics",   min: 5000,    max: 200000,  weight: 5 },
  { name: "rent",          min: 80000,   max: 250000,  weight: 3 },
  { name: "travel",        min: 15000,   max: 300000,  weight: 4 },
  { name: "entertainment", min: 1000,    max: 15000,   weight: 3 },
];

/** Stripe test payment method tokens */
const TEST_PAYMENT_METHODS = [
  "pm_card_visa",
  "pm_card_mastercard",
  "pm_card_amex",
  "pm_card_discover",
];

const CHANNELS = ["online", "in_store", "mobile_app"] as const;
const DEVICES = ["desktop", "mobile", "tablet"] as const;
const RISK_TIERS = ["low", "medium", "high"] as const;
const KYC_STATUSES = ["verified", "pending", "rejected"] as const;
const CURRENCIES = ["usd", "usd", "usd", "usd", "eur", "gbp"] as const; // 67% USD

// ── Helpers ─────────────────────────────────────────────────────

function pick<T>(arr: readonly T[]): T {
  return arr[Math.floor(Math.random() * arr.length)];
}

function randInt(min: number, max: number): number {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

function weightedPick<T extends { weight: number }>(items: T[]): T {
  const total = items.reduce((s, i) => s + i.weight, 0);
  let r = Math.random() * total;
  for (const item of items) {
    r -= item.weight;
    if (r <= 0) return item;
  }
  return items[items.length - 1];
}

// ── Public API ──────────────────────────────────────────────────

/**
 * Create a Stripe test customer with realistic fake data.
 * Returns the full Stripe.Customer object.
 */
export async function createCustomer(
  stripe: Stripe,
): Promise<Stripe.Customer> {
  const first = pick(FIRST_NAMES);
  const last = pick(LAST_NAMES);
  const loc = pick(CITIES);

  return stripe.customers.create({
    name: `${first} ${last}`,
    email: `${first.toLowerCase()}.${last.toLowerCase()}@example.com`,
    phone: `+1${randInt(200, 999)}${randInt(100, 999)}${randInt(1000, 9999)}`,
    address: {
      line1: `${randInt(100, 9999)} ${pick(["Main", "Oak", "Pine", "Elm", "Cedar", "Maple", "Park", "Lake", "River", "Hill"])} ${pick(["St", "Ave", "Blvd", "Dr", "Ln", "Ct"])}`,
      city: loc.city,
      state: loc.state,
      postal_code: loc.zip,
      country: "US",
    },
    metadata: {
      risk_tier: pick(RISK_TIERS),
      kyc_status: pick(KYC_STATUSES),
      source: "fintech-data-source",
    },
  });
}

/**
 * Create and confirm a Stripe test PaymentIntent.
 * Uses test payment method tokens — no real card is charged.
 */
export async function createCharge(
  stripe: Stripe,
  customerId: string,
): Promise<Stripe.PaymentIntent> {
  const category = weightedPick(CATEGORIES);
  const amount = randInt(category.min, category.max);
  const currency = pick(CURRENCIES);
  const channel = pick(CHANNELS);

  return stripe.paymentIntents.create({
    amount,
    currency,
    customer: customerId,
    payment_method: pick(TEST_PAYMENT_METHODS),
    confirm: true,                          // Auto-confirm in test mode
    automatic_payment_methods: {
      enabled: true,
      allow_redirects: "never",
    },
    metadata: {
      channel,
      merchant_category: category.name,
      device_type: pick(DEVICES),
      location_city: pick(CITIES).city,
      source: "fintech-data-source",
    },
    description: `Test ${category.name} purchase — ${channel}`,
  });
}

/**
 * Create a Stripe test refund for a recent PaymentIntent.
 * Randomly chooses full or partial refund.
 */
export async function createRefund(
  stripe: Stripe,
  paymentIntentId: string,
): Promise<Stripe.Refund> {
  // Retrieve the PI to know how much was charged
  const pi = await stripe.paymentIntents.retrieve(paymentIntentId);
  const isPartial = Math.random() < 0.4;
  const amount = isPartial
    ? randInt(Math.floor(pi.amount * 0.1), Math.floor(pi.amount * 0.8))
    : undefined; // undefined = full refund

  return stripe.refunds.create({
    payment_intent: paymentIntentId,
    amount,
    reason: pick(["duplicate", "fraudulent", "requested_by_customer"]),
    metadata: { source: "fintech-data-source" },
  });
}
