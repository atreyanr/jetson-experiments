# Databricks notebook source

# MAGIC %md
# MAGIC # 🎲 Notebook 01 — Synthetic Data Generation
# MAGIC
# MAGIC Generates realistic fintech data and lands it as **JSON Lines** files in
# MAGIC the Unity Catalog volume `fintech_lab.bronze.landing_zone`.
# MAGIC
# MAGIC ### Scale
# MAGIC | Entity | Count |
# MAGIC |--------|------:|
# MAGIC | Customers | 5,000 |
# MAGIC | Accounts | ~12,000 |
# MAGIC | Merchants | 2,000 |
# MAGIC | Transactions | ~250,000 (6 months) |
# MAGIC | Exchange rates | 180 days × 3 pairs |
# MAGIC
# MAGIC ### Intentional quality issues
# MAGIC Real data is messy.  We inject **ten categories** of quality problems so
# MAGIC the silver-layer cleaning is realistic:
# MAGIC
# MAGIC 1. ~5 % duplicate transactions (same `transaction_id`)
# MAGIC 2. ~3 % null `merchant_id` on purchase transactions
# MAGIC 3. ~2 % negative amounts on non-refund / non-fee rows
# MAGIC 4. ~1 % future-dated transactions
# MAGIC 5. Mixed date formats for `date_of_birth`
# MAGIC 6. Mixed casing on city names
# MAGIC 7. ~2 % invalid email addresses
# MAGIC 8. Various phone-number formats
# MAGIC 9. ~1 % orphan accounts (non-existent `customer_id`)
# MAGIC 10. Floating-point artifacts on amounts
# MAGIC
# MAGIC > **Reproducible**: random seed is fixed at `42`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0 · Imports & seed

# COMMAND ----------

import random
import uuid
import json
import math
from datetime import datetime, timedelta
from pyspark.sql import Row
from pyspark.sql.types import *

SEED = 42
random.seed(SEED)

VOLUME = "/Volumes/fintech_lab/bronze/landing_zone"

# Date range for transactions: 2024-01-01 → 2024-06-30
START_DATE = datetime(2024, 1, 1)
END_DATE   = datetime(2024, 6, 30)
NUM_DAYS   = (END_DATE - START_DATE).days + 1  # 182 days

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Reference data — Merchants
# MAGIC
# MAGIC Realistic merchant names with proper **MCC codes** (Merchant Category
# MAGIC Codes as defined by ISO 18245).

# COMMAND ----------

# -- Merchant templates: (name_prefix, mcc_code, mcc_description, is_online, amount_range) --
MERCHANT_TEMPLATES = [
    # Coffee / fast food
    ("Starbucks",           "5814", "Fast Food / Coffee",        False, (3.50, 9.00)),
    ("Dunkin'",             "5814", "Fast Food / Coffee",        False, (2.50, 8.00)),
    ("McDonald's",          "5814", "Fast Food / Coffee",        False, (4.00, 15.00)),
    ("Chick-fil-A",         "5814", "Fast Food / Coffee",        False, (5.00, 18.00)),
    # Grocery
    ("Whole Foods",         "5411", "Grocery Stores",            False, (15.00, 250.00)),
    ("Trader Joe's",        "5411", "Grocery Stores",            False, (12.00, 180.00)),
    ("Kroger",              "5411", "Grocery Stores",            False, (8.00, 200.00)),
    ("Costco",              "5411", "Grocery Stores",            False, (40.00, 400.00)),
    # Gas
    ("Shell",               "5541", "Gas Stations",              False, (20.00, 90.00)),
    ("Chevron",             "5541", "Gas Stations",              False, (20.00, 85.00)),
    ("ExxonMobil",          "5541", "Gas Stations",              False, (25.00, 95.00)),
    # Online retail
    ("Amazon.com",          "5942", "Online Retail",             True,  (5.00, 500.00)),
    ("Walmart.com",         "5311", "Department Stores",         True,  (8.00, 350.00)),
    ("Target.com",          "5311", "Department Stores",         True,  (10.00, 300.00)),
    ("Best Buy",            "5732", "Electronics Stores",        True,  (15.00, 1500.00)),
    ("Apple Store",         "5732", "Electronics Stores",        True,  (0.99, 2500.00)),
    # Streaming / subscriptions
    ("Netflix",             "4899", "Cable / Streaming",         True,  (6.99, 22.99)),
    ("Spotify",             "4899", "Cable / Streaming",         True,  (4.99, 16.99)),
    ("Hulu",                "4899", "Cable / Streaming",         True,  (7.99, 17.99)),
    ("Disney+",             "4899", "Cable / Streaming",         True,  (7.99, 13.99)),
    # Restaurants
    ("Olive Garden",        "5812", "Restaurants",               False, (15.00, 120.00)),
    ("Chipotle",            "5812", "Restaurants",               False, (8.00, 30.00)),
    ("Panera Bread",        "5812", "Restaurants",               False, (7.00, 25.00)),
    # Travel
    ("United Airlines",     "3000", "Airlines",                  True,  (150.00, 1200.00)),
    ("Delta Air Lines",     "3000", "Airlines",                  True,  (120.00, 1500.00)),
    ("Marriott",            "7011", "Hotels / Lodging",          True,  (89.00, 450.00)),
    ("Hilton",              "7011", "Hotels / Lodging",          True,  (79.00, 400.00)),
    ("Uber",                "4121", "Rideshare / Taxi",          True,  (5.00, 80.00)),
    ("Lyft",                "4121", "Rideshare / Taxi",          True,  (4.00, 75.00)),
    # Utilities / rent
    ("ConEdison",           "4900", "Utilities",                 False, (50.00, 300.00)),
    ("Verizon",             "4812", "Telecom",                   False, (40.00, 200.00)),
    ("AT&T",                "4812", "Telecom",                   False, (35.00, 180.00)),
    # Healthcare
    ("CVS Pharmacy",        "5912", "Pharmacy",                  False, (5.00, 150.00)),
    ("Walgreens",           "5912", "Pharmacy",                  False, (4.00, 120.00)),
]

US_CITIES = [
    ("New York",      "NY", "US"), ("Los Angeles",   "CA", "US"),
    ("Chicago",       "IL", "US"), ("Houston",       "TX", "US"),
    ("Phoenix",       "AZ", "US"), ("Philadelphia",  "PA", "US"),
    ("San Antonio",   "TX", "US"), ("San Diego",     "CA", "US"),
    ("Dallas",        "TX", "US"), ("Austin",        "TX", "US"),
    ("Denver",        "CO", "US"), ("Seattle",       "WA", "US"),
    ("Boston",        "MA", "US"), ("Nashville",     "TN", "US"),
    ("Portland",      "OR", "US"), ("Miami",         "FL", "US"),
    ("Atlanta",       "GA", "US"), ("Minneapolis",   "MN", "US"),
    ("Charlotte",     "NC", "US"), ("San Francisco",  "CA", "US"),
]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate 2,000 merchants
# MAGIC Each template is replicated across random US cities to reach 2,000.

# COMMAND ----------

def generate_merchants(n=2000):
    merchants = []
    template_count = len(MERCHANT_TEMPLATES)
    for i in range(n):
        tmpl = MERCHANT_TEMPLATES[i % template_count]
        name_prefix, mcc, mcc_desc, is_online, amt_range = tmpl
        city, state, country = random.choice(US_CITIES)
        # Add a branch number for duplicated templates
        suffix = f" #{i // template_count + 1}" if i >= template_count else ""
        merchants.append({
            "merchant_id":      f"m-{i+1:04d}",
            "merchant_name":    f"{name_prefix}{suffix}",
            "mcc_code":         mcc,
            "mcc_description":  mcc_desc,
            "city":             city,
            "state":            state,
            "country":          country,
            "is_online":        is_online,
            # stash amount range for transaction generation (not persisted)
            "_amt_lo":          amt_range[0],
            "_amt_hi":          amt_range[1],
        })
    return merchants

MERCHANTS = generate_merchants()

# Mapping for fast lookup during txn generation
MERCHANT_MAP = {m["merchant_id"]: m for m in MERCHANTS}

print(f"Generated {len(MERCHANTS)} merchants across {len(MERCHANT_TEMPLATES)} templates")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Customers
# MAGIC
# MAGIC 5,000 US-based customers.  Quality issues injected:
# MAGIC - **Mixed date formats** for `date_of_birth`
# MAGIC - **Invalid emails** (~2 %)
# MAGIC - **Mixed city casing** (`new york`, `NEW YORK`, `New York`)
# MAGIC - **Various phone formats** (`(555) 123-4567`, `555-123-4567`,
# MAGIC   `5551234567`, `+1-555-123-4567`)

# COMMAND ----------

FIRST_NAMES = [
    "James","Mary","Robert","Patricia","John","Jennifer","Michael","Linda",
    "David","Elizabeth","William","Barbara","Richard","Susan","Joseph","Jessica",
    "Thomas","Sarah","Christopher","Karen","Charles","Lisa","Daniel","Nancy",
    "Matthew","Betty","Anthony","Margaret","Mark","Sandra","Donald","Ashley",
    "Steven","Kimberly","Andrew","Emily","Paul","Donna","Joshua","Michelle",
    "Kenneth","Carol","Kevin","Amanda","Brian","Dorothy","George","Melissa",
    "Timothy","Deborah","Ronald","Stephanie","Edward","Rebecca","Jason","Sharon",
    "Jeffrey","Laura","Ryan","Cynthia","Jacob","Kathleen","Nicholas","Amy",
    "Aiden","Sofia","Liam","Olivia","Noah","Emma","Ethan","Ava",
    "Mason","Isabella","Logan","Mia","Lucas","Charlotte","Raj","Priya",
    "Wei","Mei","Hiroshi","Yuki","Carlos","Maria","Ahmed","Fatima",
]

LAST_NAMES = [
    "Smith","Johnson","Williams","Brown","Jones","Garcia","Miller","Davis",
    "Rodriguez","Martinez","Hernandez","Lopez","Gonzalez","Wilson","Anderson",
    "Thomas","Taylor","Moore","Jackson","Martin","Lee","Perez","Thompson",
    "White","Harris","Sanchez","Clark","Ramirez","Lewis","Robinson","Walker",
    "Young","Allen","King","Wright","Scott","Torres","Nguyen","Hill",
    "Flores","Green","Adams","Nelson","Baker","Hall","Rivera","Campbell",
    "Mitchell","Carter","Roberts","Patel","Shah","Kim","Chen","Wang",
    "Singh","Kumar","Nakamura","Tanaka","Santos","Reyes","Cruz","Morales",
]

US_STATES = [
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN",
    "IA","KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV",
    "NH","NJ","NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN",
    "TX","UT","VT","VA","WA","WV","WI","WY",
]

PHONE_FORMATS = [
    "({area}) {p1}-{p2}",        # (555) 123-4567
    "{area}-{p1}-{p2}",          # 555-123-4567
    "{area}{p1}{p2}",            # 5551234567
    "+1-{area}-{p1}-{p2}",      # +1-555-123-4567
    "+1 ({area}) {p1}-{p2}",    # +1 (555) 123-4567
    "1{area}{p1}{p2}",          # 15551234567
]

DATE_FORMATS = [
    "%Y-%m-%d",      # 1990-03-15  (ISO — correct)
    "%m/%d/%Y",      # 03/15/1990  (US)
    "%d-%m-%Y",      # 15-03-1990  (European)
]

CITY_CASING_VARIANTS = [str.title, str.lower, str.upper]  # "New York", "new york", "NEW YORK"


def _random_phone():
    area = str(random.randint(200, 999))
    p1   = str(random.randint(200, 999))
    p2   = f"{random.randint(0, 9999):04d}"
    fmt  = random.choice(PHONE_FORMATS)
    return fmt.format(area=area, p1=p1, p2=p2)


def _random_email(first, last, invalid=False):
    if invalid:
        # Quality issue #7 — invalid emails
        bad = random.choice([
            f"{first}{last}",                     # missing @
            f"{first}@@example.com",              # double @
            f"@example.com",                      # missing local part
            f"{first}.{last}@",                   # missing domain
            f"{first} {last}@example.com",        # space in address
        ])
        return bad
    domain = random.choice([
        "gmail.com", "yahoo.com", "outlook.com", "hotmail.com",
        "icloud.com", "protonmail.com", "aol.com", "mail.com",
    ])
    sep = random.choice([".", "_", ""])
    num = random.choice(["", str(random.randint(1, 99))])
    return f"{first.lower()}{sep}{last.lower()}{num}@{domain}"


def _random_dob():
    """Random DOB between 1950 and 2005, in a randomly chosen format."""
    year  = random.randint(1950, 2005)
    month = random.randint(1, 12)
    day   = random.randint(1, 28)  # safe for all months
    dt    = datetime(year, month, day)
    fmt   = random.choice(DATE_FORMATS)  # Quality issue #5
    return dt.strftime(fmt)


def generate_customers(n=5000):
    customers = []
    for i in range(n):
        first = random.choice(FIRST_NAMES)
        last  = random.choice(LAST_NAMES)
        city, state, _ = random.choice(US_CITIES)

        # Quality issue #6 — mixed casing
        city = random.choice(CITY_CASING_VARIANTS)(city)

        # Quality issue #7 — ~2% invalid emails
        is_invalid_email = random.random() < 0.02

        created = START_DATE - timedelta(days=random.randint(30, 1800))

        customers.append({
            "customer_id":    f"c-{i+1:05d}",
            "first_name":     first,
            "last_name":      last,
            "email":          _random_email(first, last, invalid=is_invalid_email),
            "phone":          _random_phone(),
            "date_of_birth":  _random_dob(),
            "street_address": f"{random.randint(1,9999)} {random.choice(LAST_NAMES)} {random.choice(['St','Ave','Blvd','Dr','Ln','Ct'])}",
            "city":           city,
            "state":          state,
            "zip_code":       f"{random.randint(10000,99999)}",
            "country":        "US",
            "created_at":     created.isoformat() + "Z",
            "kyc_status":     random.choices(["verified","pending","rejected"], weights=[0.85, 0.10, 0.05])[0],
            "risk_tier":      random.choices(["low","medium","high"],           weights=[0.70, 0.20, 0.10])[0],
        })
    return customers

CUSTOMERS = generate_customers()
CUSTOMER_IDS = [c["customer_id"] for c in CUSTOMERS]

print(f"Generated {len(CUSTOMERS)} customers")
invalid_emails = sum(1 for c in CUSTOMERS if "@" not in c["email"] or "@@" in c["email"] or c["email"].startswith("@") or c["email"].endswith("@") or " " in c["email"])
print(f"  → invalid emails: {invalid_emails} ({100*invalid_emails/len(CUSTOMERS):.1f}%)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Accounts
# MAGIC
# MAGIC ~12,000 accounts — each customer gets 1–4 accounts.
# MAGIC
# MAGIC Quality issue injected:
# MAGIC - **~1 % orphan accounts** whose `customer_id` doesn't exist in the
# MAGIC   customers table.

# COMMAND ----------

ACCOUNT_TYPES_WEIGHTS = [
    ("checking",    0.40),
    ("savings",     0.30),
    ("credit_card", 0.20),
    ("investment",  0.10),
]

CURRENCIES_WEIGHTS = [("USD", 0.90), ("EUR", 0.06), ("GBP", 0.04)]


def generate_accounts(customers, target=12000):
    accounts = []
    acct_idx = 0

    # Assign 1-4 accounts per customer
    for cust in customers:
        n_accts = random.choices([1, 2, 3, 4], weights=[0.20, 0.45, 0.25, 0.10])[0]
        for _ in range(n_accts):
            acct_type = random.choices(
                [a[0] for a in ACCOUNT_TYPES_WEIGHTS],
                weights=[a[1] for a in ACCOUNT_TYPES_WEIGHTS],
            )[0]
            currency = random.choices(
                [c[0] for c in CURRENCIES_WEIGHTS],
                weights=[c[1] for c in CURRENCIES_WEIGHTS],
            )[0]

            opened = START_DATE - timedelta(days=random.randint(1, 1500))

            acct = {
                "account_id":    f"a-{acct_idx+1:05d}",
                "customer_id":   cust["customer_id"],
                "account_type":  acct_type,
                "account_number": f"****{random.randint(1000,9999)}",
                "currency":      currency,
                "opened_at":     opened.isoformat() + "Z",
                "status":        random.choices(["active","frozen","closed"], weights=[0.90, 0.05, 0.05])[0],
                "credit_limit":  round(random.choice([1000,2500,5000,10000,15000,25000]), 2) if acct_type == "credit_card" else None,
                "interest_rate": round(random.uniform(0.01, 0.28), 4) if acct_type in ("savings","credit_card","investment") else 0.0,
            }
            accounts.append(acct)
            acct_idx += 1

            if acct_idx >= target:
                break
        if acct_idx >= target:
            break

    # Quality issue #9 — ~1% orphan accounts with non-existent customer_id
    n_orphans = max(1, int(len(accounts) * 0.01))
    orphan_indices = random.sample(range(len(accounts)), n_orphans)
    for idx in orphan_indices:
        accounts[idx]["customer_id"] = f"c-{random.randint(90000,99999):05d}"

    return accounts

ACCOUNTS = generate_accounts(CUSTOMERS)
ACCOUNT_IDS = [a["account_id"] for a in ACCOUNTS]
ACCOUNT_MAP = {a["account_id"]: a for a in ACCOUNTS}

orphan_count = sum(1 for a in ACCOUNTS if a["customer_id"] not in {c["customer_id"] for c in CUSTOMERS})
print(f"Generated {len(ACCOUNTS)} accounts")
print(f"  → orphan accounts: {orphan_count} ({100*orphan_count/len(ACCOUNTS):.1f}%)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Transactions
# MAGIC
# MAGIC ~250,000 transactions over 6 months.  This is the richest table and
# MAGIC carries the most quality issues:
# MAGIC
# MAGIC | Issue | Approx % |
# MAGIC |-------|-------:|
# MAGIC | Duplicate `transaction_id` | 5 % |
# MAGIC | Null `merchant_id` on purchases | 3 % |
# MAGIC | Negative amount (non-refund) | 2 % |
# MAGIC | Future-dated | 1 % |
# MAGIC | Floating-point artifact | 5 % |

# COMMAND ----------

TXN_TYPES_WEIGHTS = [
    ("purchase",       0.55),
    ("deposit",        0.12),
    ("transfer_out",   0.08),
    ("transfer_in",    0.07),
    ("atm_withdrawal", 0.06),
    ("refund",         0.05),
    ("fee",            0.04),
    ("interest",       0.03),
]

CHANNELS = ["online", "in_store", "atm", "mobile_app"]
DEVICES  = ["desktop", "mobile", "tablet"]
STATUSES_WEIGHTS = [("completed", 0.92), ("pending", 0.04), ("failed", 0.03), ("reversed", 0.01)]


def _random_ip():
    return f"{random.randint(1,223)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}"


def generate_transactions(accounts, merchants, n=250000):
    txns = []
    merchant_ids = [m["merchant_id"] for m in merchants]

    for i in range(n):
        txn_type = random.choices(
            [t[0] for t in TXN_TYPES_WEIGHTS],
            weights=[t[1] for t in TXN_TYPES_WEIGHTS],
        )[0]

        acct = random.choice(accounts)
        acct_id = acct["account_id"]

        # Pick merchant for purchase/refund, None otherwise
        if txn_type in ("purchase", "refund"):
            merch = random.choice(merchants)
            merchant_id = merch["merchant_id"]
            amt_lo, amt_hi = merch["_amt_lo"], merch["_amt_hi"]
            amount = round(random.uniform(amt_lo, amt_hi), 2)
        else:
            merchant_id = None
            if txn_type == "deposit":
                amount = round(random.uniform(100, 5000), 2)
            elif txn_type in ("transfer_in", "transfer_out"):
                amount = round(random.uniform(10, 3000), 2)
            elif txn_type == "atm_withdrawal":
                amount = round(random.choice([20, 40, 60, 80, 100, 200, 300, 500]), 2)
            elif txn_type == "fee":
                amount = round(random.choice([2.50, 5.00, 12.00, 25.00, 35.00]), 2)
            elif txn_type == "interest":
                amount = round(random.uniform(0.01, 50.00), 2)
            else:
                amount = round(random.uniform(1, 500), 2)

        # Timestamp — spread across 6 months with weekday bias
        day_offset = random.randint(0, NUM_DAYS - 1)
        txn_date = START_DATE + timedelta(days=day_offset)
        # Weekday bias: 70% chance it's a weekday
        if txn_date.weekday() >= 5 and random.random() < 0.30:
            txn_date -= timedelta(days=txn_date.weekday() - 4)  # shift to Friday
        hour   = random.choices(range(24), weights=[
            1,1,1,1,1,2,4,7,9,10,10,9,  # 00-11
            8,8,9,10,10,9,7,5,4,3,2,1   # 12-23
        ])[0]
        minute = random.randint(0, 59)
        second = random.randint(0, 59)
        txn_ts = txn_date.replace(hour=hour, minute=minute, second=second)

        # Channel logic
        if txn_type == "atm_withdrawal":
            channel = "atm"
        elif txn_type in ("purchase", "refund"):
            merch_data = MERCHANT_MAP.get(merchant_id, {})
            channel = "online" if merch_data.get("is_online") else random.choice(["in_store", "mobile_app"])
        else:
            channel = random.choice(["online", "mobile_app"])

        city, state, country = random.choice(US_CITIES)

        txn = {
            "transaction_id":       f"t-{i+1:06d}",
            "account_id":           acct_id,
            "merchant_id":          merchant_id,
            "transaction_type":     txn_type,
            "amount":               amount,
            "currency":             acct["currency"],
            "transaction_timestamp": txn_ts.isoformat() + "Z",
            "status":               random.choices(
                                        [s[0] for s in STATUSES_WEIGHTS],
                                        weights=[s[1] for s in STATUSES_WEIGHTS]
                                    )[0],
            "channel":              channel,
            "location_city":        city,
            "location_country":     country,
            "ip_address":           _random_ip() if channel in ("online", "mobile_app") else None,
            "device_type":          random.choice(DEVICES) if channel in ("online", "mobile_app") else None,
        }
        txns.append(txn)

    # ── Inject quality issues ──────────────────────────────────────────

    # Issue #1 — ~5% duplicate transactions
    n_dupes = int(len(txns) * 0.05)
    dupe_sources = random.sample(range(len(txns)), n_dupes)
    for idx in dupe_sources:
        txns.append(dict(txns[idx]))  # exact duplicate

    # Issue #2 — ~3% null merchant_id on purchases
    purchase_indices = [i for i, t in enumerate(txns) if t["transaction_type"] == "purchase" and t["merchant_id"] is not None]
    n_null_merch = int(len(purchase_indices) * 0.03)
    for idx in random.sample(purchase_indices, min(n_null_merch, len(purchase_indices))):
        txns[idx]["merchant_id"] = None

    # Issue #3 — ~2% negative amounts on non-refund/non-fee rows
    eligible = [i for i, t in enumerate(txns) if t["transaction_type"] not in ("refund", "fee")]
    n_neg = int(len(eligible) * 0.02)
    for idx in random.sample(eligible, min(n_neg, len(eligible))):
        txns[idx]["amount"] = -abs(txns[idx]["amount"])

    # Issue #4 — ~1% future-dated
    n_future = int(len(txns) * 0.01)
    for idx in random.sample(range(len(txns)), n_future):
        future_date = END_DATE + timedelta(days=random.randint(30, 365))
        txns[idx]["transaction_timestamp"] = future_date.isoformat() + "Z"

    # Issue #10 — ~5% floating-point artifacts
    n_fp = int(len(txns) * 0.05)
    for idx in random.sample(range(len(txns)), n_fp):
        txns[idx]["amount"] = txns[idx]["amount"] + 1e-10 * random.randint(1, 100)

    # Shuffle so issues are spread around
    random.shuffle(txns)

    return txns

TRANSACTIONS = generate_transactions(ACCOUNTS, MERCHANTS)

# Stats
n_dupes = len(TRANSACTIONS) - 250000
n_null_merch = sum(1 for t in TRANSACTIONS if t["transaction_type"] == "purchase" and t["merchant_id"] is None)
n_neg = sum(1 for t in TRANSACTIONS if t["amount"] < 0 and t["transaction_type"] not in ("refund", "fee"))
n_future = sum(1 for t in TRANSACTIONS if datetime.fromisoformat(t["transaction_timestamp"].rstrip("Z")) > END_DATE)
print(f"Generated {len(TRANSACTIONS)} transactions (including dupes)")
print(f"  → duplicates:     ~{n_dupes}")
print(f"  → null merchant:  ~{n_null_merch}")
print(f"  → negative amt:   ~{n_neg}")
print(f"  → future-dated:   ~{n_future}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Exchange Rates
# MAGIC
# MAGIC 180 days of daily rates for USD → EUR, USD → GBP, and EUR → GBP.
# MAGIC Rates are synthesised with small random daily drift to look realistic.

# COMMAND ----------

def generate_exchange_rates(n_days=180):
    rates = []
    # Starting rates (approximate real values)
    current = {"USD_EUR": 0.92, "USD_GBP": 0.79, "EUR_GBP": 0.86}
    pairs = [
        ("USD", "EUR", "USD_EUR"),
        ("USD", "GBP", "USD_GBP"),
        ("EUR", "GBP", "EUR_GBP"),
    ]
    for day_offset in range(n_days):
        dt = START_DATE + timedelta(days=day_offset)
        for base, target, key in pairs:
            # Random walk ±0.3% per day
            current[key] *= (1 + random.uniform(-0.003, 0.003))
            rates.append({
                "rate_date":       dt.strftime("%Y-%m-%d"),
                "base_currency":   base,
                "target_currency": target,
                "rate":            round(current[key], 6),
            })
    return rates

EXCHANGE_RATES = generate_exchange_rates()
print(f"Generated {len(EXCHANGE_RATES)} exchange rate records ({len(EXCHANGE_RATES)//3} days × 3 pairs)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · Write everything to the landing zone
# MAGIC
# MAGIC Files land as **JSON Lines** (one JSON object per line) in the Unity
# MAGIC Catalog volume.  Transactions are partitioned by month to simulate
# MAGIC incremental batch landing.

# COMMAND ----------

def write_jsonl(records, path):
    """Write a list of dicts as JSON Lines to the volume."""
    lines = [json.dumps(r) for r in records]
    dbutils.fs.put(path.replace("/Volumes/", "dbfs:/Volumes/"), "\n".join(lines), overwrite=True)
    return len(lines)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6a · Customers

# COMMAND ----------

n = write_jsonl(CUSTOMERS, f"{VOLUME}/customers/customers.jsonl")
print(f"✅ Wrote {n} customer records → {VOLUME}/customers/customers.jsonl")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6b · Accounts

# COMMAND ----------

n = write_jsonl(ACCOUNTS, f"{VOLUME}/accounts/accounts.jsonl")
print(f"✅ Wrote {n} account records → {VOLUME}/accounts/accounts.jsonl")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6c · Merchants

# COMMAND ----------

# Strip internal fields before writing
merchants_clean = [{k: v for k, v in m.items() if not k.startswith("_")} for m in MERCHANTS]
n = write_jsonl(merchants_clean, f"{VOLUME}/merchants/merchants.jsonl")
print(f"✅ Wrote {n} merchant records → {VOLUME}/merchants/merchants.jsonl")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6d · Transactions (partitioned by month)

# COMMAND ----------

from collections import defaultdict

txn_by_month = defaultdict(list)
for txn in TRANSACTIONS:
    ts = txn["transaction_timestamp"]
    month_key = ts[:7]  # "2024-01"
    txn_by_month[month_key].append(txn)

total_written = 0
for month_key in sorted(txn_by_month.keys()):
    records = txn_by_month[month_key]
    path = f"{VOLUME}/transactions/transactions_{month_key.replace('-','')}.jsonl"
    n = write_jsonl(records, path)
    total_written += n
    print(f"  {month_key}: {n:,} records → {path}")

print(f"\n✅ Total transaction records written: {total_written:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6e · Exchange Rates

# COMMAND ----------

n = write_jsonl(EXCHANGE_RATES, f"{VOLUME}/exchange_rates/rates.jsonl")
print(f"✅ Wrote {n} exchange rate records → {VOLUME}/exchange_rates/rates.jsonl")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 · Summary

# COMMAND ----------

print("=" * 60)
print("  DATA GENERATION COMPLETE")
print("=" * 60)
print()
print(f"  Customers:       {len(CUSTOMERS):>10,}")
print(f"  Accounts:        {len(ACCOUNTS):>10,}")
print(f"  Merchants:       {len(merchants_clean):>10,}")
print(f"  Transactions:    {len(TRANSACTIONS):>10,}")
print(f"  Exchange Rates:  {len(EXCHANGE_RATES):>10,}")
print()
print("  Quality issues injected:")
print(f"    Duplicate txns:        ~{len(TRANSACTIONS) - 250000:,}")
print(f"    Null merchant on purch: ~{n_null_merch:,}")
print(f"    Negative amounts:       ~{n_neg:,}")
print(f"    Future-dated:           ~{n_future:,}")
print(f"    Invalid emails:         ~{invalid_emails:,}")
print(f"    Orphan accounts:        ~{orphan_count:,}")
print(f"    Mixed date formats:     ✔ (3 formats)")
print(f"    Mixed city casing:      ✔ (title/lower/UPPER)")
print(f"    Phone format variants:  ✔ (6 formats)")
print(f"    Float-point artifacts:  ~{int(len(TRANSACTIONS)*0.05):,}")
print()
print(f"  Volume: {VOLUME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Next →** Run `02_bronze_ingestion` to read these files into Delta
# MAGIC tables in the `fintech_lab.bronze` schema.
