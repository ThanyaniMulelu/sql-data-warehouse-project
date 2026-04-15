"""
Generate realistic-scale CSV datasets for the SQL Data Warehouse project.

Target scale:
  - 150,000 customers  (was 18,484)
  - 2,000 products      (was 397)
  - 1,000,000 sales     (was 60,398)
  - Matching ERP tables

Preserves all original data-quality quirks:
  - Duplicate customer rows (same cst_id, different dates)
  - NAS-prefix on ~15% of ERP customer IDs
  - Hyphens in ~20% of location CIDs
  - Future birthdates (~0.5%)
  - Abbreviated + full-form gender in ERP
  - Mixed country codes (US, USA, DE, blank)
  - NULL/zero dates in sales
  - Mismatched sales != qty*price (~3%)
  - NULL costs in products
  - Trailing spaces on some fields
"""

import csv
import os
import random
from datetime import date, timedelta

random.seed(42)

OUT_DIR = os.path.join(os.path.dirname(__file__), "datasets")
CRM_DIR = os.path.join(OUT_DIR, "source_crm")
ERP_DIR = os.path.join(OUT_DIR, "source_erp")

os.makedirs(CRM_DIR, exist_ok=True)
os.makedirs(ERP_DIR, exist_ok=True)

# ── Reference data ──────────────────────────────────────────────────────────

CATEGORIES = [
    ("AC_BR", "Accessories", "Bike Racks", "Yes"),
    ("AC_BS", "Accessories", "Bike Stands", "No"),
    ("AC_BC", "Accessories", "Bottles and Cages", "No"),
    ("AC_CL", "Accessories", "Cleaners", "Yes"),
    ("AC_FE", "Accessories", "Fenders", "No"),
    ("AC_HE", "Accessories", "Helmets", "Yes"),
    ("AC_HP", "Accessories", "Hydration Packs", "No"),
    ("AC_LI", "Accessories", "Lights", "Yes"),
    ("AC_LO", "Accessories", "Locks", "Yes"),
    ("AC_PA", "Accessories", "Panniers", "No"),
    ("AC_PU", "Accessories", "Pumps", "Yes"),
    ("AC_TT", "Accessories", "Tires and Tubes", "Yes"),
    ("BI_MB", "Bikes", "Mountain Bikes", "Yes"),
    ("BI_RB", "Bikes", "Road Bikes", "Yes"),
    ("BI_TB", "Bikes", "Touring Bikes", "Yes"),
    ("CL_BS", "Clothing", "Bib-Shorts", "No"),
    ("CL_CA", "Clothing", "Caps", "No"),
    ("CL_GL", "Clothing", "Gloves", "No"),
    ("CL_JE", "Clothing", "Jerseys", "No"),
    ("CL_SH", "Clothing", "Shorts", "No"),
    ("CL_SO", "Clothing", "Socks", "No"),
    ("CL_TI", "Clothing", "Tights", "No"),
    ("CL_VE", "Clothing", "Vests", "No"),
    ("CO_BB", "Components", "Bottom Brackets", "Yes"),
    ("CO_BR", "Components", "Brakes", "Yes"),
    ("CO_CH", "Components", "Chains", "Yes"),
    ("CO_CS", "Components", "Cranksets", "Yes"),
    ("CO_DE", "Components", "Derailleurs", "Yes"),
    ("CO_FO", "Components", "Forks", "Yes"),
    ("CO_HB", "Components", "Handlebars", "No"),
    ("CO_HS", "Components", "Headsets", "No"),
    ("CO_MF", "Components", "Mountain Frames", "Yes"),
    ("CO_PD", "Components", "Pedals", "No"),
    ("CO_RF", "Components", "Road Frames", "Yes"),
    ("CO_SA", "Components", "Saddles", "No"),
    ("CO_TF", "Components", "Touring Frames", "Yes"),
    ("CO_WH", "Components", "Wheels", "Yes"),
]

FIRST_NAMES = [
    "James", "Mary", "Robert", "Patricia", "John", "Jennifer", "Michael", "Linda",
    "David", "Elizabeth", "William", "Barbara", "Richard", "Susan", "Joseph", "Jessica",
    "Thomas", "Sarah", "Christopher", "Karen", "Charles", "Lisa", "Daniel", "Nancy",
    "Matthew", "Betty", "Anthony", "Margaret", "Mark", "Sandra", "Donald", "Ashley",
    "Steven", "Kimberly", "Andrew", "Emily", "Paul", "Donna", "Joshua", "Michelle",
    "Kenneth", "Carol", "Kevin", "Amanda", "Brian", "Dorothy", "George", "Melissa",
    "Timothy", "Deborah", "Ronald", "Stephanie", "Edward", "Rebecca", "Jason", "Sharon",
    "Jeffrey", "Laura", "Ryan", "Cynthia", "Jacob", "Kathleen", "Gary", "Amy",
    "Nicholas", "Angela", "Eric", "Shirley", "Jonathan", "Anna", "Stephen", "Brenda",
    "Larry", "Pamela", "Justin", "Emma", "Scott", "Nicole", "Brandon", "Helen",
    "Benjamin", "Samantha", "Samuel", "Katherine", "Raymond", "Christine", "Gregory", "Debra",
    "Frank", "Rachel", "Alexander", "Carolyn", "Patrick", "Janet", "Jack", "Catherine",
    "Dennis", "Maria", "Jerry", "Heather", "Tyler", "Diane", "Aaron", "Ruth",
    "Jose", "Julie", "Adam", "Olivia", "Nathan", "Joyce", "Henry", "Virginia",
    "Douglas", "Victoria", "Zachary", "Kelly", "Peter", "Lauren", "Kyle", "Christina",
    "Thanyani", "Lindiwe", "Sipho", "Naledi", "Tshegofatso", "Mpho", "Kagiso", "Lesedi",
    "Bongani", "Zanele", "Liam", "Fatima", "Amir", "Aisha", "Wei", "Yuki",
    "Carlos", "Sofia", "Ivan", "Ingrid", "Lars", "Freya", "Hans", "Greta",
    "Pierre", "Marie", "Ahmed", "Priya", "Ravi", "Mei", "Jin", "Sakura",
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
    "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
    "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson",
    "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson", "Walker",
    "Young", "Allen", "King", "Wright", "Scott", "Torres", "Nguyen", "Hill",
    "Flores", "Green", "Adams", "Nelson", "Baker", "Hall", "Rivera", "Campbell",
    "Mitchell", "Carter", "Roberts", "Gomez", "Phillips", "Evans", "Turner", "Diaz",
    "Parker", "Cruz", "Edwards", "Collins", "Reyes", "Stewart", "Morris", "Morales",
    "Murphy", "Cook", "Rogers", "Gutierrez", "Ortiz", "Morgan", "Cooper", "Peterson",
    "Bailey", "Reed", "Kelly", "Howard", "Ramos", "Kim", "Cox", "Ward",
    "Mulelu", "Nkosi", "Dlamini", "Mokoena", "Ndlovu", "Mahlangu", "Maseko", "Zulu",
    "Van der Merwe", "Botha", "Naidoo", "Pillay", "Singh", "Patel", "Chen", "Wang",
    "Tanaka", "Yamamoto", "Müller", "Schmidt", "Schneider", "Fischer", "Weber", "Meyer",
    "Dubois", "Bernard", "Moreau", "Laurent", "Ivanov", "Smirnov", "Johansson", "Eriksson",
]

PRODUCT_NAMES = {
    "AC_BR": ["Explorer Bike Rack", "Summit Bike Rack", "Urban Bike Rack", "Trail Bike Rack"],
    "AC_BS": ["Pro Bike Stand", "Garage Bike Stand", "Portable Bike Stand"],
    "AC_BC": ["Sport Bottle", "Insulated Bottle", "Cage Mount-500", "Cage Mount-700"],
    "AC_CL": ["All-Purpose Cleaner", "Chain Degreaser", "Frame Polish"],
    "AC_FE": ["Front Fender-100", "Rear Fender-100", "Full Fender Set"],
    "AC_HE": ["Sport-100 Helmet- Red", "Sport-100 Helmet- Blue", "Sport-100 Helmet- Black",
              "Pro-200 Helmet- White", "Pro-200 Helmet- Red", "Youth Helmet"],
    "AC_HP": ["Trail Hydration Pack", "Enduro Hydration Pack"],
    "AC_LI": ["Front Light-500", "Rear Light-300", "Light Combo Set"],
    "AC_LO": ["Cable Lock-1000", "U-Lock Pro", "Chain Lock-800"],
    "AC_PA": ["Touring Pannier- Black", "Commuter Pannier- Grey"],
    "AC_PU": ["Mini Floor Pump", "Trail Hand Pump", "Pro Floor Pump"],
    "AC_TT": ["Road Tire-700x25", "Mountain Tire-26x2.1", "Tube-700c", "Tube-26in",
              "Patch Kit", "Touring Tire-700x32"],
    "BI_MB": ["Mountain-100 Silver- 38", "Mountain-100 Silver- 42", "Mountain-100 Silver- 44",
              "Mountain-100 Black- 38", "Mountain-100 Black- 42", "Mountain-100 Black- 44",
              "Mountain-200 Silver- 38", "Mountain-200 Silver- 42", "Mountain-200 Black- 38",
              "Mountain-200 Black- 42", "Mountain-300 Black- 38", "Mountain-300 Black- 42",
              "Mountain-400-W Silver- 38", "Mountain-400-W Silver- 42",
              "Mountain-500 Silver- 40", "Mountain-500 Silver- 42", "Mountain-500 Silver- 44",
              "Mountain-500 Black- 40", "Mountain-500 Black- 42", "Mountain-500 Black- 44"],
    "BI_RB": ["Road-150 Red- 44", "Road-150 Red- 48", "Road-150 Red- 52",
              "Road-150 Red- 56", "Road-250 Red- 44", "Road-250 Red- 48",
              "Road-250 Red- 52", "Road-250 Black- 44", "Road-250 Black- 48",
              "Road-350-W Yellow- 40", "Road-350-W Yellow- 44", "Road-350-W Yellow- 48",
              "Road-450 Red- 44", "Road-450 Red- 48", "Road-450 Red- 52",
              "Road-550-W Yellow- 38", "Road-550-W Yellow- 40",
              "Road-650 Red- 44", "Road-650 Red- 48", "Road-650 Red- 52",
              "Road-650 Black- 44", "Road-650 Black- 48", "Road-650 Black- 52",
              "Road-750 Black- 44", "Road-750 Black- 48", "Road-750 Black- 52"],
    "BI_TB": ["Touring-1000 Yellow- 46", "Touring-1000 Yellow- 50", "Touring-1000 Yellow- 54",
              "Touring-1000 Blue- 46", "Touring-1000 Blue- 50", "Touring-1000 Blue- 54",
              "Touring-2000 Blue- 46", "Touring-2000 Blue- 50", "Touring-2000 Blue- 54",
              "Touring-3000 Yellow- 44", "Touring-3000 Yellow- 50",
              "Touring-3000 Blue- 44", "Touring-3000 Blue- 50"],
    "CL_BS": ["Men Bib-Short- S", "Men Bib-Short- M", "Men Bib-Short- L"],
    "CL_CA": ["AWC Logo Cap", "Classic Cap- Black", "Classic Cap- White"],
    "CL_GL": ["Full-Finger Gloves- S", "Full-Finger Gloves- M", "Full-Finger Gloves- L",
              "Half-Finger Gloves- S", "Half-Finger Gloves- M", "Half-Finger Gloves- L"],
    "CL_JE": ["Long-Sleeve Logo Jersey- S", "Long-Sleeve Logo Jersey- M", "Long-Sleeve Logo Jersey- L",
              "Short-Sleeve Classic Jersey- S", "Short-Sleeve Classic Jersey- M",
              "Short-Sleeve Classic Jersey- L", "Short-Sleeve Classic Jersey- XL"],
    "CL_SH": ["Men Sports-Short- S", "Men Sports-Short- M", "Men Sports-Short- L",
              "Women Sports-Short- S", "Women Sports-Short- M"],
    "CL_SO": ["Mountain Bike Socks- M", "Mountain Bike Socks- L", "Racing Socks- M", "Racing Socks- L"],
    "CL_TI": ["Classic Tights- S", "Classic Tights- M", "Classic Tights- L"],
    "CL_VE": ["Classic Vest- S", "Classic Vest- M", "Classic Vest- L"],
    "CO_BB": ["LL Bottom Bracket", "ML Bottom Bracket", "HL Bottom Bracket"],
    "CO_BR": ["Front Brakes", "Rear Brakes"],
    "CO_CH": ["Chain-400", "Chain-500"],
    "CO_CS": ["LL Crankset", "ML Crankset", "HL Crankset"],
    "CO_DE": ["Front Derailleur", "Rear Derailleur"],
    "CO_FO": ["LL Fork", "ML Fork", "HL Fork"],
    "CO_HB": ["LL Mountain Handlebars", "HL Mountain Handlebars"],
    "CO_HS": ["LL Headset", "HL Headset"],
    "CO_MF": ["HL Mountain Frame- Silver- 38", "HL Mountain Frame- Silver- 42",
              "HL Mountain Frame- Silver- 44", "HL Mountain Frame- Silver- 46",
              "HL Mountain Frame- Black- 38", "HL Mountain Frame- Black- 42",
              "HL Mountain Frame- Black- 44", "HL Mountain Frame- Black- 46",
              "ML Mountain Frame- Black- 38", "ML Mountain Frame- Black- 40",
              "ML Mountain Frame-W Silver- 38", "ML Mountain Frame-W Silver- 40",
              "LL Mountain Frame- Silver- 42", "LL Mountain Frame- Black- 42"],
    "CO_PD": ["LL Mountain Pedal", "ML Mountain Pedal", "HL Mountain Pedal",
              "LL Road Pedal", "ML Road Pedal", "HL Road Pedal",
              "Touring Pedal"],
    "CO_RF": ["HL Road Frame- Black- 58", "HL Road Frame- Black- 62",
              "HL Road Frame- Red- 44", "HL Road Frame- Red- 48",
              "HL Road Frame- Red- 52", "HL Road Frame- Red- 56",
              "HL Road Frame- Red- 58", "HL Road Frame- Red- 62",
              "LL Road Frame- Black- 44", "LL Road Frame- Black- 48",
              "LL Road Frame- Black- 52", "LL Road Frame- Black- 58",
              "LL Road Frame- Red- 44", "LL Road Frame- Red- 48",
              "LL Road Frame- Red- 52", "LL Road Frame- Red- 58"],
    "CO_SA": ["LL Mountain Seat/Saddle", "ML Mountain Seat/Saddle", "HL Mountain Seat/Saddle",
              "LL Road Seat/Saddle", "ML Road Seat/Saddle", "HL Road Seat/Saddle",
              "Touring Seat/Saddle"],
    "CO_TF": ["HL Touring Frame- Yellow- 46", "HL Touring Frame- Yellow- 50",
              "HL Touring Frame- Yellow- 54", "HL Touring Frame- Yellow- 60",
              "HL Touring Frame- Blue- 46", "HL Touring Frame- Blue- 50",
              "LL Touring Frame- Yellow- 44", "LL Touring Frame- Yellow- 50",
              "LL Touring Frame- Yellow- 54", "LL Touring Frame- Yellow- 62",
              "LL Touring Frame- Blue- 44", "LL Touring Frame- Blue- 50"],
    "CO_WH": ["LL Mountain Wheel", "ML Mountain Wheel", "HL Mountain Wheel",
              "LL Road Wheel", "ML Road Wheel", "HL Road Wheel",
              "LL Touring Wheel", "ML Touring Wheel", "HL Touring Wheel"],
}

COUNTRIES = [
    "Australia", "Australia", "Australia", "Australia",  # weighted heavily
    "United States", "United States", "United States", "United States", "United States",
    "United Kingdom", "United Kingdom", "United Kingdom",
    "Germany", "Germany", "Germany",
    "France", "France",
    "Canada", "Canada",
    "South Africa", "South Africa",
    "Japan",
    "Brazil",
    "India",
    "China",
    "Mexico",
    "Italy",
    "Spain",
    "Netherlands",
    "Sweden",
    "South Korea",
]

# Country codes as they appear messily in the ERP source
COUNTRY_MESSY = {
    "Australia": ["Australia", "Australia", "AU"],
    "United States": ["US", "USA", "United States"],
    "United Kingdom": ["United Kingdom", "UK"],
    "Germany": ["DE", "Germany"],
    "France": ["France", "FR"],
    "Canada": ["Canada", "CA"],
    "South Africa": ["South Africa", "ZA"],
    "Japan": ["Japan", "JP"],
    "Brazil": ["Brazil", "BR"],
    "India": ["India", "IN"],
    "China": ["China", "CN"],
    "Mexico": ["Mexico", "MX"],
    "Italy": ["Italy", "IT"],
    "Spain": ["Spain", "ES"],
    "Netherlands": ["Netherlands", "NL"],
    "Sweden": ["Sweden", "SE"],
    "South Korea": ["South Korea", "KR"],
}

PRODUCT_LINES = {"BI": "R", "CO": "M", "CL": "S", "AC": "S"}

# Size/color variants to multiply product count
BIKE_SIZES = ["38", "42", "44", "46", "48", "50", "52", "54", "56", "58", "60", "62"]
FRAME_SIZES = BIKE_SIZES
CLOTHING_SIZES = ["XS", "S", "M", "L", "XL", "XXL"]
COLORS = ["Black", "Silver", "Red", "Blue", "Yellow", "White", "Green"]

# ── Price ranges by category ────────────────────────────────────────────────
PRICE_RANGE = {
    "AC": (5, 120),
    "BI": (500, 3600),
    "CL": (8, 90),
    "CO": (20, 700),
}

# ── Helpers ──────────────────────────────────────────────────────────────────

def rand_date(start: date, end: date) -> date:
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, max(delta, 0)))

def fmt_date(d: date) -> str:
    return d.strftime("%Y-%m-%d")

def int_date(d: date) -> int:
    return int(d.strftime("%Y%m%d"))

# ── Generate products (prd_info.csv) ─────────────────────────────────────────

print("Generating products...")

products = []  # (prd_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt)
product_keys_for_sales = []  # only current products (end_dt empty)
prd_id = 210

for cat_id_raw, cat_name, subcat, maint in CATEGORIES:
    prefix_parts = cat_id_raw.replace("_", "-")  # AC_HE -> AC-HE
    line_code = PRODUCT_LINES.get(cat_id_raw[:2], "")
    names = PRODUCT_NAMES.get(cat_id_raw, [f"{subcat} Standard"])
    price_lo, price_hi = PRICE_RANGE[cat_id_raw[:2]]
    cat_prefix = cat_id_raw[:2]

    for base_name in names:
        # Generate size/color variants depending on category
        if cat_prefix == "BI":
            # Bikes already have color/size in name; add 2-4 extra variants
            variants = [base_name]
            for _ in range(random.randint(2, 4)):
                sz = random.choice(BIKE_SIZES)
                cl = random.choice(COLORS)
                variants.append(f"{base_name.rsplit('-', 1)[0]}- {cl}- {sz}")
        elif cat_prefix == "CO" and "Frame" in base_name:
            variants = [base_name]
            for _ in range(random.randint(2, 5)):
                sz = random.choice(FRAME_SIZES)
                cl = random.choice(COLORS)
                variants.append(f"{base_name.rsplit('-', 1)[0]}- {cl}- {sz}")
        elif cat_prefix == "CL":
            # Clothing: expand to all sizes
            variants = [f"{base_name.rsplit('-', 1)[0]}- {sz}" for sz in random.sample(CLOTHING_SIZES, random.randint(3, 6))]
        else:
            # Accessories and other components: 1-2 color variants
            variants = [base_name]
            if random.random() < 0.5:
                cl = random.choice(COLORS)
                variants.append(f"{base_name}- {cl}")

        for name in variants:
            # Generate a product code suffix
            suffix = f"{random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ')}{random.randint(100,999)}{random.choice('BRSWY')}-{random.randint(20,70)}"
            full_prd_key = f"{prefix_parts}-{suffix}"

            # Some products have 2-3 historical versions
            num_versions = random.choices([1, 2, 3], weights=[60, 30, 10])[0]
            base_start = rand_date(date(2003, 1, 1), date(2012, 12, 31))

            for v in range(num_versions):
                version_start = base_start + timedelta(days=v * random.randint(365, 1200))
                if v < num_versions - 1:
                    version_end = version_start + timedelta(days=random.randint(300, 1100))
                else:
                    version_end = None  # Current version

                cost = random.randint(price_lo, price_hi) if random.random() > 0.05 else ""  # 5% NULL cost
                line_val = f"{line_code} " if line_code else ""  # trailing space like original

                products.append((
                    prd_id,
                    full_prd_key,
                    name,
                    cost,
                    line_val,
                    fmt_date(version_start),
                    fmt_date(version_end) if version_end else "",
                ))

                if version_end is None:
                    product_keys_for_sales.append((
                        full_prd_key.split("-", 2)[2],  # strip category prefix for sales
                        cost if cost else random.randint(price_lo, price_hi),
                    ))

                prd_id += 1

print(f"  {len(products)} product rows ({len(product_keys_for_sales)} current)")

# Write prd_info.csv
with open(os.path.join(CRM_DIR, "prd_info.csv"), "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(["prd_id", "prd_key", "prd_nm", "prd_cost", "prd_line", "prd_start_dt", "prd_end_dt"])
    w.writerows(products)

# Write PX_CAT_G1V2.csv
with open(os.path.join(ERP_DIR, "PX_CAT_G1V2.csv"), "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(["ID", "CAT", "SUBCAT", "MAINTENANCE"])
    for cat_id, cat, subcat, maint in CATEGORIES:
        w.writerow([cat_id, cat, subcat, maint])

# ── Generate customers (cust_info.csv) ───────────────────────────────────────

NUM_CUSTOMERS = 150_000

print(f"Generating {NUM_CUSTOMERS:,} customers...")

customers = []  # list of (cst_id, cst_key, first, last, marital, gender, create_date)
customer_ids = []
customer_keys = []
customer_countries = []

for i in range(NUM_CUSTOMERS):
    cst_id = 11000 + i
    cst_key = f"AW{cst_id:08d}"
    first = random.choice(FIRST_NAMES)
    last = random.choice(LAST_NAMES)

    # Introduce quirks matching original data
    marital = random.choice(["M", "S", "S", "M", ""])  # some blank
    gender = random.choice(["M", "F", "M", "F", ""])   # some blank

    # Add whitespace quirks like original (~10%)
    if random.random() < 0.05:
        first = f" {first}"
    if random.random() < 0.05:
        first = f"{first} "
    if random.random() < 0.05:
        last = f"  {last}"
    if random.random() < 0.05:
        last = f"{last}  "

    create_date = rand_date(date(2010, 1, 1), date(2026, 3, 30))
    country = random.choice(COUNTRIES)

    customers.append((cst_id, cst_key, first, last, marital, gender, fmt_date(create_date)))
    customer_ids.append(cst_id)
    customer_keys.append(cst_key)
    customer_countries.append(country)

# Add ~3% duplicate rows (same cst_id, different create_date) to simulate SCD
num_dupes = int(NUM_CUSTOMERS * 0.03)
print(f"  Adding {num_dupes:,} duplicate customer rows...")
for _ in range(num_dupes):
    idx = random.randint(0, NUM_CUSTOMERS - 1)
    orig = customers[idx]
    # Slightly altered version
    new_create = rand_date(date(2024, 1, 1), date(2026, 3, 30))
    new_marital = random.choice(["M", "S"])
    customers.append((orig[0], orig[1], orig[2], orig[3], new_marital, orig[5], fmt_date(new_create)))

# Add a handful of NULL cst_id rows (like original)
for _ in range(5):
    customers.append(("", random.choice(["13451235", "A01Ass", "BADKEY", "X999", "NULL"]), "", "", "", "", ""))

random.shuffle(customers)  # mix duplicates in

print(f"  {len(customers)} total customer rows")

with open(os.path.join(CRM_DIR, "cust_info.csv"), "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(["cst_id", "cst_key", "cst_firstname", "cst_lastname", "cst_marital_status", "cst_gndr", "cst_create_date"])
    w.writerows(customers)

# ── Generate ERP CUST_AZ12.csv (demographics) ───────────────────────────────

print("Generating ERP customer demographics...")

with open(os.path.join(ERP_DIR, "CUST_AZ12.csv"), "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(["CID", "BDATE", "GEN"])
    for i in range(NUM_CUSTOMERS):
        cst_key = customer_keys[i]
        # ~15% get NAS prefix
        if random.random() < 0.15:
            cid = f"NAS{cst_key}"
        else:
            cid = cst_key

        # Birthdate: mostly 1940-2005, ~0.5% future dates
        if random.random() < 0.005:
            bdate = rand_date(date(2027, 1, 1), date(2030, 12, 31))
        else:
            bdate = rand_date(date(1940, 1, 1), date(2005, 12, 31))

        # Gender quirks: mix of abbreviations and full words
        gen = random.choice(["Male", "Female", "M", "F", "Male", "Female", "MALE", "FEMALE", ""])

        w.writerow([cid, fmt_date(bdate), gen])

# ── Generate ERP LOC_A101.csv (locations) ────────────────────────────────────

print("Generating ERP customer locations...")

with open(os.path.join(ERP_DIR, "LOC_A101.csv"), "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(["CID", "CNTRY"])
    for i in range(NUM_CUSTOMERS):
        cst_key = customer_keys[i]
        country = customer_countries[i]

        # ~20% get hyphens inserted
        if random.random() < 0.20:
            pos = random.randint(3, len(cst_key) - 3)
            cid = cst_key[:pos] + "-" + cst_key[pos:]
        else:
            cid = cst_key

        # Messy country code
        messy_codes = COUNTRY_MESSY.get(country, [country])
        cntry = random.choice(messy_codes)

        # ~2% blank country
        if random.random() < 0.02:
            cntry = ""

        w.writerow([cid, cntry])

# ── Generate sales (sales_details.csv) ───────────────────────────────────────

NUM_SALES = 1_000_000

print(f"Generating {NUM_SALES:,} sales transactions...")

order_counter = 43697
rows_in_order = 0
max_rows_per_order = random.randint(1, 8)
current_order_num = f"SO{order_counter}"
current_cust = random.choice(customer_ids)
current_order_date = rand_date(date(2010, 12, 29), date(2026, 3, 15))

with open(os.path.join(CRM_DIR, "sales_details.csv"), "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(["sls_ord_num", "sls_prd_key", "sls_cust_id", "sls_order_dt", "sls_ship_dt", "sls_due_dt", "sls_sales", "sls_quantity", "sls_price"])

    for _ in range(NUM_SALES):
        # Start new order?
        if rows_in_order >= max_rows_per_order:
            order_counter += 1
            current_order_num = f"SO{order_counter}"
            current_cust = random.choice(customer_ids)
            current_order_date = rand_date(date(2010, 12, 29), date(2026, 3, 15))
            max_rows_per_order = random.choices([1, 2, 3, 4, 5, 6, 7, 8], weights=[25, 25, 20, 12, 8, 5, 3, 2])[0]
            rows_in_order = 0

        prd_key, unit_price = random.choice(product_keys_for_sales)
        unit_price = int(unit_price) if unit_price else random.randint(5, 2000)
        quantity = random.choices([1, 2, 3, 4, 5, 6, 8, 10], weights=[50, 25, 10, 5, 4, 3, 2, 1])[0]
        sales = quantity * unit_price

        ship_date = current_order_date + timedelta(days=random.randint(5, 14))
        due_date = current_order_date + timedelta(days=random.randint(10, 21))

        order_dt = int_date(current_order_date)
        ship_dt = int_date(ship_date)
        due_dt = int_date(due_date)

        # ~1% zero/invalid dates
        if random.random() < 0.005:
            order_dt = 0
        if random.random() < 0.005:
            ship_dt = 0
        if random.random() < 0.005:
            due_dt = 0

        # ~3% mismatched sales (corruption)
        if random.random() < 0.03:
            sales = sales + random.randint(-50, 200)
            if sales < 0:
                sales = ""  # NULL

        # ~1% NULL price
        if random.random() < 0.01:
            unit_price = ""

        # ~0.5% negative price
        if random.random() < 0.005:
            unit_price = -abs(int(unit_price)) if unit_price else -random.randint(5, 500)

        w.writerow([current_order_num, prd_key, current_cust, order_dt, ship_dt, due_dt, sales, quantity, unit_price])
        rows_in_order += 1

print("Done! Summary:")
print(f"  source_crm/cust_info.csv      — {len(customers):>10,} rows")
print(f"  source_crm/prd_info.csv       — {len(products):>10,} rows")
print(f"  source_crm/sales_details.csv  — {NUM_SALES:>10,} rows")
print(f"  source_erp/CUST_AZ12.csv      — {NUM_CUSTOMERS:>10,} rows")
print(f"  source_erp/LOC_A101.csv       — {NUM_CUSTOMERS:>10,} rows")
print(f"  source_erp/PX_CAT_G1V2.csv    — {len(CATEGORIES):>10,} rows")
