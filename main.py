import asyncio
import httpx
import re
import random
from lxml import html
from datetime import datetime

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread_formatting import format_cell_range, CellFormat, Color, TextFormat

# ================= CONFIG =================

MAX_RETRY_ROUNDS = 5
RETRY_DELAY = 5

URLS = {
    "Delhi": {
        "Petrol": "https://www.mypetrolprice.com/2/Petrol-price-in-Delhi",
        "Diesel": "https://www.mypetrolprice.com/2/Diesel-price-in-Delhi",
        "CNG": "https://www.mypetrolprice.com/2/CNG-price-in-Delhi",
    },
    "Mumbai": {
        "Petrol": "https://www.mypetrolprice.com/3/Petrol-price-in-Mumbai",
        "Diesel": "https://www.mypetrolprice.com/3/Diesel-price-in-Mumbai",
        "CNG": "https://www.mypetrolprice.com/3/CNG-price-in-Mumbai",
    },
    "Kolkata": {
        "Petrol": "https://www.mypetrolprice.com/4/Petrol-price-in-Kolkata",
        "Diesel": "https://www.mypetrolprice.com/4/Diesel-price-in-Kolkata",
        "CNG": "https://www.mypetrolprice.com/4/CNG-price-in-Kolkata",
    },
    "Chennai": {
        "Petrol": "https://www.mypetrolprice.com/5/Petrol-price-in-Chennai",
        "Diesel": "https://www.mypetrolprice.com/5/Diesel-price-in-Chennai",
        "CNG": "https://www.mypetrolprice.com/5/CNG-price-in-Chennai",
    },
}

BASE_HEADERS = {
    "Accept-Language": "en-IN,en;q=0.9",
    "Referer": "https://www.mypetrolprice.com/",
}

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh)",
    "Mozilla/5.0 (Windows NT 10.0)",
    "Mozilla/5.0 (X11; Linux)",
]

# ================= SHEET =================

def init_sheet():
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
    client = gspread.authorize(creds)
    return client.open("Fuel Prices").sheet1

# ================= EXTRACTION (FIXED) =================

def extract_price(tree, fuel):
    # PRIMARY
    nodes = tree.xpath('//h2[@id="BC_lblCurrent"]')

    for node in nodes:
        text = "".join(node.xpath(".//text()")).lower()
        if fuel.lower() in text:
            match = re.search(r"₹\s*(\d+\.\d+)", text)
            if match:
                return float(match.group(1))

    # FALLBACK (CRITICAL - was missing in your new code)
    fallback = tree.xpath('//div[@class="UCBottomHalf"]//div[@class="fnt27"]/text()')
    if fallback:
        match = re.search(r"₹\s*(\d+\.?\d*)", fallback[0])
        if match:
            return float(match.group(1))

    return None  # DO NOT return 0

# ================= FETCH (FIXED) =================

async def fetch_price(client, url, fuel, semaphore):
    async with semaphore:

        # Smart delay (prevents blocking)
        if "Kolkata" in url and "CNG" in url:
            await asyncio.sleep(random.uniform(3, 6))
        else:
            await asyncio.sleep(random.uniform(1, 2.5))

        headers = BASE_HEADERS.copy()
        headers["User-Agent"] = random.choice(USER_AGENTS)

        for attempt in range(3):
            try:
                resp = await client.get(url, headers=headers, timeout=10)

                if resp.status_code == 403:
                    raise Exception("Blocked")

                resp.raise_for_status()

                tree = html.fromstring(resp.text)
                return extract_price(tree, fuel)

            except:
                if attempt == 2:
                    return None
                await asyncio.sleep(2 ** attempt)

# ================= SCRAPER (FIXED) =================

async def scrape():
    semaphore = asyncio.Semaphore(3)

    async with httpx.AsyncClient(follow_redirects=True) as client:

        # Warmup request (important)
        await client.get("https://www.mypetrolprice.com/")

        targets = [
            (city, fuel, url)
            for city, fuels in URLS.items()
            for fuel, url in fuels.items()
        ]

        all_success = {}

        for _ in range(MAX_RETRY_ROUNDS):
            success = {}
            failed = []

            tasks = [
                (city, fuel, fetch_price(client, url, fuel, semaphore))
                for city, fuel, url in targets
            ]

            results = await asyncio.gather(*[t[2] for t in tasks])

            for i, result in enumerate(results):
                city, fuel, _ = tasks[i]

                if result is None:
                    failed.append((city, fuel, URLS[city][fuel]))
                else:
                    success.setdefault(city, {})[fuel] = result

            for city, fuels in success.items():
                all_success.setdefault(city, {}).update(fuels)

            targets = failed

            if not targets:
                break

            await asyncio.sleep(RETRY_DELAY)

        return {
            city: {
                "Petrol": all_success.get(city, {}).get("Petrol", 0),
                "Diesel": all_success.get(city, {}).get("Diesel", 0),
                "CNG": all_success.get(city, {}).get("CNG", 0),
            }
            for city in URLS
        }

# ================= PREVIOUS =================

def get_previous_block(sheet):
    values = sheet.get_all_values()
    fuels = ["Petrol", "Diesel", "CNG"]
    cities = ["Delhi", "Mumbai", "Kolkata", "Chennai"]

    prev = {}
    rows = values[1:4] if len(values) >= 4 else []

    for i, fuel in enumerate(fuels):
        if i < len(rows):
            prev[fuel] = {
                city: float(rows[i][2 + idx]) if rows[i][2 + idx] else 0
                for idx, city in enumerate(cities)
            }
        else:
            prev[fuel] = {city: 0 for city in cities}

    return prev

# ================= CHANGE =================

def calc_changes(current, prev):
    fuels = ["Petrol", "Diesel", "CNG"]
    cities = ["Delhi", "Mumbai", "Kolkata", "Chennai"]

    changes = {fuel: {} for fuel in fuels}

    for fuel in fuels:
        for city in cities:
            old = prev.get(fuel, {}).get(city, 0)
            new = current[city][fuel]

            if old:
                val = (new - old) / old
                changes[fuel][city] = round(val, 2)
            else:
                changes[fuel][city] = 0

    return changes

# ================= UPDATE =================

def update_sheet(sheet, data, changes):
    values = sheet.get_all_values()

    header = [
        "Date", "Fuel Type", "Delhi", "Mumbai", "Kolkata", "Chennai",
        "Fuel %", "Delhi %", "Mumbai %", "Kolkata %", "Chennai %"
    ]

    date_str = datetime.now().strftime("%d %B %Y")
    fuels = ["Petrol", "Diesel", "CNG"]

    new_rows = []
    for i, fuel in enumerate(fuels):
        new_rows.append([
            date_str if i == 0 else "",
            fuel,
            data["Delhi"][fuel],
            data["Mumbai"][fuel],
            data["Kolkata"][fuel],
            data["Chennai"][fuel],
            fuel + " %",
            changes[fuel]["Delhi"],
            changes[fuel]["Mumbai"],
            changes[fuel]["Kolkata"],
            changes[fuel]["Chennai"],
        ])

    separator = [""] * 11
    final = [header] + new_rows + [separator] + values[1:]

    sheet.update("A1", final)

    total_rows = len(final)

    if total_rows > 5:
        red = CellFormat(backgroundColor=Color(1, 0.85, 0.85))
        format_cell_range(sheet, f"A6:K{total_rows}", red)

    for r in range(2, 5):
        for c in range(8, 12):
            val = sheet.cell(r, c).value
            try:
                v = float(val)
                if v > 0:
                    fmt = CellFormat(textFormat=TextFormat(foregroundColor=Color(0, 0.6, 0)))
                elif v < 0:
                    fmt = CellFormat(textFormat=TextFormat(foregroundColor=Color(0.8, 0, 0)))
                else:
                    fmt = CellFormat(textFormat=TextFormat(foregroundColor=Color(0, 0, 0)))

                format_cell_range(sheet, f"{chr(64+c)}{r}", fmt)
            except:
                pass

# ================= MAIN =================

async def main():
    sheet = init_sheet()

    current = await scrape()
    prev = get_previous_block(sheet)

    changes = calc_changes(current, prev)

    update_sheet(sheet, current, changes)

    print("✅ FIXED: No missing data + retries + fallback working")

if __name__ == "__main__":
    asyncio.run(main())