import asyncio
import httpx
import re
import random
import os
import json
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

    creds_json = os.environ.get("GOOGLE_CREDS")
    if not creds_json:
        raise Exception("GOOGLE_CREDS not found in environment variables")

    creds_dict = json.loads(creds_json)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)

    client = gspread.authorize(creds)
    return client.open("Fuel Prices").sheet1

# ================= EXTRACTION =================

def extract_price(tree, fuel):
    nodes = tree.xpath('//h2[@id="BC_lblCurrent"]')

    for node in nodes:
        text = "".join(node.xpath(".//text()")).lower()
        if fuel.lower() in text:
            match = re.search(r"₹\s*(\d+\.\d+)", text)
            if match:
                return float(match.group(1))

    fallback = tree.xpath('//div[@class="UCBottomHalf"]//div[@class="fnt27"]/text()')
    if fallback:
        match = re.search(r"₹\s*(\d+\.?\d*)", fallback[0])
        if match:
            return float(match.group(1))

    return None

# ================= FETCH =================

async def fetch_price(client, url, fuel, semaphore):
    async with semaphore:
        await asyncio.sleep(random.uniform(1, 3))

        headers = BASE_HEADERS.copy()
        headers["User-Agent"] = random.choice(USER_AGENTS)

        for attempt in range(3):
            try:
                resp = await client.get(url, headers=headers, timeout=10)

                if resp.status_code == 403:
                    raise Exception("Blocked (403)")

                resp.raise_for_status()

                tree = html.fromstring(resp.text)
                return extract_price(tree, fuel)

            except Exception as e:
                print(f"Retry {attempt+1} failed for {fuel}: {e}")
                if attempt == 2:
                    return None
                await asyncio.sleep(2 ** attempt)

# ================= SCRAPER =================

async def scrape():
    semaphore = asyncio.Semaphore(3)

    async with httpx.AsyncClient(follow_redirects=True) as client:
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

        return all_success

# ================= PREVIOUS =================

def get_previous_block(sheet):
    values = sheet.get_all_values()
    fuels = ["Petrol", "Diesel", "CNG"]
    cities = ["Delhi", "Mumbai", "Kolkata", "Chennai"]

    prev = {}
    rows = values[1:4] if len(values) >= 4 else []

    for i, fuel in enumerate(fuels):
        prev[fuel] = {}
        for idx, city in enumerate(cities):
            try:
                prev[fuel][city] = float(rows[i][2 + idx])
            except:
                prev[fuel][city] = None

    return prev

# ================= CHANGE =================

def calc_changes(current, prev):
    fuels = ["Petrol", "Diesel", "CNG"]
    cities = ["Delhi", "Mumbai", "Kolkata", "Chennai"]

    changes = {fuel: {} for fuel in fuels}

    for fuel in fuels:
        for city in cities:
            old = prev.get(fuel, {}).get(city)
            new = current.get(city, {}).get(fuel)

            if old and new:
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
    cities = ["Delhi", "Mumbai", "Kolkata", "Chennai"]

    new_rows = []

    for i, fuel in enumerate(fuels):
        row = [
            date_str if i == 0 else "",
            fuel,
        ]

        for city in cities:
            value = data.get(city, {}).get(fuel)
            row.append(value if value is not None else "N/A")

        row += [
            fuel + " %",
            changes[fuel]["Delhi"],
            changes[fuel]["Mumbai"],
            changes[fuel]["Kolkata"],
            changes[fuel]["Chennai"],
        ]

        new_rows.append(row)

    separator = [""] * 11
    final = [header] + new_rows + [separator] + values[1:]

    sheet.update("A1", final)

# ================= MAIN =================

async def main():
    print("Starting scraper...")

    sheet = init_sheet()

    current = await scrape()
    print("Scraped data:", current)

    prev = get_previous_block(sheet)
    changes = calc_changes(current, prev)

    update_sheet(sheet, current, changes)

    print("✅ Done")

if __name__ == "__main__":
    asyncio.run(main())
