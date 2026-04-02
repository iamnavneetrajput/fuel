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

# ================= CONFIG =================

MAX_RETRY_ROUNDS = 5
RETRY_DELAY = 5

URLS = {
    "Delhi": {
        "Petrol": "https://www.goodreturns.in/petrol-price-in-new-delhi.html",
        "Diesel": "https://www.goodreturns.in/diesel-price-in-new-delhi.html",
        "CNG": "https://www.goodreturns.in/cng-price-in-new-delhi.html",
    },
    "Mumbai": {
        "Petrol": "https://www.goodreturns.in/petrol-price-in-mumbai.html",
        "Diesel": "https://www.goodreturns.in/diesel-price-in-mumbai.html",
        "CNG": "https://www.goodreturns.in/cng-price-in-mumbai.html",
    },
    "Kolkata": {
        "Petrol": "https://www.goodreturns.in/petrol-price-in-kolkata.html",
        "Diesel": "https://www.goodreturns.in/diesel-price-in-kolkata.html",
        "CNG": "https://www.goodreturns.in/cng-price-in-kolkata.html",
    },
    "Chennai": {
        "Petrol": "https://www.goodreturns.in/petrol-price-in-chennai.html",
        "Diesel": "https://www.goodreturns.in/diesel-price-in-chennai.html",
        "CNG": "https://www.goodreturns.in/cng-price-in-chennai.html",
    },
}

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "text/html",
}

# ================= SHEET =================

def init_sheet():
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]

    creds_json = os.environ.get("GOOGLE_CREDS")
    creds_dict = json.loads(creds_json)

    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)

    return client.open("Fuel Prices").sheet1

# ================= EXTRACTION =================

def extract_price(tree):
    texts = tree.xpath('//div[contains(@class,"gd-fuel-price")]//text()')

    for t in texts:
        t = t.strip()
        if "₹" in t:
            match = re.search(r"₹\s*(\d+(?:\.\d+)?)", t)
            if match:
                return float(match.group(1))
    return None

# ================= FETCH =================

async def fetch_price(client, url, semaphore):
    async with semaphore:
        await asyncio.sleep(random.uniform(2, 4))

        for attempt in range(4):
            try:
                resp = await client.get(url, headers=HEADERS, timeout=15)
                resp.raise_for_status()

                tree = html.fromstring(resp.text)
                return extract_price(tree)

            except:
                await asyncio.sleep(2 * (attempt + 1))

        return None

# ================= SCRAPER =================

async def scrape():
    semaphore = asyncio.Semaphore(2)

    async with httpx.AsyncClient() as client:
        targets = [
            (city, fuel, url)
            for city, fuels in URLS.items()
            for fuel, url in fuels.items()
        ]

        result = {}

        tasks = [
            (city, fuel, fetch_price(client, url, semaphore))
            for city, fuel, url in targets
        ]

        responses = await asyncio.gather(*[t[2] for t in tasks])

        for i, res in enumerate(responses):
            city, fuel, _ = tasks[i]
            result.setdefault(city, {})[fuel] = res

        return result

# ================= PREVIOUS DATA =================

def get_previous_block(sheet):
    values = sheet.get_all_values()

    fuels = ["Petrol", "Diesel", "CNG"]
    cities = ["Delhi", "Mumbai", "Kolkata", "Chennai"]

    prev = {fuel: {} for fuel in fuels}

    # Skip header + current block (3 rows) + separator
    start_row = None

    for i in range(1, len(values)):
        if values[i][1] in fuels:
            start_row = i
            break

    if start_row is None:
        return prev

    for i, fuel in enumerate(fuels):
        row = values[start_row + i]

        try:
            prev[fuel]["Delhi"] = float(row[2])
            prev[fuel]["Mumbai"] = float(row[4])
            prev[fuel]["Kolkata"] = float(row[6])
            prev[fuel]["Chennai"] = float(row[8])
        except:
            for city in cities:
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

            if old is not None and new is not None and old != 0:
                changes[fuel][city] = (new - old) / old
            else:
                changes[fuel][city] = 0

    return changes

# ================= UPDATE =================

def update_sheet(sheet, data, changes):
    values = sheet.get_all_values()

    header = [
        "Date", "Fuel Type",
        "Delhi", "Delhi %",
        "Mumbai", "Mumbai %",
        "Kolkata", "Kolkata %",
        "Chennai", "Chennai %"
    ]

    date_str = datetime.now().strftime("%d %B %Y")
    fuels = ["Petrol", "Diesel", "CNG"]
    cities = ["Delhi", "Mumbai", "Kolkata", "Chennai"]

    new_rows = []

    for i, fuel in enumerate(fuels):
        row = [date_str if i == 0 else "", fuel]

        for city in cities:
            value = data.get(city, {}).get(fuel)
            change = changes[fuel][city]

            row.append(value if value else "N/A")
            row.append(change)

        new_rows.append(row)

    separator = [""] * len(header)

    final = [header] + new_rows + [separator] + values[1:]

    sheet.update("A1", final)

# ================= MAIN =================

async def main():
    print("Starting...")

    sheet = init_sheet()

    current = await scrape()
    print("Data:", current)

    prev = get_previous_block(sheet)
    changes = calc_changes(current, prev)

    update_sheet(sheet, current, changes)

    print("✅ Done")

if __name__ == "__main__":
    asyncio.run(main())
