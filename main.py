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

HEADERS = {"User-Agent": "Mozilla/5.0"}

# ================= SHEET =================

def init_sheet():
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]

    creds_dict = json.loads(os.environ["GOOGLE_CREDS"])
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)

    client = gspread.authorize(creds)
    return client.open("Fuel Prices").sheet1

# ================= SCRAPER =================

def extract_price(tree):
    texts = tree.xpath('//div[contains(@class,"gd-fuel-price")]//text()')
    for t in texts:
        t = t.strip()
        if "₹" in t:
            match = re.search(r"₹\s*(\d+(?:\.\d+)?)", t)
            if match:
                return float(match.group(1))
    return None


async def fetch_price(client, url):
    try:
        resp = await client.get(url, headers=HEADERS, timeout=15)
        tree = html.fromstring(resp.text)
        return extract_price(tree)
    except:
        return None


async def scrape():
    async with httpx.AsyncClient() as client:
        result = {}

        tasks = []
        meta = []

        for city, fuels in URLS.items():
            for fuel, url in fuels.items():
                tasks.append(fetch_price(client, url))
                meta.append((city, fuel))

        responses = await asyncio.gather(*tasks)

        for i, val in enumerate(responses):
            city, fuel = meta[i]
            result.setdefault(city, {})[fuel] = val

        return result

# ================= PREVIOUS =================

def get_previous_block(sheet):
    values = sheet.get_all_values()

    fuels = ["Petrol", "Diesel", "CNG"]

    prev = {f: {} for f in fuels}

    start = None
    for i in range(1, len(values)):
        if values[i][1] in fuels:
            start = i
            break

    if start is None:
        return prev

    for i, fuel in enumerate(fuels):
        row = values[start + i]

        try:
            prev[fuel]["Delhi"] = float(row[2])
            prev[fuel]["Mumbai"] = float(row[4])
            prev[fuel]["Kolkata"] = float(row[6])
            prev[fuel]["Chennai"] = float(row[8])
        except:
            pass

    return prev

# ================= CHANGE =================

def calc_changes(current, prev):
    fuels = ["Petrol", "Diesel", "CNG"]
    cities = ["Delhi", "Mumbai", "Kolkata", "Chennai"]

    changes = {f: {} for f in fuels}

    for fuel in fuels:
        for city in cities:
            old = prev.get(fuel, {}).get(city)
            new = current.get(city, {}).get(fuel)

            if old and new:
                pct = ((new - old) / old) * 100
                changes[fuel][city] = round(pct, 2)
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

    fuels = ["Petrol", "Diesel", "CNG"]
    cities = ["Delhi", "Mumbai", "Kolkata", "Chennai"]

    date_str = datetime.now().strftime("%d %B %Y")

    new_rows = []

    for i, fuel in enumerate(fuels):
        row = [date_str if i == 0 else "", fuel]

        for city in cities:
            val = data.get(city, {}).get(fuel)
            change = changes[fuel][city]

            # format % with sign
            if change > 0:
                change_str = f"+{change}"
            elif change < 0:
                change_str = f"{change}"
            else:
                change_str = "0"

            row.append(val)
            row.append(change_str)

        new_rows.append(row)

    separator = [""] * len(header)
    final = [header] + new_rows + [separator] + values[1:]

    sheet.update("A1", final)

    # ================= FORMATTING =================

    total_rows = len(final)

    # 🔴 old data background
    format_cell_range(
        sheet,
        f"A5:J{total_rows}",
        CellFormat(backgroundColor=Color(1, 0.8, 0.8))
    )

    # 🔥 color % cells
    for r in range(2, 5):
        for c in [4, 6, 8, 10]:  # % columns
            cell = sheet.cell(r, c).value

            try:
                val = float(cell.replace("+", ""))
                if val > 0:
                    color = Color(1, 0, 0)  # red
                elif val < 0:
                    color = Color(0, 0.6, 0)  # green
                else:
                    continue

                format_cell_range(
                    sheet,
                    gspread.utils.rowcol_to_a1(r, c),
                    CellFormat(textFormat=TextFormat(foregroundColor=color))
                )
            except:
                pass

# ================= MAIN =================

async def main():
    sheet = init_sheet()

    current = await scrape()
    prev = get_previous_block(sheet)
    changes = calc_changes(current, prev)

    update_sheet(sheet, current, changes)

    print("✅ Done")

if __name__ == "__main__":
    asyncio.run(main())
