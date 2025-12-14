import asyncio
import json
import os
import logging
import re
from typing import List, Dict, Union

from pydantic import ValidationError

import config
from models.zid_item import ZIDItem
from models.client import Client

# --- Configuration ---
logging.basicConfig(level=config.loglevel, format=config.logformat)
input_dir = "output"
output_file = os.path.join("output", "wikitable-z8-stats.txt")
jsonl_files = [os.path.join(input_dir, f) for f in os.listdir(input_dir) if f.endswith(".jsonl")]
zid_pattern = re.compile(r"^Z\d+$")
progress_interval = 1000


async def main():
    # --- Collect ZIDItems ---
    items: List[ZIDItem] = []
    zid_count = 0

    for jsonl_file in jsonl_files:
        logging.info(f"Processing {jsonl_file}...")
        processed = 0
        with open(jsonl_file, "r", encoding="utf-8") as f:
            for line in f:
                processed += 1
                if processed % progress_interval == 0:
                    logging.info(f"Processed {processed} lines, {zid_count} ZIDs so far...")

                try:
                    raw = json.loads(line)
                    item = ZIDItem.model_validate(raw)
                except (ValidationError, json.JSONDecodeError) as e:
                    logging.warning(f"Skipping invalid line at {processed}: {e}")
                    continue

                if not zid_pattern.match(item.title):
                    continue

                if item.is_function:
                    items.append(item)
                    zid_count += 1
                    # debug break out at low number
                    if zid_count >105:
                        break

    logging.info(f"Collected {zid_count} ZFunctions.")

    # --- Fetch connected implementations in bulk ---
    client = Client(concurrency=8)
    try:
        zid_map = await client.bulk_fetch_connected_implementations(
            item.zid for item in items
        )
    finally:
        await client.close()

    for item in items:
        item.apply_connected_implementations(zid_map.get(item.zid, []))

    # --- Prepare table ---
    table: List[Dict[str, Union[str, int]]] = []
    for item in items:
        table.append({
            "FunctionID": item.title,
            "Aliases": item.count_aliases,
            "Implementations": item.count_implementations,
            "Connected": item.number_of_connected_implementations,
            "Disconnected": item.count_implementations - item.number_of_connected_implementations,
            "Tests": item.count_testers,
            "Languages": item.count_languages,
        })

    logging.info(f"Finished processing {len(items)} ZFunctions.")

    # --- Write wikitext table ---
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, "w", encoding="utf-8") as f:
        f.write("{| class=\"wikitable sortable\"\n")
        f.write("! FunctionID !! Aliases !! Implementations !! Connected !! Disconnected !! Tests !! Translations\n")
        for row in table:
            f.write(
                f"|-\n| [[{row['FunctionID']}]] || {row['Aliases']} || {row['Implementations']} || "
                f"{row['Connected']} || {row['Disconnected']} || {row['Tests']} || {row['Languages']}\n"
            )
        f.write("|}\n")

    logging.info(f"Wikitext table written to {output_file}")


if __name__ == "__main__":
    asyncio.run(main())
