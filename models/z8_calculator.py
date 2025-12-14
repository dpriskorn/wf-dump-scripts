# ./models/z8_calculator.py
from pydantic import BaseModel
from typing import List, Dict, Union
import json
import os
import logging

from models.wf.client import Client
from models.wf.zfunction import Zfunction
from models.wf.ztester import Ztester
import config

logger = logging.getLogger(__name__)


class Z8Calculator(BaseModel):
    jsonl_file: str  # only one file at a time
    output_file: str = "output/wikitable-z8-stats.txt"
    progress_interval: int = 1000

    class Config:
        arbitrary_types_allowed = True

    def build_tester_map(self) -> Dict[str, Ztester]:
        """
        Iterate once over the JSONL file and instantiate all Ztester objects.
        Returns a dict: {ZID -> Ztester}
        """
        tester_map: Dict[str, Ztester] = {}
        logging.info(f"Building tester map from {self.jsonl_file}...")
        processed = 0

        with open(self.jsonl_file, "r", encoding="utf-8") as f:
            for line in f:
                processed += 1
                if processed % self.progress_interval == 0:
                    logging.info(f"Processed {processed} lines for tester map...")

                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    continue

                zid = obj.get("Z1K1")
                if not zid:
                    continue

                # Only include testers (Z13 type) or whatever your logic requires
                if zid not in tester_map and zid.startswith("Z"):
                    tester_map[zid] = Ztester(data=obj)

        logging.info(f"Tester map built with {len(tester_map)} entries.")
        return tester_map

    async def calculate(self):
        zfunctions: List[Zfunction] = []
        zid_count = 0

        # First, build tester map once
        tester_map = self.build_tester_map()

        logging.info(f"Processing functions from {self.jsonl_file}...")
        processed = 0
        with open(self.jsonl_file, "r", encoding="utf-8") as f:
            for line in f:
                processed += 1
                if processed % self.progress_interval == 0:
                    logging.info(
                        f"Processed {processed} lines, {zid_count} ZFunctions so far..."
                    )

                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    logging.warning(f"Skipping invalid line at {processed}")
                    continue

                func = Zfunction(data=obj)
                if func.is_function:
                    func.populate()
                    func.extract_testers(tester_map)  # attach testers from map
                    zfunctions.append(func)
                    zid_count += 1

                    # --- Stop after MAX_FUNCTIONS for testing ---
                    if zid_count >= config.MAX_FUNCTIONS:
                        logging.info(
                            f"Reached MAX_FUNCTIONS={config.MAX_FUNCTIONS}, stopping early for testing."
                        )
                        break

        logging.info(f"Collected {zid_count} ZFunctions.")

        # Fetch connected implementations using async context manager
        async with Client(concurrency=8) as client:
            zid_map = await client.bulk_fetch_connected_implementations(
                item.zid for item in zfunctions
            )

        for zfunction in zfunctions:
            zfunction.apply_connected_implementations(zid_map.get(zfunction.zid, []))

        # Build wikitext table
        table: List[Dict[str, Union[str, int]]] = []
        for zfunction in zfunctions:
            table.append(
                {
                    "FunctionID": zfunction.zid,
                    "Aliases": zfunction.count_aliases,
                    "Implementations": "? (not in the dump)",
                    "Connected": zfunction.number_of_connected_implementations,
                    "Disconnected": "? (not in the dump)",
                    "Tests": zfunction.count_testers,
                    "Languages": zfunction.count_languages,
                }
            )

        os.makedirs(os.path.dirname(self.output_file), exist_ok=True)
        with open(self.output_file, "w", encoding="utf-8") as f:
            f.write('{| class="wikitable sortable"\n')
            f.write(
                "! FunctionID !! Aliases !! Implementations !! Connected impl !! Disconnected impl !! Tests !! Translations\n"
            )
            for row in table:
                f.write(
                    f"|-\n| [[{row['FunctionID']}]] || {row['Aliases']} || {row['Implementations']} || "
                    f"{row['Connected']} || {row['Disconnected']} || {row['Tests']} || {row['Languages']}\n"
                )
            f.write("|}\n")

        logging.info(f"Wikitext table written to {self.output_file}")
