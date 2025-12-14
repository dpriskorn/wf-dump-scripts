# ./models/z8_calculator.py
import re
from datetime import datetime
from typing import List, Dict, Union
import json
import os
import logging

from pydantic import BaseModel
from models.exceptions import DateError
from models.wf.client import Client
from models.wf.zfunction import Zfunction
from models.wf.ztester import Ztester
import config

logger = logging.getLogger(__name__)


class Z8Calculator(BaseModel):
    jsonl_file: str  # only one file at a time
    output_file: str = "output/wikitable-z8-stats.txt"
    progress_interval: int = 1000

    # --- Attributes to hold intermediate data ---
    tester_map: Dict[str, Ztester] = {}
    zfunctions: List[Zfunction] = []
    table: List[Dict[str, Union[str, int]]] = []
    last_update: str = ""

    class Config:
        arbitrary_types_allowed = True

    def build_tester_map(self) -> None:
        """Populate self.tester_map with ZID -> Ztester objects."""
        self.tester_map = {}
        processed = 0
        logging.info(f"Building tester map from {self.jsonl_file}...")

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
                if zid and zid.startswith("Z") and zid not in self.tester_map:
                    self.tester_map[zid] = Ztester(data=obj)

        logging.info(f"Tester map built with {len(self.tester_map)} entries.")

    async def calculate(self) -> None:
        """Compute ZFunctions and connected implementations; populate self.zfunctions and self.table."""
        self.zfunctions = []
        zid_count = 0

        # Build tester map
        self.build_tester_map()

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
                    func.extract_testers(self.tester_map)
                    self.zfunctions.append(func)
                    zid_count += 1

                    if zid_count >= config.MAX_FUNCTIONS:
                        logging.info(
                            f"Reached MAX_FUNCTIONS={config.MAX_FUNCTIONS}, stopping early."
                        )
                        break

        logging.info(f"Collected {zid_count} ZFunctions.")

        # Fetch connected implementations
        async with Client(concurrency=8) as client:
            zid_map = await client.bulk_fetch_connected_implementations(
                item.zid for item in self.zfunctions
            )

        for zfunction in self.zfunctions:
            zfunction.apply_connected_implementations(zid_map.get(zfunction.zid, []))

        # Build table
        self.table = [
            {
                "FunctionID": z.zid,
                "Aliases": z.count_aliases,
                "Implementations": z.number_of_connected_implementations,
                "Tests": z.count_testers,
                "Languages": z.count_languages,
            }
            for z in self.zfunctions
        ]

        # Extract date from filename
        basename = os.path.basename(self.jsonl_file)
        match = re.search(r"(\d{8})", basename)
        if match:
            date_str = match.group(1)
            try:
                self.last_update = datetime.strptime(date_str, "%Y%m%d").strftime(
                    "%Y-%m-%d"
                )
            except ValueError:
                raise DateError()
        else:
            raise DateError()

    def write_wikitext(self) -> None:
        """Wrapper: write two separate wikitext files based on ZID ranges."""
        # File for ZID1-9999
        output_file_1 = os.path.splitext(self.output_file)[0] + "-1-9999.txt"
        self._write_zids_file(output_file_1, min_zid=1, max_zid=9999)

        # File for ZID>=10000
        output_file_2 = os.path.splitext(self.output_file)[0] + "-10000+.txt"
        self._write_zids_file(output_file_2, min_zid=10000)

    def _write_zids_file(self, filename: str, min_zid: int = 1, max_zid: int = None) -> None:
        """Write a wikitext table for a given ZID range to a specific file."""
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, "w", encoding="utf-8") as f:
            # Write header
            f.write(
                '; Note: Disconnected tests/implementations are not in presently in the dump\n\n'
                f'Last update: {self.last_update}\n'
                '{| class="wikitable sortable"\n'
                "! rowspan='2' | Function \n"
                "! rowspan='2' | Aliases \n"
                "! colspan='2' | Connected \n"
                "! rowspan='2' | Translations\n"
                "|-\n"
                "! Implementations \n"
                "! Tests\n"
            )

            # Write rows in the given range
            for row in self.table:
                try:
                    zid_number = int(re.sub(r"^Z", "", row["FunctionID"]))
                except ValueError:
                    continue

                if (min_zid is not None and zid_number < min_zid) or (
                    max_zid is not None and zid_number > max_zid
                ):
                    continue

                f.write(
                    f"|-\n| [[{row['FunctionID']}]] || {row['Aliases']} || "
                    f"{row['Implementations']} || {row['Tests']} || {row['Languages']}\n"
                )

            f.write("|}\n")

        logging.info(f"Wikitext table written to {filename}")