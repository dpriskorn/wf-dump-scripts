# ./models/z8_calculator.py
import re
from datetime import datetime
from typing import List, Dict, Union
import json
import os
import logging

from pydantic import BaseModel, Field
from models.exceptions import DateError
from models.wf.client import Client
from models.wf.zfunction import Zfunction
from models.wf.zimpl import Zimpl
from models.wf.ztester import Ztester
import config

logger = logging.getLogger(__name__)


class Z8Calculator(BaseModel):
    # --- Input / Configuration ---
    jsonl_file: str = Field(
        ..., description="Path to the input JSONL file (only one file at a time)."
    )
    output_file_prefix: str = Field(
        default="output/wikitable-z8-stats", description="Prefix for output files."
    )
    progress_interval: int = Field(
        default=1000,
        description="Number of ZFunctions processed before reporting progress.",
    )

    # --- Attributes to hold intermediate data ---
    ztester_map: Dict[str, Ztester] = Field(
        default_factory=dict, description="Mapping from ZTester IDs to ZTester objects."
    )
    zimpl_map: Dict[str, Zimpl] = Field(
        default_factory=dict,
        description="Mapping from ZImplementation IDs to ZImpl objects.",
    )
    zfunctions: List[Zfunction] = Field(
        default_factory=list,
        description="List of ZFunction objects collected from the input file.",
    )
    table: List[Dict[str, Union[str, int]]] = Field(
        default_factory=list, description="Intermediate table storing processed stats."
    )
    last_update: str = Field(default="", description="Timestamp of the last update.")

    class Config:
        arbitrary_types_allowed = True

    def build_map(
            self,
            cls: type,
            target_map: Dict[str, BaseModel],
            check_attr: str,
            description: str
    ) -> None:
        """
        Generic method to populate a map with ZID -> object of type cls.

        Args:
            cls: The class to instantiate (Ztester or Zimpl).
            target_map: The dictionary to populate.
            check_attr: Attribute name to check (e.g., 'is_tester' or 'is_implementation').
            description: Text description for logging.
        """
        processed = 0
        logging.info(f"Building {description} from {self.jsonl_file}...")

        with open(self.jsonl_file, "r", encoding="utf-8") as f:
            for line in f:
                processed += 1
                if processed % self.progress_interval == 0:
                    logging.info(f"Processed {processed} lines for {description}...")

                try:
                    data = json.loads(line)
                except json.JSONDecodeError:
                    continue

                obj = cls(data=data)
                if getattr(obj, check_attr, False):
                    zid = getattr(obj, "zid", None)
                    if zid and zid not in target_map:
                        target_map[zid] = obj

        logging.info(f"{description} built with {len(target_map)} entries.")

    # Then replace the original methods with:

    def build_ztester_map(self) -> None:
        self.build_map(Ztester, self.ztester_map, "is_tester", "tester map")

    def build_zimpl_map(self) -> None:
        self.build_map(Zimpl, self.zimpl_map, "is_implementation", "implementation map")

    async def calculate(self) -> None:
        """Compute ZFunctions and connected implementations; populate self.zfunctions and self.table."""
        self.zfunctions = []
        zid_count = 0

        # Build maps
        self.build_tester_map()
        self.build_zimpl_map()

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
                    func.extract_ztesters(self.ztester_map)
                    func.extract_zimpl(self.ztester_map)
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
        output_file_1 = self.output_file_prefix + "-1-9999.txt"
        self._write_zids_file(output_file_1, min_zid=1, max_zid=9999)

        # File for ZID>=10000
        output_file_2 = self.output_file_prefix + "-10000+.txt"
        self._write_zids_file(output_file_2, min_zid=10000)

    def _write_zids_file(
        self, filename: str, min_zid: int = 1, max_zid: int = None
    ) -> None:
        """Write a wikitext table for a given ZID range to a specific file."""
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, "w", encoding="utf-8") as f:
            # Write header
            f.write(
                "; Note: Disconnected tests/implementations are not in presently in the dump\n\n"
                f"Last update: {self.last_update}\n"
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
