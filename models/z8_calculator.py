# ./models/z8_calculator.py
import asyncio
import json
import logging
import os
import re
from datetime import datetime
from pathlib import Path
from typing import List, Dict

from pydantic import BaseModel, Field

import config
from models.exceptions import DateError, MissingData
from models.wf.client import Client
from models.wf.enums import TestStatus
from models.wf.zfunction import Zfunction
from models.wf.zimpl import Zimpl
from models.wf.ztester import Ztester

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
    test_status_map: Dict[str, Dict[str, TestStatus]] = Field(
        default_factory=dict, description="implementation_zid -> tester_zid -> status"
    )
    last_update: str = Field(default="", description="Timestamp of the last update.")

    class Config:
        arbitrary_types_allowed = True

    def build_map(
        self,
        cls: type,
        target_map: Dict[str, BaseModel],
        description: str,
    ) -> None:
        """
        Generic method to populate a map with ZID -> object of type cls.

        Args:
            cls: The class to instantiate (Ztester or Zimpl).
            target_map: The dictionary to populate.
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
                if obj.is_correct_type:
                    zid = obj.zid
                    if zid and zid not in target_map:
                        target_map[zid] = obj

        length = len(target_map)
        logging.info(f"{description} built with {length} entries.")
        if length == 0:
            raise MissingData(
                f"{description} should have at least 3k entries but it does not have a single one!"
            )
        if length < 3000:
            raise MissingData(
                f"{description} should have at least 3k entries but does not"
            )
        if config.loglevel == logging.DEBUG:
            # --- Write map to disk for debugging ---
            debug_dir = Path("debug_maps")
            debug_dir.mkdir(exist_ok=True)
            debug_file = debug_dir / f"{description.replace(' ', '_')}.json"

            # Convert BaseModel objects to dicts for JSON serialization
            serializable_map = {
                zid: obj.model_dump() for zid, obj in target_map.items()
            }
            with open(debug_file, "w", encoding="utf-8") as out_f:
                json.dump(serializable_map, out_f, indent=2)

            logging.info(f"{description} written to {debug_file}")

    def build_ztester_map(self) -> None:
        self.build_map(Ztester, self.ztester_map, "tester map")

    def build_zimpl_map(self) -> None:
        self.build_map(Zimpl, self.zimpl_map, "implementation map")

    def init_test_status_map(self) -> None:
        for zf in self.zfunctions:
            for impl in zf.zimplementations:
                self.test_status_map.setdefault(impl.zid, {})

    def apply_test_status_map(self) -> None:
        for zf in self.zfunctions:
            for impl in zf.zimplementations:
                if hasattr(impl, "test_results") and impl.test_results:
                    self.test_status_map[impl.zid] = impl.test_results

    async def fetch_zfunction_test_status(self, client: Client, zf: Zfunction) -> None:
        """Fetch test statuses for a single ZFunction and update the map."""
        status_map = await client.fetch_function_test_status_map(zf)
        for impl_zid, tests in status_map.items():
            self.test_status_map.setdefault(impl_zid, {}).update(tests)

    async def fetch_zfunction_statuses(
        self,
        client: Client,
        semaphore: asyncio.Semaphore,
        zf: Zfunction,
        processed: int,
        total_tests: int,
    ) -> None:
        """Fetch all test statuses for a single ZFunction."""
        for impl in zf.zimplementations:
            for tester in zf.ztesters:
                await self.fetch_single_test_status(client, semaphore, zf, impl, tester)
                processed += 1
                if processed % self.progress_interval == 0 or processed == total_tests:
                    logging.info(
                        "Fetched %d/%d tests (latest ZFunction: %s)",
                        processed,
                        total_tests,
                        zf.zid,
                    )

    async def fetch_all_test_statuses(self) -> None:
        """Fetch all test statuses concurrently with progress logging."""
        async with Client(concurrency=8) as client:
            semaphore = asyncio.Semaphore(client.concurrency)
            total_tests = sum(
                len(zf.zimplementations) * len(zf.ztesters) for zf in self.zfunctions
            )
            processed = [0]  # mutable counter

            tasks = [
                self.fetch_single_test_status(
                    client, semaphore, zf, impl, tester, processed, total_tests
                )
                for zf in self.zfunctions
                for impl in zf.zimplementations
                for tester in zf.ztesters
            ]

            # Run all tasks concurrently with progress logging
            for coro in asyncio.as_completed(tasks):
                await coro

    async def fetch_single_test_status(
        self,
        client: Client,
        semaphore: asyncio.Semaphore,
        zf: Zfunction,
        impl: Zimpl,
        tester: Ztester,
        processed: list[int],
        total_tests: int,
    ) -> None:
        """Fetch test status for a single implementation/tester."""
        async with semaphore:
            status = await client.fetch_test_status(zf.zid, impl.zid, tester.zid)

        if not hasattr(impl, "test_results") or impl.test_results is None:
            impl.test_results = {}
        impl.test_results[tester.zid] = status

        # update progress
        processed[0] += 1
        if processed[0] % self.progress_interval == 0 or processed[0] == total_tests:
            logging.info(
                "Fetched %d/%d tests (latest ZFunction: %s)",
                processed[0],
                total_tests,
                zf.zid,
            )

    async def calculate(self) -> None:
        """Compute ZFunctions and connected implementations; populate self.zfunctions and self.table."""
        self.zfunctions = []
        zid_count = 0

        # Build maps
        self.build_ztester_map()
        if "Z13517" not in self.ztester_map.keys():
            raise MissingData("Z13517 is missing from testers_map!")
        self.build_zimpl_map()
        if "Z201" not in self.zimpl_map.keys():
            raise MissingData("Z201 is missing from zimpl_map!")

        logging.info(f"Processing functions from {self.jsonl_file}...")
        processed = 0
        with open(self.jsonl_file, "r", encoding="utf-8") as f:
            for line in f:
                processed += 1
                if processed % self.progress_interval == 0:
                    logging.info(
                        f"Processed {processed} lines, {zid_count} ZFunctions so far..."
                    )
                zf = Zfunction.from_json_line(line)
                if zf.is_correct_type:
                    logger.debug(f"Working on {zf.link}")
                    zf.extract_ztesters(self.ztester_map)
                    logger.debug(
                        "ZFunction %s has testers: %s",
                        zf.zid,
                        [t.zid for t in zf.ztesters],
                    )
                    zf.extract_zimpl(self.zimpl_map)
                    logger.debug(
                        "ZFunction %s has implementations: %s",
                        zf.zid,
                        [t.zid for t in zf.zimplementations],
                    )
                    self.zfunctions.append(zf)
                    zid_count += 1

                    if zid_count >= config.MAX_FUNCTIONS:
                        logging.info(
                            f"Reached MAX_FUNCTIONS={config.MAX_FUNCTIONS}, stopping early."
                        )
                        break

        logging.info(f"Collected {zid_count} ZFunctions.")

        # --- Tester status ------------------
        # Initialize test_status_map for all implementations
        self.init_test_status_map()
        # Fetch all statuses
        await self.fetch_all_test_statuses()
        self.apply_test_status_map()
        # Write debug file if in DEBUG mode
        self.write_test_status_debug()

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
        """Write a wikitext table for a given ZID range."""
        os.makedirs(os.path.dirname(filename), exist_ok=True)

        with open(filename, "w", encoding="utf-8") as f:
            self._write_table_header(f)
            self._write_table_rows(f, min_zid, max_zid)
            f.write("|}\n")

        logging.info(f"Wikitext table written to {filename}")

    def _write_table_header(self, f) -> None:
        """Write the wikitext table header."""
        f.write(
            "; Note: Disconnected tests/implementations are not in presently in the dump\n\n"
            f"Last update: {self.last_update}\n"
            '{| class="wikitable sortable"\n'
            "! rowspan='2' | Function \n"
            "! rowspan='2' | Aliases \n"
            "! colspan='3' | Connected \n"
            "! rowspan='2' | Translations\n"
            "|-\n"
            "! Implementations \n"
            "! Pass / Fail / Error \n"
            "! Total Tests\n"
        )

    def _write_table_rows(self, f, min_zid: int = 1, max_zid: int = None) -> None:
        """Write all rows in the given ZID range."""
        for zf in self.zfunctions:
            zid_number = self._parse_zid_number(zf.zid)
            if zid_number is None:
                continue

            if (min_zid is not None and zid_number < min_zid) or (
                max_zid is not None and zid_number > max_zid
            ):
                continue

            pass_count, fail_count, error_count, total_tests = self._count_test_status(
                zf
            )

            f.write(
                f"|-\n| [[{zf.zid}]] || {zf.count_aliases} || "
                f"{zf.number_of_implementations} || "
                f"{pass_count} / {fail_count} / {error_count} || "
                f"{total_tests} || {zf.count_languages}\n"
            )

    # ----------------- Static helpers --------------
    @staticmethod
    def _parse_zid_number(zid: str) -> int | None:
        """Convert a ZID string like 'Z27327' to an integer."""
        try:
            return int(zid.lstrip("Z"))
        except ValueError:
            return None

    @staticmethod
    def _count_test_status(zf: Zfunction) -> tuple[int, int, int, int]:
        """Return pass_count, fail_count, error_count, total_tests for a ZFunction."""
        pass_count = fail_count = error_count = total_tests = 0

        for impl in zf.zimplementations:
            impl_results = getattr(impl, "test_results", {})
            for status in impl_results.values():
                total_tests += 1
                if status == TestStatus.PASS:
                    pass_count += 1
                elif status == TestStatus.FAIL:
                    fail_count += 1
                elif status == TestStatus.ERROR:
                    error_count += 1

        return pass_count, fail_count, error_count, total_tests

    def write_test_status_debug(self) -> None:
        """Write the full test_status_map to a file for debugging (DEBUG only)."""
        if config.loglevel != logging.DEBUG:
            return

        debug_dir = Path("debug_maps")
        debug_dir.mkdir(exist_ok=True)
        debug_file = debug_dir / "test_status_map.json"

        # Serialize enums as strings
        serializable_map = {
            impl_zid: {
                tester_zid: status.name if hasattr(status, "name") else str(status)
                for tester_zid, status in tester_map.items()
            }
            for impl_zid, tester_map in self.test_status_map.items()
        }

        with open(debug_file, "w", encoding="utf-8") as f:
            json.dump(serializable_map, f, indent=2)

        logging.debug(f"Full test_status_map written to {debug_file}")
