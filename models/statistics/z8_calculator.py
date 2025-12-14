# ./models/z8_calculator.py
import logging
from typing import List, Dict

from pydantic import BaseModel, Field

import config
from models.statistics.zmap import ZMap
from models.statistics.zwikiwriter import ZwikiWriter
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
    progress_interval: int = Field(
        default=1000,
        description="Number of ZFunctions processed before reporting progress.",
    )

    # --- Attributes to hold intermediate data ---
    zfunctions: List[Zfunction] = Field(
        default_factory=list,
        description="List of ZFunction objects collected from the input file.",
    )
    test_status_map: Dict[str, Dict[str, TestStatus]] = Field(
        default_factory=dict, description="implementation_zid -> tester_zid -> status"
    )

    class Config:
        arbitrary_types_allowed = True

    def init_test_status_map(self) -> None:
        for zf in self.zfunctions:
            for impl in zf.zimplementations:
                self.test_status_map.setdefault(impl.zid, {})

    def apply_test_status_map(self) -> None:
        for zf in self.zfunctions:
            for impl in zf.zimplementations:
                if hasattr(impl, "test_results") and impl.test_results:
                    self.test_status_map[impl.zid] = impl.test_results

    # async def fetch_zfunction_test_status(self, client: Client, zf: Zfunction) -> None:
    #     """Fetch test statuses for a single ZFunction and update the map."""
    #     status_map = await client.fetch_function_test_status_map(zf)
    #     for impl_zid, tests in status_map.items():
    #         self.test_status_map.setdefault(impl_zid, {}).update(tests)
    #
    # async def fetch_zfunction_statuses(
    #     self,
    #     client: Client,
    #     semaphore: asyncio.Semaphore,
    #     zf: Zfunction,
    #     processed: int,
    #     total_tests: int,
    # ) -> None:
    #     """Fetch all test statuses for a single ZFunction."""
    #     for impl in zf.zimplementations:
    #         for tester in zf.ztesters:
    #             await self.fetch_single_test_status(client, semaphore, zf, impl, tester)
    #             processed += 1
    #             if processed % self.progress_interval == 0 or processed == total_tests:
    #                 logging.info(
    #                     "Fetched %d/%d tests (latest ZFunction: %s)",
    #                     processed,
    #                     total_tests,
    #                     zf.zid,
    #                 )
    #
    # async def fetch_all_test_statuses(self) -> None:
    #     """Fetch all test statuses concurrently with progress logging."""
    #     async with Client(concurrency=8) as client:
    #         semaphore = asyncio.Semaphore(client.concurrency)
    #         total_tests = sum(
    #             len(zf.zimplementations) * len(zf.ztesters) for zf in self.zfunctions
    #         )
    #         processed = [0]  # mutable counter
    #
    #         tasks = [
    #             self.fetch_single_test_status(
    #                 client, semaphore, zf, impl, tester, processed, total_tests
    #             )
    #             for zf in self.zfunctions
    #             for impl in zf.zimplementations
    #             for tester in zf.ztesters
    #         ]
    #
    #         # Run all tasks concurrently with progress logging
    #         for coro in asyncio.as_completed(tasks):
    #             await coro
    #
    # async def fetch_single_test_status(
    #     self,
    #     client: Client,
    #     semaphore: asyncio.Semaphore,
    #     zf: Zfunction,
    #     impl: Zimpl,
    #     tester: Ztester,
    #     processed: list[int],
    #     total_tests: int,
    # ) -> None:
    #     """Fetch test status for a single implementation/tester."""
    #     async with semaphore:
    #         status = await client.fetch_test_status(zf.zid, impl.zid, tester.zid)
    #
    #     if not hasattr(impl, "test_results") or impl.test_results is None:
    #         impl.test_results = {}
    #     impl.test_results[tester.zid] = status
    #
    #     # update progress
    #     processed[0] += 1
    #     if processed[0] % self.progress_interval == 0 or processed[0] == total_tests:
    #         logging.info(
    #             "Fetched %d/%d tests (latest ZFunction: %s)",
    #             processed[0],
    #             total_tests,
    #             zf.zid,
    #         )

    async def calculate(self) -> None:
        """Compute ZFunctions and connected implementations; populate self.zfunctions and self.table."""
        self.extract_date()
        zid_count = 0

        # Build maps
        builder = ZMap(self.jsonl_file)
        ztester_map = builder.build_map(Ztester, "tester map")
        zimpl_map = builder.build_map(Zimpl, "implementation map")

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
                    zf.extract_ztesters(ztester_map)
                    logger.debug(
                        "ZFunction %s has testers: %s",
                        zf.zid,
                        [t.zid for t in zf.ztesters],
                    )
                    zf.extract_zimpl(zimpl_map)
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

        # Test statuses
        status_manager = TestStatus()
        status_manager.init_map(self.zfunctions)
        await status_manager.fetch_all(self.zfunctions)
        status_manager.apply_to_impls(self.zfunctions)

        # Wikitext
        writer = ZwikiWriter(self.zfunctions, self.last_update, self.output_file_prefix, self.jsonl_file)
        writer.write_wikitext()

        # # --- Tester status ------------------
        # # Initialize test_status_map for all implementations
        # self.init_test_status_map()
        # # Fetch all statuses
        # await self.fetch_all_test_statuses()
        # self.apply_test_status_map()
        # # Write debug file if in DEBUG mode
        # self.write_test_status_debug()

