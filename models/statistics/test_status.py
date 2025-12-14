import asyncio
import json
import logging
from pathlib import Path
from typing import Dict

from pydantic import BaseModel

import config
from models.wf.client import Client
from models.wf.enums import TestStatus
from models.wf.zfunction import Zfunction


class TestStatusManager(BaseModel):
    """Handles fetching and storing test statuses for ZFunctions."""
    test_status_map: Dict[str, Dict[str, TestStatus]] = {}

    def init_map(self, zfunctions: list[Zfunction]):
        for zf in zfunctions:
            for impl in zf.zimplementations:
                self.test_status_map.setdefault(impl.zid, {})

    def apply_to_impls(self, zfunctions: list[Zfunction]):
        for zf in zfunctions:
            for impl in zf.zimplementations:
                if hasattr(impl, "test_results") and impl.test_results:
                    self.test_status_map[impl.zid] = impl.test_results

    async def fetch_all(self, zfunctions: list[Zfunction]):
        async with Client(concurrency=8) as client:
            semaphore = asyncio.Semaphore(client.concurrency)
            tasks = [
                self._fetch_single(client, semaphore, zf, impl, tester)
                for zf in zfunctions
                for impl in zf.zimplementations
                for tester in zf.ztesters
            ]
            for coro in asyncio.as_completed(tasks):
                await coro

    @staticmethod
    async def _fetch_single(client, semaphore, zf, impl, tester):
        async with semaphore:
            status = await client.fetch_test_status(zf.zid, impl.zid, tester.zid)
        if not hasattr(impl, "test_results") or impl.test_results is None:
            impl.test_results = {}
        impl.test_results[tester.zid] = status

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

