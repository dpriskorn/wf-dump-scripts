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

from models.statistics.rate_limiter import RateLimiter


class TestStatusManager(BaseModel):
    """Handles fetching and storing test statuses for ZFunctions."""

    test_status_map: Dict[str, Dict[str, TestStatus]] = {}
    zfunctions: list[Zfunction]

    async def fetch_statuses_apply_and_write_debug(self) -> None:
        self.init_map()
        await self.fetch_all()
        self.apply_to_impls()
        self.write_test_status_debug()

    def init_map(self) -> None:
        for zf in self.zfunctions:
            for impl in zf.zimplementations:
                self.test_status_map.setdefault(impl.zid, {})

    def apply_to_impls(self) -> None:
        for zf in self.zfunctions:
            for impl in zf.zimplementations:
                if hasattr(impl, "test_results") and impl.test_results:
                    self.test_status_map[impl.zid] = impl.test_results

    async def fetch_all(self) -> None:
        async with Client(concurrency=8) as client:

            queue = asyncio.Queue()

            rate_limiter = RateLimiter(min_interval=0.5)

            # ----------------------------
            # Build job queue
            # ----------------------------
            for zf in self.zfunctions:
                for impl in zf.zimplementations:
                    for tester in zf.ztesters:
                        queue.put_nowait((zf, impl, tester))

            total = queue.qsize()
            completed = 0
            lock = asyncio.Lock()

            logging.info(f"[TestStatus] Starting fetch: {total} tasks")

            # ----------------------------
            # Worker
            # ----------------------------
            async def worker(worker_id: int):
                nonlocal completed

                sem = asyncio.Semaphore(client.concurrency)

                while True:
                    try:
                        zf, impl, tester = queue.get_nowait()
                    except asyncio.QueueEmpty:
                        return

                    async with sem:
                        await rate_limiter.wait()

                        try:
                            status = await client.fetch_test_status(
                                zf.zid,
                                impl.zid,
                                tester.zid
                            )

                            if not hasattr(impl, "test_results") or impl.test_results is None:
                                impl.test_results = {}

                            impl.test_results[tester.zid] = status

                        except Exception as e:
                            logging.warning(
                                f"[TestStatus] FAIL {zf.zid}/{impl.zid}/{tester.zid}: {e}"
                            )

                        async with lock:
                            completed += 1

                            if completed % 50 == 0 or completed == total:
                                logging.info(
                                    f"[TestStatus] Progress: {completed}/{total} "
                                    f"({completed / total * 100:.1f}%) | "
                                    f"Queue left: {queue.qsize()}"
                                )

                    queue.task_done()

            # ----------------------------
            # Start workers
            # ----------------------------
            workers = [
                asyncio.create_task(worker(i))
                for i in range(client.concurrency)
            ]

            await queue.join()

            for w in workers:
                w.cancel()

            logging.info("[TestStatus] DONE fetching all statuses")

    def write_test_status_debug(self) -> None:
        """Write the full test_status_map to a file for debugging (DEBUG only)."""

        if config.loglevel != logging.DEBUG:
            return

        debug_dir = Path("debug_maps")
        debug_dir.mkdir(exist_ok=True)

        debug_file = debug_dir / "test_status_map.json"

        serializable_map = {
            impl_zid: {
                tester_zid: (
                    status.name if hasattr(status, "name") else str(status)
                )
                for tester_zid, status in tester_map.items()
            }
            for impl_zid, tester_map in self.test_status_map.items()
        }

        with open(debug_file, "w", encoding="utf-8") as f:
            json.dump(serializable_map, f, indent=2)

        logging.debug(f"Full test_status_map written to {debug_file}")
