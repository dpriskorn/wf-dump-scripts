# models/wikifunctions_client.py
import asyncio
import logging
from typing import Dict, Optional, List
from urllib.parse import urlencode

import httpx
from pydantic import BaseModel, Field
from tenacity import (
    retry,
    wait_exponential,
    stop_after_attempt,
    retry_if_exception_type,
)

import config
from models.exceptions import NoTestResultFound
from models.wf.enums import TestStatus
from models.wf.zfunction import Zfunction
from models.wf.zimpl import Zimpl
from models.wf.ztester import Ztester

logger = logging.getLogger(__name__)


class Client(BaseModel):
    concurrency: int = Field(default=8)
    timeout: float = Field(default=10.0)
    client: Optional[httpx.AsyncClient] = None
    semaphore: Optional[asyncio.Semaphore] = None

    class Config:
        arbitrary_types_allowed = True
        extra = "allow"

    async def __aenter__(self):
        await self.init_client()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    async def init_client(self):
        """Initialize HTTP client and semaphore (async setup)."""
        self.semaphore = asyncio.Semaphore(self.concurrency)
        self.client = httpx.AsyncClient(
            base_url=config.BASE_API_URL,
            headers={"User-Agent": config.user_agent},
            timeout=httpx.Timeout(self.timeout),
            follow_redirects=True,
        )

    async def close(self):
        if self.client:
            await self.client.aclose()

    # ---------- low-level request ----------

    @retry(
        wait=wait_exponential(multiplier=1, min=1, max=32),
        stop=stop_after_attempt(5),
        retry=retry_if_exception_type((httpx.HTTPError,)),
    )
    async def _get(self, params: dict) -> dict:
        if self.semaphore is None or self.client is None:
            raise RuntimeError("HTTP client not initialized. Call init_client first.")

        async with self.semaphore:
            full_url = httpx.URL(self.client.base_url, params=params)
            logger.debug("Fetching URL: %s", full_url)

            resp = await self.client.get("", params=params)
            if resp.status_code == 429:
                raise httpx.HTTPError("Rate limited (429)")
            resp.raise_for_status()

            logger.debug("Final URL after redirects: %s", resp.url)
            return resp.json()

    # ---------- high-level APIs ----------
    async def fetch_test_status(
        self,
        function_zid: str,
        impl_zid: str,
        tester_zid: str,
    ) -> TestStatus:
        """
        Example url and response:
        https://www.wikifunctions.org/w/api.php?action=wikilambda_perform_test&format=json&formatversion=2&wikilambda_perform_test_zfunction=Z27327&wikilambda_perform_test_zimplementations=Z30176&wikilambda_perform_test_ztesters=Z27328&uselang=en
        :param function_zid:
        :param impl_zid:
        :param tester_zid:
        :return:
        """
        logger.debug(
            "Fetching test status for Function=%s, Implementation=%s, Tester=%s",
            function_zid,
            impl_zid,
            tester_zid,
        )

        params = {
            "action": "wikilambda_perform_test",  # <-- correct API action
            "format": "json",
            "formatversion": 2,
            "wikilambda_perform_test_zfunction": function_zid,
            "wikilambda_perform_test_zimplementations": impl_zid,
            "wikilambda_perform_test_ztesters": tester_zid,
            "uselang": "en",
        }

        # Build a clickable URL using the configured base URL
        full_url = f"{config.BASE_API_URL}?{urlencode(params)}"
        logger.debug("Query URL: %s", full_url)
        logger.debug("Query parameters: %s", params)

        try:
            data = await self._get(params)
            # logger.debug("Raw response data: %s", data)
        except Exception as e:
            logger.exception(f"Error fetching test status, {e}")
            return TestStatus.UNKNOWN

        entries = data.get("query", {}).get("wikilambda_perform_test", [])
        logger.debug("Parsed entries: %s", entries)

        if not entries:
            logger.debug("No entries returned, status UNKNOWN")
            raise NoTestResultFound(f"See {full_url}")

        status_raw = entries[0].get("validateStatus", "")
        logger.debug("Raw status from entry: '%s'", status_raw)

        if "Z41" in status_raw:
            logger.debug("Test passed")
            return TestStatus.PASS

        logger.debug("Test failed")
        return TestStatus.FAIL

    async def fetch_impl_test_statuses(
        self,
        function_zid: str,
        impl: Zimpl,
        testers: List[Ztester],
    ) -> Dict[str, TestStatus]:
        results: Dict[str, TestStatus] = {}

        for tester in testers:
            try:
                status = await self.fetch_test_status(
                    function_zid,
                    impl.zid,
                    tester.zid,
                )
                logging.debug(
                    "Fetched status: %s for ZF %s, Impl %s, Tester %s",
                    status, function_zid, impl.zid, tester.zid
                )
            except (
                httpx.HTTPError,
                asyncio.TimeoutError,
                OSError,
                KeyError,
                ValueError,
                TypeError,
            ) as e:
                logger.warning(
                    "Test status fetch failed " "(function=%s impl=%s tester=%s): %s",
                    function_zid,
                    impl.zid,
                    tester.zid,
                    e,
                )
                status = TestStatus.ERROR

            results[tester.zid] = status

        return results

    async def fetch_function_test_status_map(
        self,
        function: Zfunction,
    ) -> Dict[str, Dict[str, TestStatus]]:
        result: Dict[str, Dict[str, TestStatus]] = {}

        tasks = [
            self.fetch_impl_test_statuses(
                function.zid,
                impl,
                function.ztesters,
            )
            for impl in function.zimplementations
        ]

        impl_results = await asyncio.gather(*tasks)

        for impl, statuses in zip(function.zimplementations, impl_results):
            result[impl.zid] = statuses

        return result
