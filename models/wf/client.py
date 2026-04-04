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

    username: str = Field(default=config.BOT_USERNAME)
    password: str = Field(default=config.BOT_PASSWORD)

    logged_in: bool = False
    login_lock: asyncio.Lock = Field(default_factory=asyncio.Lock)

    class Config:
        arbitrary_types_allowed = True
        extra = "allow"

    # ---------- lifecycle ----------

    async def __aenter__(self):
        await self.init_client()
        await self.ensure_login()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    async def init_client(self):
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

    # ---------- auth ----------

    async def login(self):
        """
        MediaWiki bot login using AsyncClient.
        Keeps session cookies automatically.
        """

        if self.client is None:
            raise RuntimeError("Client not initialized")

        logger.debug("Fetching login token")

        # 1. get login token
        r1 = await self.client.get(
            "",
            params={
                "action": "query",
                "meta": "tokens",
                "type": "login",
                "format": "json",
            },
        )
        r1.raise_for_status()

        login_token = r1.json()["query"]["tokens"]["logintoken"]

        logger.debug("Logging in as %s", self.username)

        # 2. login
        r2 = await self.client.post(
            config.BASE_API_URL,
            data={
                "action": "login",
                "lgname": self.username,
                "lgpassword": self.password,
                "lgtoken": login_token,
                "format": "json",
            },
            headers={
                "Content-Type": "application/x-www-form-urlencoded"
            },
        )
        logger.error("Login raw response status: %s", r2.status_code)
        logger.error("Login raw response headers: %s", r2.headers)
        logger.error("Login raw response text: %s", r2.text[:500])
        r2.raise_for_status()

        data = r2.json()

        if data.get("login", {}).get("result") != "Success":
            raise Exception(f"Login failed: {data}")

        self.logged_in = True
        logger.debug("Login successful")

    async def ensure_login(self):
        """
        Ensures login happens once even with concurrency.
        """
        if self.logged_in:
            return

        async with self.login_lock:
            if not self.logged_in:
                await self.login()

    async def get_userinfo(self) -> dict:
        await self.ensure_login()

        r = await self.client.get(
            "",
            params={
                "action": "query",
                "meta": "userinfo",
                "format": "json",
            },
        )
        r.raise_for_status()
        return r.json()

    # ---------- low-level request ----------

    @retry(
        wait=wait_exponential(multiplier=1, min=1, max=32),
        stop=stop_after_attempt(5),
        retry=retry_if_exception_type((httpx.HTTPError,)),
    )
    async def _get(self, params: dict) -> dict:
        if self.client is None or self.semaphore is None:
            raise RuntimeError("Client not initialized")

        await self.ensure_login()

        async with self.semaphore:
            full_url = httpx.URL(self.client.base_url, params=params)
            logger.debug("Fetching URL: %s", full_url)

            resp = await self.client.get("", params=params)

            if resp.status_code == 429:
                raise httpx.HTTPError("Rate limited (429)")

            resp.raise_for_status()

            logger.debug("Final URL: %s", resp.url)
            return resp.json()

    # ---------- high-level APIs ----------

    async def fetch_test_status(
        self,
        function_zid: str,
        impl_zid: str,
        tester_zid: str,
    ) -> TestStatus:

        params = {
            "action": "wikilambda_perform_test",
            "format": "json",
            "formatversion": 2,
            "wikilambda_perform_test_zfunction": function_zid,
            "wikilambda_perform_test_zimplementations": impl_zid,
            "wikilambda_perform_test_ztesters": tester_zid,
            "uselang": "en",
        }

        full_url = f"{config.BASE_API_URL}?{urlencode(params)}"
        logger.debug("Query URL: %s", full_url)

        try:
            data = await self._get(params)
        except Exception as e:
            raise NoTestResultFound(f"Error fetching test status: {e}")

        entries = data.get("query", {}).get("wikilambda_perform_test", [])

        if not entries:
            raise NoTestResultFound(f"No result. See {full_url}")

        status_raw = entries[0].get("validateStatus", "")

        if "Z41" in status_raw:
            return TestStatus.PASS

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
            except Exception as e:
                raise NoTestResultFound(
                    f"Failed (function={function_zid} impl={impl.zid} tester={tester.zid}): {e}"
                )

            results[tester.zid] = status

        return results

    async def fetch_function_test_status_map(
        self,
        function: Zfunction,
    ) -> Dict[str, Dict[str, TestStatus]]:

        tasks = [
            self.fetch_impl_test_statuses(
                function.zid,
                impl,
                function.ztesters,
            )
            for impl in function.zimplementations
        ]

        impl_results = await asyncio.gather(*tasks)

        return {
            impl.zid: statuses
            for impl, statuses in zip(function.zimplementations, impl_results)
        }
