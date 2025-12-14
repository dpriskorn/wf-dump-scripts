# models/wikifunctions_client.py
import asyncio
import logging
from typing import Iterable, Dict, List

import httpx
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type

import config

logger = logging.getLogger(__name__)


class Client:
    def __init__(
        self,
        *,
        concurrency: int = 8,
        timeout: float = 10.0,
    ):
        self.semaphore = asyncio.Semaphore(concurrency)
        self.timeout = timeout

        self.client = httpx.AsyncClient(
            base_url=config.BASE_API_URL,
            headers={"User-Agent": config.user_agent},
            timeout=httpx.Timeout(timeout),
            follow_redirects=True,
        )

    async def close(self):
        await self.client.aclose()

    # ---------- low-level request ----------

    @retry(
        wait=wait_exponential(multiplier=1, min=1, max=32),
        stop=stop_after_attempt(5),
        retry=retry_if_exception_type((httpx.HTTPError,)),
    )
    async def _get(self, params: dict) -> dict:
        async with self.semaphore:
            # Log the URL before the request
            full_url = httpx.URL(self.client.base_url, params=params)
            logger.debug("Fetching URL: %s", full_url)

            resp = await self.client.get("", params=params)
            if resp.status_code == 429:
                raise httpx.HTTPError("Rate limited (429)")
            resp.raise_for_status()

            # Log the final URL after redirects
            logger.debug("Final URL after redirects: %s", resp.url)

            return resp.json()

    # ---------- high-level APIs ----------

    async def fetch_connected_implementations(self, zid: str, limit: int = 100) -> List[str]:
        params = {
            "action": "query",
            "format": "json",
            "formatversion": 2,
            "list": "wikilambdafn_search",
            "wikilambdafn_zfunction_id": zid,
            "wikilambdafn_type": "Z14",
            "wikilambdafn_limit": limit,
        }

        data = await self._get(params)

        impls = [
            entry["zid"]
            for entry in data.get("query", {}).get("wikilambdafn_search", [])
            if "zid" in entry
        ]

        logger.debug("ZID %s → %d implementations", zid, len(impls))
        return impls

    async def bulk_fetch_connected_implementations(
        self, zids: Iterable[str]
    ) -> Dict[str, List[str]]:
        tasks = {
            zid: asyncio.create_task(self.fetch_connected_implementations(zid))
            for zid in zids
        }

        results: Dict[str, List[str]] = {}
        for zid, task in tasks.items():
            try:
                results[zid] = await task
            except Exception:
                logger.exception("Failed fetching implementations for %s", zid)
                results[zid] = []

        return results
