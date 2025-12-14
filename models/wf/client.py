# models/wikifunctions_client.py
import asyncio
import logging
from typing import Iterable, Dict, List, Optional

import httpx
from pydantic import BaseModel, Field
from tenacity import (
    retry,
    wait_exponential,
    stop_after_attempt,
    retry_if_exception_type,
)

import config

logger = logging.getLogger(__name__)


class Client(BaseModel):
    concurrency: int = Field(default=8)
    timeout: float = Field(default=10.0)
    client: Optional[httpx.AsyncClient] = None
    semaphore: Optional[asyncio.Semaphore] = None

    class Config:
        arbitrary_types_allowed = True  # Allows asyncio.Semaphore and httpx.AsyncClient

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

    async def fetch_connected_implementations(
        self, zid: str, limit: int = 100
    ) -> List[str]:
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

    async def _safe_fetch_connected(self, zid: str) -> (str, List[str]):
        try:
            impls = await self.fetch_connected_implementations(zid)
            return zid, impls
        except (asyncio.TimeoutError, OSError, httpx.HTTPError) as e:
            logger.warning("Failed fetching implementations for %s: %s", zid, e)
            return zid, []

    async def bulk_fetch_connected_implementations(
        self, zids: Iterable[str]
    ) -> Dict[str, List[str]]:
        if self.client is None or self.semaphore is None:
            raise RuntimeError("HTTP client not initialized. Call init_client first.")

        results = await asyncio.gather(
            *(self._safe_fetch_connected(zid) for zid in zids)
        )
        return dict(results)
