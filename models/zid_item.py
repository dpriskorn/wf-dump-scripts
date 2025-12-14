import logging
from typing import Any, List, Dict

from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


class ZIDItem(BaseModel):
    """
    Represents a Wikifunctions ZID item with optional test metadata.

    Attributes:
        title (str): The human-readable title of the ZID item.
        data (Any): The raw JSON object representing the full item data.

        zfunction_id (str): Optional ZID of the ZFunction to be tested.
        ztesters (List[str]): Optional list of ZIDs of testers to run on the implementation.

        test_results (Dict[str, bool]): Stores the pass/fail results of each tester, keyed by tester ZID.
                                        True indicates a passed test (Z41), False indicates failed (Z42) or unknown.
    """
    title: str = Field(description="ZID of the ZFunction to test")
    data: Any  # raw JSON object

    connected_implementations: List[str] = Field(
        default_factory=list,
        description="List of ZIDs of implementations (Z14) connected to this ZFunction",
    )
    ztesters: List[str] = Field(default_factory=list, description="List of tester ZIDs")

    # test_results: Dict[str, bool] = Field(default_factory=dict)

    @property
    def is_function(self) -> bool:
        """
        Returns True if this ZID item represents a ZFunction (Z8).
        """
        if not isinstance(self.data, dict):
            return False

        z2k2 = self.data.get("Z2K2")
        if not isinstance(z2k2, dict):
            return False

        return z2k2.get("Z1K1") == "Z8"

    @property
    def zid(self) -> str:
        return self.title

    # --- Public counters ---
    @property
    def count_aliases(self) -> int:
        return self._count_aliases(self.data)

    @property
    def count_languages(self) -> int:
        return self._count_languages(self.data)

    @property
    def count_implementations(self) -> int:
        return self._count_implementations(self.data)

    @property
    def count_testers(self) -> int:
        return len(self.ztesters)

    # @property
    # def testers_passed(self) -> int:
    #     return sum(1 for t in self.test_results if t)
    #
    # @property
    # def testers_failed(self) -> int:
    #     return sum(1 for t in self.test_results if not t)

    # --- Private recursive helpers ---
    def _count_aliases(self, d: Any) -> int:
        if isinstance(d, dict):
            if "Z1K1" in d:
                return 1
            return sum(self._count_aliases(v) for v in d.values())
        elif isinstance(d, list):
            return sum(self._count_aliases(i) for i in d)
        return 0

    def _count_languages(self, d: Any) -> int:
        if isinstance(d, dict):
            if "Z12K1" in d:
                langs = d["Z12K1"]
                if isinstance(langs, list):
                    return sum(1 for i in langs if isinstance(i, dict) and "Z11K2" in i)
            return sum(self._count_languages(v) for v in d.values())
        elif isinstance(d, list):
            return sum(self._count_languages(i) for i in d)
        return 0

    def _count_implementations(self, d: Any) -> int:
        count = 0
        if isinstance(d, dict):
            z2k1 = d.get("Z2K1")
            if isinstance(z2k1, dict):
                z6k1 = z2k1.get("Z6K1")
                if isinstance(z6k1, list):
                    count += len(z6k1)
                elif z6k1 is not None:
                    count += 1
            count += sum(self._count_implementations(v) for v in d.values())
        elif isinstance(d, list):
            count += sum(self._count_implementations(i) for i in d)
        return count

    # @retry(
    #     stop=stop_after_attempt(5),
    #     wait=wait_exponential(multiplier=1, min=1, max=16),
    #     retry=retry_if_exception_type((requests.HTTPError, requests.ConnectionError, requests.Timeout))
    # )
    # def _perform_request(self, params: dict) -> Response:
    #     headers = {"User-Agent": config.user_agent}
    #     response = requests.get(config.BASE_API_URL, params=params, headers=headers, timeout=10)
    #     logger.debug(f"URL: {response.url}")
    #     if response.status_code == 429:
    #         # raise to trigger retry with exponential backoff
    #         raise requests.HTTPError("Rate limited (429)")
    #     response.raise_for_status()
    #     return response

    # def fetch_test_results(self) -> None:
    #     """Fetch testers' pass/fail status and store as a dict of {tester_id: bool}."""
    #     if self.test_results:
    #         logger.info("Test results already fetched; skipping request.")
    #         return
    #
    #     if not self.zimplementation_id or not self.ztesters:
    #         logger.warning(
    #             "No implementation ID or testers provided. Setting empty test_results."
    #         )
    #         self.test_results = {}
    #         return
    #
    #     logger.info(
    #         "Fetching test results for ZFunction %s, Implementation %s with %d testers.",
    #         self.zfunction_id,
    #         self.zimplementation_id,
    #         len(self.ztesters)
    #     )
    #
    #     params = {
    #         "action": "wikilambda_perform_test",
    #         "format": "json",
    #         "formatversion": 2,
    #         "wikilambda_perform_test_zfunction": self.zfunction_id,
    #         "wikilambda_perform_test_zimplementations": self.zimplementation_id,
    #         "wikilambda_perform_test_ztesters": "|".join(self.ztesters),
    #         "uselang": self.uselang
    #     }
    #
    #     try:
    #         data = self._perform_request(params)
    #     except Exception as e:
    #         logger.error("Failed to fetch test results: %s", e)
    #         self.test_results = {}
    #         return
    #
    #     results: dict[str, bool] = {}
    #     for entry in data.get("query", {}).get("wikilambda_perform_test", []):
    #         tester_id = entry.get("zTesterId")
    #         validate_status = entry.get("validateStatus")
    #         results[tester_id] = validate_status == "Z41"
    #         logger.debug("Tester %s: %s", tester_id, "PASSED" if results[tester_id] else "FAILED")
    #
    #     self.test_results = results
    #     logger.info("Fetched %d test results.", len(self.test_results))
    #
    #     # New attribute
    #     connected_implementations: List[str] = Field(
    #         default_factory=list,
    #         description="List of ZIDs of implementations (Z14) connected to this ZFunction",
    #     )

    def apply_connected_implementations(self, impls: List[str]) -> None:
        self.connected_implementations = impls

    @property
    def number_of_connected_implementations(self):
        return len(self.connected_implementations)

    def populate_and_fetch_tests_and_implementations(self) -> None:
        """
        Populate implementation and tester metadata from the `data` attribute
        and fetch connected implementations and test results.

        Non-function ZIDs (non-Z8) are skipped entirely.
        """
        if self.is_function:
            self.fetch_connected_implementations()
            logger.debug(f"Implementations:"
                         f"Total: {self.count_implementations}"
                         f"Connected: {self.number_of_connected_implementations}"
                         f"Disconnected: {self.count_implementations - self.number_of_connected_implementations}")
            #logger.debug("Populating test metadata from data...")
            self.ztesters = self.data.get("ZTesters", [])
            logger.debug(f"Testers: {self.ztesters}")
            # self.fetch_test_results()
            # input("press enter to cont")
