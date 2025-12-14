# ./models/wf/zfunction.py
from typing import List, Dict
import logging
from pydantic import Field
from flatten_json import flatten

from models.wf.zentity import Zentity
from models.wf.zimpl import Zimpl
from models.wf.ztester import Ztester

logger = logging.getLogger(__name__)


class Zfunction(Zentity):
    """
    Z8 function wrapper.
    """

    connected_implementations: List[str] = Field(default_factory=list)
    ztesters: List[Ztester] = Field(default_factory=list)

    # ---------- Identity ----------
    @property
    def is_function(self) -> bool:
        if not isinstance(self.data, dict):
            return False
        z2k2 = self.data.get("Z2K2")
        return isinstance(z2k2, dict) and z2k2.get("Z1K1") == "Z8"

    # ---------- Counts ----------
    @property
    def number_of_connected_implementations(self) -> int:
        return len(self.connected_implementations)

    @property
    def count_testers(self) -> int:
        return len(self.ztesters)

    # ---------- Population ----------
    def extract_testers(self, tester_map: Dict[str, Ztester]) -> None:
        """
        Populate self.ztesters by looking up ZIDs in the prebuilt tester_map.
        """
        self.ztesters = []

        z2k2 = self.data.get("Z2K2")
        if not isinstance(z2k2, dict):
            return

        z8k1 = z2k2.get("Z8K1")
        if not z8k1:
            return

        # Flatten Z8K1 to get all tester ZIDs
        to_flatten = {"root": z8k1} if isinstance(z8k1, list) else z8k1
        flat_dict = flatten(to_flatten)
        tester_zids = {v for v in flat_dict.values() if isinstance(v, str)}

        # Lookup in the prebuilt map
        for zid in tester_zids:
            tester = tester_map.get(zid)
            if tester:
                self.ztesters.append(tester)

    def populate(self) -> None:
        """
        Populate implementations.
        Call extract_testers separately with tester_map.
        """
        if not self.is_function:
            return

        self.connected_implementations = Zimpl(data=self.data).extract_connected()

        logger.debug(
            "ZFunction %s: %d implementations, %d testers",
            self.zid,
            self.number_of_connected_implementations,
            self.count_testers,
        )

    # ---------- Apply external connected implementations ----------
    def apply_connected_implementations(self, impls: List[str]) -> None:
        """
        Apply externally fetched connected implementations to this function.
        """
        self.connected_implementations = impls
        logger.debug(
            "Applied %d connected implementations to ZFunction %s",
            len(impls),
            self.zid,
        )
