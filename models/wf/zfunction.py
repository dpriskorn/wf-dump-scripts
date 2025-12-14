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
    zimpl: List[Zimpl] = Field(default_factory=list)

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
    def extract_ztesters(self, map: Dict[str, Ztester]) -> None:
        """
        Populate self.ztesters by looking up ZIDs in Z2K2 > Z8K3.
        """
        z2k2 = self.data.get("Z2K2")
        if not isinstance(z2k2, dict):
            return

        z8k3 = z2k2.get("Z8K3")
        if not z8k3:
            return

        # Ensure z8k3 is a list
        if isinstance(z8k3, str):
            z8k3 = [z8k3]

        for zid in z8k3:
            tester = map.get(zid)
            if tester:
                self.ztesters.append(tester)

    def extract_zimpl(self, map: Dict[str, Zimpl]) -> None:
        """
        Populate self.zimpl by looking up ZIDs in Z2K2 > Z8K4.
        See https://www.wikifunctions.org/view/en/Z8
        """
        z2k2 = self.data.get("Z2K2")
        if not isinstance(z2k2, dict):
            return

        z8k4 = z2k2.get("Z8K4")
        if not z8k4:
            return

        # Ensure z8k4 is a list
        if isinstance(z8k4, str):
            z8k4 = [z8k4]

        for zid in z8k4:
            zimpl = map.get(zid)
            if zimpl:
                self.zimpl.append(zimpl)

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
