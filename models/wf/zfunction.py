# ./models/wf/zfunction.py
import logging
from pprint import pprint
from typing import List, Dict

from pydantic import Field

import config
from models.wf.enums import ZobjectType
from models.wf.zentity import Zentity
from models.wf.zimpl import Zimpl
from models.wf.ztester import Ztester

logger = logging.getLogger(__name__)


class Zfunction(Zentity):
    """
    Z8 function wrapper.
    """

    EXPECTED_TYPE: ZobjectType = ZobjectType.FUNCTION

    ztesters: List[Ztester] = Field(default_factory=list)
    zimplementations: List[Zimpl] = Field(default_factory=list)

    # ---------- Counts ----------
    @property
    def number_of_implementations(self) -> int:
        return len(self.zimplementations)

    # ---------- Population ----------
    def extract_ztesters(self, map_: Dict[str, Ztester]) -> None:
        """
        Populate self.ztesters by looking up ZIDs in Z2K2 > Z8K3.
        """
        z2k2 = self.data.get("Z2K2")
        logger.debug(f"Processing: ")
        if config.loglevel == logging.DEBUG:
            pprint(z2k2)
        if not isinstance(z2k2, dict):
            return

        z8k3 = z2k2.get("Z8K3")
        if not z8k3:
            return

        # Ensure z8k3 is a list
        if isinstance(z8k3, str):
            z8k3 = [z8k3]

        for zid in z8k3:
            tester = map_.get(zid)
            if tester:
                self.ztesters.append(tester)
        logger.debug(
            "ZFunction %s extracted testers: %s",
            self.zid,
            [i.zid for i in self.ztesters],
        )

    def extract_zimpl(self, map_: Dict[str, Zimpl]) -> None:
        """
        Populate self.zimpl by looking up ZIDs in Z2K2 > Z8K4.
        See https://www.wikifunctions.org/view/en/Z8
        """
        value_of_persistant_object = self.data.get("Z2K2")
        logger.debug(f"Processing: value_of_persistant_object")
        if config.loglevel == logging.DEBUG:
            pprint(value_of_persistant_object)
        if not isinstance(value_of_persistant_object, dict):
            return

        implementation_data = value_of_persistant_object.get("Z8K4")
        logger.debug(f"Processing: implementation_data:")
        if config.loglevel == logging.DEBUG:
            pprint(implementation_data)
        if not implementation_data:
            return

        # Ensure z8k4 is a list
        if isinstance(implementation_data, str):
            implementation_data = [implementation_data]

        for zid in implementation_data:
            logger.debug(f"Looking up: {zid}")
            zimpl = map_.get(zid)
            if zimpl:
                self.zimplementations.append(zimpl)
        logger.debug(
            "ZFunction %s extracted impls: %s",
            self.zid,
            [i.zid for i in self.zimplementations],
        )
