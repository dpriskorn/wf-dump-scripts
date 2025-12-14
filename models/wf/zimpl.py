# ./models/zimpl.py
from typing import List, Dict

from models.wf.enums import ZobjectType, TestStatus
from models.wf.zentity import Zentity


class Zimpl(Zentity):
    EXPECTED_TYPE: ZobjectType = ZobjectType.IMPLEMENTATION
    test_results: Dict[str, TestStatus] = {}

    def extract_connected(self) -> List[str]:
        """
        Extract connected Z14 implementations from a ZFunction.
        """
        try:
            impls = self.data["Z2K2"]["Z8K4"]
        except (KeyError, TypeError):
            return []

        if not isinstance(impls, list):
            return []

        # Typed list: ["Z14", impl1, impl2, ...]
        return impls[1:]
