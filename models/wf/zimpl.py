# ./models/zimpl.py
from typing import List

from models.wf.zentity import Zentity


class Zimpl(Zentity):
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

    def is_implementation(self) -> bool:
        return isinstance(self.data, dict) and self.data.get("Z1K1") == "Z14"
