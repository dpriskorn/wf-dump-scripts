# ./models/ztester.py

from models.wf.zentity import Zentity


class Ztester(Zentity):
    def is_tester(self) -> bool:
        return isinstance(self.data, dict) and self.data.get("Z1K1") == "Z13"
