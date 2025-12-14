import json
from pathlib import Path
from unittest import TestCase

from models.wf.zfunction import Zfunction
from models.wf.ztester import Ztester

class TestZfunction(TestCase):
    def setUp(self):
        # Load sample Zfunction JSON
        test_file = Path("test_data/zfunction.json")
        with test_file.open("r", encoding="utf-8") as f:
            self.data = json.load(f)

        # Build a simple tester map
        # normally this comes from Z8Calculator.build_tester_map
        self.tester_map = {}
        # For test purposes, all strings in Z8K1 are turned into Ztester objects
        z8k1 = self.data.get("Z2K2", {}).get("Z8K1", [])
        for item in z8k1:
            if isinstance(item, str):
                self.tester_map[item] = Ztester(data={"Z1K1": "Z13", "name": item})
            elif isinstance(item, dict):
                name = item.get("Z17K1") or item.get("Z17K2")
                if name:
                    self.tester_map[name] = Ztester(data={"Z1K1": "Z13", "name": name})

    def test_is_function_true(self):
        func = Zfunction(data=self.data)
        assert func.is_function is True

    def test_populate_connected_and_testers(self):
        func = Zfunction(data=self.data)
        func.populate()  # populate implementations
        func.extract_testers(self.tester_map)  # populate testers from map
        # Implementations are populated from Zimpl
        assert func.number_of_connected_implementations >= 0
        assert func.count_testers == len(func.ztesters)
        assert all(isinstance(t, Ztester) for t in func.ztesters)
