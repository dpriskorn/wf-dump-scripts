import json
from pathlib import Path
from unittest import TestCase

from models.wf.zfunction import Zfunction
from models.wf.ztester import Ztester
from models.wf.zimpl import Zimpl


class TestZfunction(TestCase):
    def setUp(self):
        # Load sample Zfunction JSON
        zfunc_file = Path("test_data/dump/zfunction.json")
        with zfunc_file.open("r", encoding="utf-8") as f:
            self.zfunction_data = json.load(f)

        # Mock maps with keys matching Z8K3 and Z8K4
        self.impl_map = {
            "Z14": Zimpl(data={"Z1K1": "Z2", "Z2K1": {"Z1K1": "Z6", "Z6K1": "Z14"}}),
            "Z27335": Zimpl(
                data={"Z1K1": "Z2", "Z2K1": {"Z1K1": "Z6", "Z6K1": "Z27335"}}
            ),
        }
        self.tester_map = {
            "Z20": Ztester(data={"Z1K1": "Z2", "Z2K1": {"Z1K1": "Z6", "Z6K1": "Z20"}}),
            "Z27328": Ztester(
                data={"Z1K1": "Z2", "Z2K1": {"Z1K1": "Z6", "Z6K1": "Z27328"}}
            ),
            "Z27329": Ztester(
                data={"Z1K1": "Z2", "Z2K1": {"Z1K1": "Z6", "Z6K1": "Z27329"}}
            ),
            "Z27331": Ztester(
                data={"Z1K1": "Z2", "Z2K1": {"Z1K1": "Z6", "Z6K1": "Z27331"}}
            ),
            "Z27891": Ztester(
                data={"Z1K1": "Z2", "Z2K1": {"Z1K1": "Z6", "Z6K1": "Z27891"}}
            ),
        }

    def test_is_function_true(self):
        func = Zfunction(data=self.zfunction_data)
        self.assertTrue(func.is_correct_type)

    def test_extract_ztesters(self):
        func = Zfunction(data=self.zfunction_data)
        func.extract_ztesters(self.tester_map)

        self.assertEqual(len(func.ztesters), len(self.tester_map))
        for t in func.ztesters:
            self.assertIsInstance(t, Ztester)
            self.assertIn(t.zid, self.tester_map)

    def test_extract_zimpl(self):
        func = Zfunction(data=self.zfunction_data)
        func.extract_zimpl(self.impl_map)

        self.assertEqual(len(func.zimplementations), len(self.impl_map))
        for impl in func.zimplementations:
            self.assertIsInstance(impl, Zimpl)
            self.assertIn(impl.zid, self.impl_map)

    def test_empty_maps(self):
        func = Zfunction(data=self.zfunction_data)
        func.extract_ztesters({})
        func.extract_zimpl({})
        self.assertEqual(len(func.ztesters), 0)
        self.assertEqual(len(func.zimplementations), 0)
