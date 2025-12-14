import json
from pathlib import Path
from unittest import TestCase

from models.wf.ztester import Ztester


class TestZtester(TestCase):
    def setUp(self):
        dump_test_file = Path("test_data/dump/ztester.json")
        with dump_test_file.open("r", encoding="utf-8") as f:
            self.data = json.load(f)

    def test_is_tester_true(self):
        tester = Ztester(data={"Z1K1": "Z13"})
        assert tester.is_tester() is True

    def test_extract_testers_from_json(self):
        # In this new design, Ztester itself may just wrap data
        tester = Ztester(data=self.data)
        assert tester.is_tester() is False  # Z1K1 is "Z2", not "Z13"
        # if you want, you could test name extraction if your Ztester now supports that
