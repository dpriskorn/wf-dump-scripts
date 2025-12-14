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
        tester = Ztester(data=self.data)
        assert tester.is_correct_type is True
