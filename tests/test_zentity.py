from unittest import TestCase
import json
from pathlib import Path

from models.exceptions import NoZidFound
from models.wf.zentity import Zentity


class TestZentity(TestCase):
    def setUp(self):
        test_file = Path("test_data/zfunction.json")
        with test_file.open("r", encoding="utf-8") as f:
            self.sample_data = json.load(f)

    def test_zid_extracted(self):
        entity = Zentity(data={"Z1K1": "Z2", "Z2K1": {"Z1K1": "Z6", "Z6K1": "Z11515"}})
        assert entity.zid == "Z11515"

    def test_zid_missing_raises(self):
        entity = Zentity(data={"Z1K1": "Z2", "Z2K1": {"Z1K1": "Z6"}})
        try:
            _ = entity.zid
            assert False, "Expected NoZidFound"
        except NoZidFound:
            pass

    def test_count_aliases(self):
        entity = Zentity(data=self.sample_data)
        assert entity.count_aliases() >= 0

    def test_count_languages(self):
        entity = Zentity(data=self.sample_data)
        assert entity.count_languages() >= 0
