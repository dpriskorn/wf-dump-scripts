from unittest import TestCase
from models.wf.zimpl import Zimpl

import json
from pathlib import Path


class TestZimpl(TestCase):
    def setUp(self):
        test_file = Path("test_data/zimplementation.json")
        with test_file.open("r", encoding="utf-8") as f:
            self.data = json.load(f)

    def test_is_implementation_true(self):
        impl = Zimpl(data={"Z1K1": "Z14"})
        assert impl.is_implementation is True

    def test_extract_connected(self):
        impl = Zimpl(data=self.data)
        connected = impl.extract_connected()
        assert isinstance(connected, list)
        assert all(isinstance(zid, str) for zid in connected)
