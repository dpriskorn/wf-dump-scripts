# ./models/zentity.py
import json
import logging
from abc import ABC
from typing import Any

from pydantic import BaseModel

import config
from models.exceptions import NoZidFound
from models.wf.enums import ZobjectType

logger = logging.getLogger(__name__)


class Zentity(ABC, BaseModel):
    data: Any  # raw JSON
    EXPECTED_TYPE: ZobjectType = None

    @classmethod
    def from_json_line(cls, line: str) -> "Zentity":
        """Load a JSONL line into the model and store it in self.data"""
        try:
            raw = json.loads(line)
            # validate via Pydantic
            return cls(data=raw)
        except json.JSONDecodeError as e:
            logger.error("Failed to parse JSON line: %s", e)
            raise
        except Exception as e:
            logger.exception(f"Unexpected error parsing line: {e}")
            raise

    @property
    def is_correct_type(self) -> bool:
        """
        Detect if this object represents the correct type.

        Expected structure:
        {
            "Z1K1": "Z2",
            "Z2K1": {
                "Z1K1": "Z6",
                "Z6K1": "Z11515"
            },
            "Z2K2": {
                "Z1K1": "<EXPECTED_TYPE>",
                ...
            }
        }

        The method checks that 'Z2K2' exists and that its 'Z1K1' field
        matches the expected type.
        """
        z2k2 = self.data.get("Z2K2")
        if not isinstance(z2k2, dict):
            logger.debug(f"We ignore String Z6 for now.")
            return False

        # Check if the type matches the expected type
        return z2k2.get("Z1K1") == self.EXPECTED_TYPE.value

    @property
    def zid(self) -> str:
        """
        Extract ZID from data.
        Expected structure: {"Z1K1": "Z2", "Z2K1": {"Z1K1": "Z6", "Z6K1": "Z11515"}}
        """
        try:
            return self.data["Z2K1"]["Z6K1"]
        except (KeyError, TypeError):
            raise NoZidFound(f"Could not extract ZID from data")

    # ---------- Generic recursive helpers ----------

    @property
    def count_aliases(self) -> int:
        """
        Recursively count all dicts containing "Z1K1" anywhere in the data.
        """

        def _count(d: Any) -> int:
            if isinstance(d, dict):
                if "Z1K1" in d:
                    return 1
                return sum(_count(v) for v in d.values())
            if isinstance(d, list):
                return sum(_count(i) for i in d)
            return 0

        return _count(self.data)

    @property
    def count_languages(self) -> int:
        """
        Recursively count all language entries, i.e. dicts under "Z12K1" that contain "Z11K2".
        """

        def _count(d: Any) -> int:
            if isinstance(d, dict):
                if "Z12K1" in d and isinstance(d["Z12K1"], list):
                    return sum(
                        1 for i in d["Z12K1"] if isinstance(i, dict) and "Z11K2" in i
                    )
                return sum(_count(v) for v in d.values())
            if isinstance(d, list):
                return sum(_count(i) for i in d)
            return 0

        return _count(self.data)

    @property
    def link(self) -> str:
        return f"{config.BASE_URL}/{self.zid}"
