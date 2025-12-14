# ./models/zentity.py
import json
import logging
from typing import Any
from pydantic import BaseModel

from models.exceptions import NoZidFound

logger = logging.getLogger(__name__)


class Zentity(BaseModel):
    data: Any  # raw JSON

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
