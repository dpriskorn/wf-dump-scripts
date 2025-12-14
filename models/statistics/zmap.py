import json
import logging
from typing import Dict

from pydantic import BaseModel, Field

import config
from models.wf.zentity import Zentity


class ZMap(BaseModel):
    """Builds maps from JSONL files for ZTesters and ZImplementations."""

    jsonl_file: str = Field(
        ..., description="Path to the input JSONL file (only one file at a time)."
    )
    progress_interval: int = config.log_progress_interval

    def build_map(self, cls, description: str) -> Dict[str, Zentity]:
        result: Dict[str, Zentity] = {}
        processed = 0
        logging.info(f"Building {description} from {self.jsonl_file}...")

        with open(self.jsonl_file, "r", encoding="utf-8") as f:
            for line in f:
                processed += 1
                if processed % self.progress_interval == 0:
                    logging.info(f"Processed {processed} lines for {description}...")
                try:
                    data = json.loads(line)
                except json.JSONDecodeError:
                    continue
                obj = cls(data=data)
                if obj.is_correct_type and obj.zid and obj.zid not in result:
                    result[obj.zid] = obj

        logging.info(f"{description} built with {len(result)} entries.")
        return result
