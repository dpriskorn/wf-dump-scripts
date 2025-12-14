# ./models/z8_calculator.py
import logging
from typing import List, Dict

from pydantic import BaseModel, Field

import config
from models.statistics.test_status_manager import TestStatusManager
from models.statistics.zmap import ZMap
from models.statistics.zwikiwriter import ZwikiWriter
from models.wf.enums import TestStatus
from models.wf.zfunction import Zfunction
from models.wf.zimpl import Zimpl
from models.wf.ztester import Ztester

logger = logging.getLogger(__name__)


class Z8Calculator(BaseModel):
    # --- Input / Configuration ---
    jsonl_file: str = Field(
        ..., description="Path to the input JSONL file (only one file at a time)."
    )
    progress_interval: int = Field(
        default=1000,
        description="Number of ZFunctions processed before reporting progress.",
    )

    # --- Attributes to hold intermediate data ---
    zfunctions: List[Zfunction] = Field(
        default_factory=list,
        description="List of ZFunction objects collected from the input file.",
    )
    test_status_map: Dict[str, Dict[str, TestStatus]] = Field(
        default_factory=dict, description="implementation_zid -> tester_zid -> status"
    )

    class Config:
        arbitrary_types_allowed = True

    def process_functions(self):
        zid_count = 0

        # Build maps
        builder = ZMap(jsonl_file=self.jsonl_file)
        ztester_map = builder.build_map(Ztester, "tester map")
        zimpl_map = builder.build_map(Zimpl, "implementation map")

        logging.info(f"Processing functions from {self.jsonl_file}...")
        processed = 0
        with open(self.jsonl_file, "r", encoding="utf-8") as f:
            for line in f:
                processed += 1
                if processed % self.progress_interval == 0:
                    logging.info(
                        f"Processed {processed} lines, {zid_count} ZFunctions so far..."
                    )
                zf = Zfunction.from_json_line(line)
                if zf.is_correct_type:
                    logger.debug(f"Working on {zf.link}")
                    zf.extract_ztesters(ztester_map)
                    logger.debug(
                        "ZFunction %s has testers: %s",
                        zf.zid,
                        [t.zid for t in zf.ztesters],
                    )
                    zf.extract_zimpl(zimpl_map)
                    logger.debug(
                        "ZFunction %s has implementations: %s",
                        zf.zid,
                        [t.zid for t in zf.zimplementations],
                    )
                    self.zfunctions.append(zf)
                    zid_count += 1

                    if zid_count >= config.MAX_FUNCTIONS:
                        logging.info(
                            f"Reached MAX_FUNCTIONS={config.MAX_FUNCTIONS}, stopping early."
                        )
                        break

        logging.info(f"Collected {zid_count} ZFunctions.")

    async def process_all_z8_and_fetch_test_status_and_write_wikitext(self) -> None:
        """Compute ZFunctions and connected implementations; populate self.zfunctions and self.table."""
        self.process_functions()

        # Test statuses
        status_manager = TestStatusManager(zfunctions=self.zfunctions)
        await status_manager.fetch_statuses_apply_and_write_debug()

        # Wikitext
        writer = ZwikiWriter(zfunctions=self.zfunctions, jsonl_file=self.jsonl_file)
        writer.write_wikitext()
