from typing import Any

from pydantic import BaseModel
import os
import glob
import logging
import re
import xml.etree.ElementTree as ET
import html
import json


class DumpConverter(BaseModel):
    input_dir: str = "data"
    output_dir: str = "output"
    progress_interval: int = 1000
    zid_pattern: re.Pattern = None
    namespace: Any = None

    class Config:
        arbitrary_types_allowed = True

    def __init__(self, **data):
        super().__init__(**data)
        os.makedirs(self.output_dir, exist_ok=True)
        self.zid_pattern = re.compile(r"^Z\d+$")
        self.namespace = {"mw": "http://www.mediawiki.org/xml/export-0.11/"}

    def convert_all(self) -> list[str]:
        output_files = []
        for input_file in glob.glob(os.path.join(self.input_dir, "*.xml*")):
            output_file = self.convert_file(input_file)
            if output_file:
                output_files.append(output_file)
        return output_files

    def convert_file(self, input_file: str) -> str:
        base_name = os.path.splitext(os.path.basename(input_file))[0]
        output_file = os.path.join(
            self.output_dir, f"{base_name}-ZID-and-json-only.jsonl"
        )
        logging.info(f"Processing file: {input_file}")

        try:
            context = ET.iterparse(input_file, events=("start", "end"))
            _, root = next(context)
        except ET.ParseError as e:
            logging.error(f"XML parsing failed for {input_file}: {e}")
            return ""

        page_count = 0
        zid_count = 0

        with open(output_file, "w", encoding="utf-8") as out_f:
            for event, elem in context:
                if event == "end" and elem.tag.endswith("page"):
                    page_count += 1
                    title_elem = elem.find("./mw:title", self.namespace)
                    revision_elem = elem.find("./mw:revision", self.namespace)
                    text_elem = (
                        revision_elem.find("./mw:text", self.namespace)
                        if revision_elem
                        else None
                    )

                    if title_elem is not None and text_elem is not None:
                        title = title_elem.text
                        if title and self.zid_pattern.match(title):
                            zid_count += 1
                            text_json_str = html.unescape(text_elem.text or "")
                            try:
                                data = json.loads(text_json_str)
                                out_f.write(
                                    json.dumps(
                                        data,
                                        ensure_ascii=False,
                                    )
                                    + "\n"
                                )
                            except json.JSONDecodeError as e:
                                logging.error(f"JSON decode error for {title}: {e}")
                                continue

                    if page_count % self.progress_interval == 0:
                        logging.info(
                            f"Processed {page_count} pages, {zid_count} ZIDs so far..."
                        )

                    elem.clear()
                    root.clear()

        logging.info(
            f"Finished {input_file}: {page_count} pages, {zid_count} ZIDs saved to {output_file}"
        )
        return output_file
