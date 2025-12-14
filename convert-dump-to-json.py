import xml.etree.ElementTree as ET
import json
import html
import os
import glob
import logging
import re

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

input_dir = "data"
output_dir = "output"
os.makedirs(output_dir, exist_ok=True)

# MediaWiki namespace
NS = {"mw": "http://www.mediawiki.org/xml/export-0.11/"}

# Regex to match ZIDs like Z12345
zid_pattern = re.compile(r"^Z\d+$")

# How often to log progress
progress_interval = 1000

# Iterate over all .xml files in input_dir
for input_file in glob.glob(os.path.join(input_dir, "*.xml")):
    base_name = os.path.splitext(os.path.basename(input_file))[0]
    output_file = os.path.join(output_dir, f"{base_name}-ZID-and-json-only.jsonl")

    logging.info(f"Processing file: {input_file}")

    # Open JSONL output file
    with open(output_file, "w", encoding="utf-8") as out_f:
        # Parse XML iteratively
        context = ET.iterparse(input_file, events=("start", "end"))
        _, root = next(context)  # get root element

        page_count = 0
        zid_count = 0

        try:
            for event, elem in context:
                if event == "end" and elem.tag.endswith("page"):
                    page_count += 1

                    title_elem = elem.find("./mw:title", NS)
                    revision_elem = elem.find("./mw:revision", NS)
                    text_elem = revision_elem.find("./mw:text", NS) if revision_elem is not None else None

                    if title_elem is not None and text_elem is not None:
                        title = title_elem.text
                        if title and zid_pattern.match(title):  # Only process ZIDs
                            zid_count += 1
                            text = text_elem.text
                            if text:
                                # Unescape HTML entities
                                text_json_str = html.unescape(text)
                                try:
                                    data = json.loads(text_json_str)
                                    # Write each page as a JSON line
                                    out_f.write(json.dumps({"title": title, "data": data}, ensure_ascii=False) + "\n")
                                except json.JSONDecodeError as e:
                                    logging.error(f"Failed to parse JSON for {title} in {input_file}: {e}")
                                    raise RuntimeError("Stopping due to first JSON parsing error") from e

                    # Log progress
                    if page_count % progress_interval == 0:
                        logging.info(f"Processed {page_count} pages, {zid_count} ZIDs so far...")

                    # Clear element to save memory
                    elem.clear()
                    root.clear()
        except Exception as e:
            logging.exception("Parsing stopped due to error")
            break

    logging.info(f"Finished {input_file}: {page_count} total pages, {zid_count} ZIDs saved to {output_file}")
