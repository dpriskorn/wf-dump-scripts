from pydantic import BaseModel
import os
import logging
import requests
from datetime import datetime, timezone


class DumpDownloader(BaseModel):
    data_dir: str = "data"

    class Config:
        arbitrary_types_allowed = True

    def __init__(self, **data):
        super().__init__(**data)
        os.makedirs(self.data_dir, exist_ok=True)

    def download_today_dump(self) -> str:
        today = datetime.now(timezone.utc).strftime("%Y%m%d")
        url = f"https://dumps.wikimedia.org/wikifunctionswiki/{today}/wikifunctionswiki-{today}-pages-meta-current.xml.bz2"
        local_file = os.path.join(
            self.data_dir, f"wikifunctionswiki-{today}-pages-meta-current.xml.bz2"
        )
        logging.info(f"Trying to download {url} ...")

        resp = requests.get(url, stream=True)
        if resp.status_code == 200:
            with open(local_file, "wb") as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    f.write(chunk)
            logging.info(f"Downloaded to {local_file}")
            return local_file
        else:
            logging.warning(f"File not found (HTTP {resp.status_code})")
            return ""
