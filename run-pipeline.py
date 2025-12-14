# ./run-pipeline.py
import logging
import time
import config
from pydantic import BaseModel
from models.dump_converter import DumpConverter
from models.dump_downloader import DumpDownloader
from models.z8_calculator import Z8Calculator

logging.basicConfig(level=config.loglevel, format=config.logformat)
logging.getLogger("httpx").setLevel(config.httpx_loglevel)


class Pipeline(BaseModel):
    class Config:
        arbitrary_types_allowed = True

    downloader: DumpDownloader = DumpDownloader()
    converter: DumpConverter = DumpConverter()
    calculator: Z8Calculator | None = None

    async def run_pipeline(self):
        # TODO re-enable dump download
        # dump_file = self.downloader.download_today_dump()
        # if not dump_file:
        #     logging.warning("No dump downloaded, exiting pipeline.")
        #     return

        jsonl_files = self.converter.convert_all()
        if not jsonl_files:
            logging.warning("No JSONL files produced, exiting pipeline.")
            return

        # Process each JSONL file separately
        for jsonl_file in jsonl_files:
            logging.info(f"Processing JSONL file: {jsonl_file}")
            self.calculator = Z8Calculator(jsonl_file=jsonl_file)
            await self.calculator.calculate()
            self.calculator.write_wikitext()


if __name__ == "__main__":
    import asyncio

    start_time = time.time()
    pipeline = Pipeline()
    asyncio.run(pipeline.run_pipeline())
    end_time = time.time()

    elapsed = end_time - start_time
    logging.info(f"Total pipeline run time: {elapsed:.2f} seconds")
