import logging
import os
import re
from datetime import datetime
from typing import List

from pydantic import Field, BaseModel

from config import output_file_prefix
from models.exceptions import DateError
from models.wf.enums import TestStatus
from models.wf.zfunction import Zfunction

logger = logging.getLogger(__name__)


class ZwikiWriter(BaseModel):
    """Writes wikitext tables and summary statistics."""

    jsonl_file: str = Field(
        ..., description="Path to the input JSONL file (only one file at a time)."
    )
    zfunctions: List[Zfunction] = Field(
        ...,
        description="List of ZFunction objects collected from the input file.",
    )
    last_update: str = Field(default="", description="Timestamp of the last update.")

    def extract_date(self):
        # Extract date from filename
        basename = os.path.basename(self.jsonl_file)
        match = re.search(r"(\d{8})", basename)
        if match:
            date_str = match.group(1)
            try:
                self.last_update = datetime.strptime(date_str, "%Y%m%d").strftime(
                    "%Y-%m-%d"
                )
            except ValueError:
                raise DateError()
        else:
            raise DateError()

    def write_wikitext(self):
        self.extract_date()
        self._write_zids_file(f"{output_file_prefix}-1-9999.txt", 1, 9999)
        self._write_zids_file(f"{output_file_prefix}-10000-19999.txt", 10000, 19999)
        self._write_zids_file(f"{output_file_prefix}-20000+.txt", 20000)
        self.write_summary_statistics()

    def _write_zids_file(self, filename, min_zid: int = 1, max_zid: int = None):
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, "w", encoding="utf-8") as f:
            self._write_table_header(f)
            self._write_table_rows(f, min_zid, max_zid)
            self._write_table_footer(f)

    def write_summary_statistics(self) -> None:
        """Compute summary statistics for all ZFunctions and write to a file."""
        total_implementations = 0
        total_tests = 0
        num_functions = len(self.zfunctions)
        fail_counts = []

        total_pass = total_fail = 0
        deletion_candidates: list[str] = []

        for zf in self.zfunctions:
            total_implementations += zf.number_of_implementations

            pass_count, fail_count, zf_total_tests = self._count_test_status(zf)
            fail_counts.append((fail_count, zf_total_tests))

            total_pass += pass_count
            total_fail += fail_count
            total_tests += zf_total_tests

            # Deletion candidate: no implementations, no tests
            if zf.number_of_implementations == 0 and zf_total_tests == 0:
                deletion_candidates.append(zf.zid)

        mean_implementations = (
            total_implementations / num_functions if num_functions else 0
        )
        mean_tests = total_tests / num_functions if num_functions else 0

        zero_fail = sum(1 for f, _ in fail_counts if f == 0)
        one_fail = sum(1 for f, _ in fail_counts if f == 1)
        two_fail = sum(1 for f, _ in fail_counts if f == 2)
        two_or_more_fail = sum(1 for f, _ in fail_counts if f >= 2)
        over_50_percent_fail = sum(
            1 for f, total in fail_counts if total > 0 and f / total >= 0.5
        )
        _100_percent_fail = sum(
            1 for f, total in fail_counts if total > 0 and f / total == 1
        )

        deletion_candidates_percent = round(
            (len(deletion_candidates) * 100) / num_functions
        )

        output_file = "summary.txt"
        with open(f"{output_file_prefix}-{output_file}", "w", encoding="utf-8") as f:
            f.write(f"== Z8 Summary ==\n" f"(last update: {self.last_update})\n\n")

            f.write(f"  Number of functions processed: {num_functions}\n")
            f.write(
                f"  Mean number of implementations per function: "
                f"{mean_implementations:.2f}\n"
            )
            f.write(f"  Mean number of tests per function: {mean_tests:.2f}\n")
            f.write(
                f"  Deletion candidates: {len(deletion_candidates)} ({deletion_candidates_percent}%)\n"
            )

            f.write("\n=== Functions by failed tests count ===\n")

            failed_test_stats = {
                "0 failed tests": zero_fail,
                "1 failed test": one_fail,
                "2 failed tests": two_fail,
                "2+ failed tests": two_or_more_fail,
                ">50% failed tests": over_50_percent_fail,
                "100% failed tests": _100_percent_fail,
            }

            for label, count in failed_test_stats.items():
                percentage = round((count * 100) / num_functions)
                f.write(f"  {label}: {count} ({percentage}%)\n")

            f.write("\n=== Total tests by status ===\n")

            test_status_stats = {
                "Pass": total_pass,
                "Fail": total_fail,
            }

            for label, count in test_status_stats.items():
                percentage = round((count * 100) / total_tests)
                f.write(f"  {label}: {count} ({percentage}%)%\n")

            f.write("== Maintenance candidates ==\n")
            f.write("Deletion candidates (no implementations, no tests):\n")

            if deletion_candidates:
                for zid in deletion_candidates:
                    f.write(f"* [[{zid}]]\n")
            else:
                f.write("  (none)\n")

        logging.info(f"Summary statistics written to {output_file}")

    def _write_table_header(self, f) -> None:
        """Write the wikitext table header."""
        f.write(
            "'''Health Status''': ✅ = all tests pass AND at least one implementation exists, ❌ = otherwise\n\n"
            f"Last update: {self.last_update}\n"
            '{| class="wikitable sortable"\n'
            "! rowspan='2' | Function \n"
            "! rowspan='2' | Aliases \n"
            "! colspan='3' | Connected \n"
            "! rowspan='2' | Translations\n"
            "! rowspan='2' | Health Status\n"  # new column
            "|-\n"
            "! Implementations \n"
            "! Pass / Fail \n"
            "! Total Tests\n"
        )

    def _write_table_rows(self, f, min_zid: int = 1, max_zid: int = None) -> None:
        """Write all rows in the given ZID range."""
        for zf in self.zfunctions:
            zid_number = self._parse_zid_number(zf.zid)
            if zid_number is None:
                continue

            if (min_zid is not None and zid_number < min_zid) or (
                max_zid is not None and zid_number > max_zid
            ):
                continue

            pass_count, fail_count, total_tests = self._count_test_status(zf)

            # Determine health status
            if fail_count == 0 and zf.number_of_implementations > 0:
                health = "✅"
            else:
                health = "❌"

            f.write(
                f"|-\n| [[{zf.zid}]] || {zf.count_aliases} || "
                f"{zf.number_of_implementations} || "
                f"{pass_count} / {fail_count} || "
                f"{total_tests} || {zf.count_languages} || {health}\n"
            )

    @staticmethod
    def _write_table_footer(f) -> None:
        # Add explanation below the table
        f.write(
            "|}\n"
            "Note: Disconnected tests/implementations are not presently in the dump\n\n"
        )

    # ----------------- Static helpers --------------
    @staticmethod
    def _parse_zid_number(zid: str) -> int | None:
        """Convert a ZID string like 'Z27327' to an integer."""
        try:
            return int(zid.lstrip("Z"))
        except ValueError:
            return None

    @staticmethod
    def _count_test_status(zf: Zfunction) -> tuple[int, int, int]:
        """Return pass_count, fail_count, total_tests for a ZFunction."""
        pass_count = fail_count = total_tests = 0

        for impl in zf.zimplementations:
            impl_results = getattr(impl, "test_results", {})
            for status in impl_results.values():
                total_tests += 1
                if status == TestStatus.PASS:
                    pass_count += 1
                elif status == TestStatus.FAIL:
                    fail_count += 1

        return pass_count, fail_count, total_tests
