from enum import Enum


class TestStatus(str, Enum):
    PASS = "pass"
    FAIL = "fail"


class ZobjectType(str, Enum):
    """See https://www.wikifunctions.org/wiki/Wikifunctions:Reserved_ZIDs#Core_types"""

    TESTER = "Z20"
    IMPLEMENTATION = "Z14"
    FUNCTION = "Z8"
