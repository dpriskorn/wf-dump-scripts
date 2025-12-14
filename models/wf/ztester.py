# ./models/ztester.py
from models.wf.enums import ZobjectType
from models.wf.zentity import Zentity


class Ztester(Zentity):
    EXPECTED_TYPE: ZobjectType = ZobjectType.TESTER
