import json
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio

from models.exceptions import NoTestResultFound
from models.wf.client import Client
from models.wf.enums import TestStatus
from models.wf.zfunction import Zfunction
from models.wf.zimpl import Zimpl
from models.wf.ztester import Ztester


# ----------------------
# Async fixtures
# ----------------------
@pytest_asyncio.fixture
async def client():
    async with Client(concurrency=2, timeout=1) as c:
        yield c


# Load example data from a file
with open("test_data/dump/zimplementation.json") as f:
    zimpl_json = json.load(f)
with open("test_data/dump/ztester.json") as f:
    ztester_json = json.load(f)
with open("test_data/dump/zfunction.json") as f:
    zfunction_json = json.load(f)


@pytest_asyncio.fixture
def example_impl():
    # Construct Zimpl using the full JSON
    return Zimpl(data=zimpl_json)


@pytest_asyncio.fixture
def example_tester():
    return Ztester(data=ztester_json)


@pytest_asyncio.fixture
def example_function(example_impl, example_tester):
    return Zfunction(
        data=zfunction_json, zimplementations=[example_impl], ztesters=[example_tester]
    )


# ----------------------
# Test class
# ----------------------
@pytest.mark.asyncio
class TestClient:

    # ----------------------
    # Context manager / initialization
    # ----------------------
    async def test_client_context_manager(
        self,
    ):
        c = Client(concurrency=1, timeout=0.5)
        assert c.client is None
        async with c as client_inside:
            assert client_inside.client is not None
            assert client_inside.semaphore is not None

    # ----------------------
    # _get()
    # ----------------------
    async def test_get_success(self, client):
        # Mock the response object
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json = lambda: {"query": {"wikilambda_perform_test": []}}
        mock_response.raise_for_status = lambda: None  # do nothing
        client.client.get = AsyncMock(return_value=mock_response)

        result = await client._get({"foo": "bar"})
        assert result == {"query": {"wikilambda_perform_test": []}}

    async def test_get_raises_runtime_error(self):
        c = Client()
        with pytest.raises(RuntimeError):
            await c._get({})

    # ----------------------
    # fetch_test_status()
    # ----------------------
    async def test_fetch_test_status_pass(self, client):
        response_data = {
            "query": {
                "wikilambda_perform_test": [
                    {"validateStatus": '{"Z1K1": "Z40", "Z40K1": "Z41"}'}
                ]
            }
        }
        client._get = AsyncMock(return_value=response_data)

        status = await client.fetch_test_status("Zfunc", "Zimpl", "Ztester")
        assert status == TestStatus.PASS

    async def test_fetch_test_status_fail(self, client):
        response_data = {
            "query": {
                "wikilambda_perform_test": [{"validateStatus": '{"Z1K1": "Z40"}'}]
            }
        }
        client._get = AsyncMock(return_value=response_data)

        status = await client.fetch_test_status("Zfunc", "Zimpl", "Ztester")
        assert status == TestStatus.FAIL

    async def test_fetch_test_status_no_entries(self, client):
        client._get = AsyncMock(return_value={"query": {"wikilambda_perform_test": []}})
        with pytest.raises(NoTestResultFound):
            await client.fetch_test_status("Zfunc", "Zimpl", "Ztester")

    # ----------------------
    # fetch_impl_test_statuses()
    # ----------------------
    async def test_fetch_impl_test_statuses_success(
        self, client, example_impl, example_tester
    ):
        client.fetch_test_status = AsyncMock(return_value=TestStatus.PASS)
        results = await client.fetch_impl_test_statuses(
            "Zfunc", example_impl, [example_tester]
        )
        assert results == {example_tester.zid: TestStatus.PASS}

    async def test_fetch_impl_test_statuses_error(
        self, client, example_impl, example_tester
    ):
        client.fetch_test_status = AsyncMock(side_effect=NoTestResultFound())

        with pytest.raises(NoTestResultFound):
            await client.fetch_impl_test_statuses(
                "Zfunc", example_impl, [example_tester]
            )

    # ----------------------
    # fetch_function_test_status_map()
    # ----------------------
    async def test_fetch_function_test_status_map(self, client, example_function):
        client.fetch_impl_test_statuses = AsyncMock(
            return_value={t.zid: TestStatus.PASS for t in example_function.ztesters}
        )

        result = await client.fetch_function_test_status_map(example_function)
        for impl_zid, tester_map in result.items():
            for tester_zid, status in tester_map.items():
                assert status == TestStatus.PASS
