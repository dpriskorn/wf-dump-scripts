from models.zid_item import ZIDItem

# --- Sample JSON objects ---

sample_data_1 = {
    "Z1K1": "Z2",
    "Z2K1": {
        "Z1K1": "Z6",
        "Z6K1": ["Z100", "Z101"]
    },
    "Z2K2": {
        "Z1K1": "Z8",
        "Z8K1": ["Tester1", "Tester2"]
    },
    "Z2K3": {
        "Z1K1": "Z12",
        "Z12K1": [
            "Z11",
            {"Z1K1": "Z11", "Z11K1": "Z1002", "Z11K2": "English"},
            {"Z1K1": "Z11", "Z11K1": "Z1430", "Z11K2": "German"}
        ]
    }
}

sample_data_2 = {
    "Z2K1": {
        "Z1K1": "Z6",
        "Z6K1": "Z200"
    },
    "Z2K2": {
        "Z1K1": "Z8",
        "Z8K1": "TesterOnly"
    }
}

# --- Tests ---

def test_count_aliases():
    item = ZIDItem(title="Z1000", data=sample_data_1)
    assert item.count_aliases() > 0
    item2 = ZIDItem(title="Z1001", data=sample_data_2)
    assert item2.count_aliases() > 0

def test_count_languages():
    item = ZIDItem(title="Z1000", data=sample_data_1)
    assert item.count_languages() == 2
    item2 = ZIDItem(title="Z1001", data=sample_data_2)
    assert item2.count_languages() == 0

def test_count_implementations():
    item = ZIDItem(title="Z1000", data=sample_data_1)
    assert item.count_implementations() == 2
    item2 = ZIDItem(title="Z1001", data=sample_data_2)
    assert item2.count_implementations() == 1

def test_count_testers():
    item = ZIDItem(title="Z1000", data=sample_data_1)
    assert item.count_testers() == 2
    item2 = ZIDItem(title="Z1001", data=sample_data_2)
    assert item2.count_testers() == 1
