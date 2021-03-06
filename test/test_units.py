from unittest.mock import Mock

import pytest

from main import main
from controller.tasks import ACCOUNTS, TABLES

START = "2021-11-01"
END = "2021-12-01"


def run(data):
    return main(Mock(get_json=Mock(return_value=data), args=data))


@pytest.mark.parametrize(
    "table",
    TABLES,
)
@pytest.mark.parametrize(
    "ads_account_id",
    ACCOUNTS,
)
@pytest.mark.parametrize(
    ("start", "end"),
    [
        # (None, None),
        (START, END),
    ],
    ids=[
        # "auto",
        "manual",
    ],
)
def test_pipelines(table, ads_account_id, start, end):
    res = run(
        {
            "table": table,
            "ads_account_id": ads_account_id,
            "start": start,
            "end": end,
        }
    )
    assert res["num_processed"] >= 0
    if res["num_processed"] > 0:
        assert res["output_rows"] == res["num_processed"]

@pytest.mark.parametrize(
    "table",
    TABLES,
)
@pytest.mark.parametrize(
    ("start", "end"),
    [
        (None, None),
        (START, END),
    ],
    ids=[
        "auto",
        "manual",
    ],
)
def test_tasks(table, start, end):
    res = run(
        {
            "task": "fb",
            "table": table,
            "start": start,
            "end": end,
        }
    )
    assert res["tasks"] > 0
