import pytest
import pandas as pd


@pytest.fixture
def api_data():
    return [
        {
            "id": "1",
            "cores": [
                {"core": "core1"},
                {"core": "core2"}
            ],
            "success": True,
            "flight_number": 1,
            "name": "Test_1",
            "date_local": "2021-12-19T03:58:00.000Z",
            "date_precision": "hour",
            "date_utc": "2021-12-18T22:58:00-05:00",
        },
        {
            "id": "2",
            "cores": [
                {"core": "core1"},
                {"core": "core3"}
            ],
            "success": True,
            "flight_number": 2,
            "name": "Test_2",
            "date_local": "2021-12-19T03:58:00.000Z",
            "date_precision": "hour",
            "date_utc": "2021-12-18T22:58:00-05:00",
        }
    ]
