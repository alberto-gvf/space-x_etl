import pytest
import pandas as pd
from src.transformer import read_json, add_date_partition_column, save_parquet, get_cores, get_launches


def test_get_cores(api_data):
    result = get_cores(api_data)
    assert len(result) == 4


def test_get_cores_append_launch_id(api_data):
    result = get_cores(api_data)
    assert 'launch_id' in result.columns


def test_get_launches(api_data):
    result = get_launches(api_data)
    assert len(result) == 2


def test_read_json_success(tmp_path):
    # Create a temporary JSON file
    json_file = tmp_path / 'test.json'
    json_file.write_text('{"key": "value"}')

    # Test read_json function
    result = read_json(str(json_file))
    assert result == {"key": "value"}


def test_read_json_file_not_found():
    result = read_json('nonexistent_file.json')
    assert result is None


def test_read_json_decode_error(tmp_path):
    # Create a temporary JSON file with invalid JSON
    json_file = tmp_path / 'test.json'
    json_file.write_text('{"key": "value"')

    # Test read_json function
    result = read_json(str(json_file))
    assert result is None


def test_add_date_partition_column():
    pdf = pd.DataFrame({'date_utc': ['2023-01-01T00:00:00.000Z', '2023-01-02T00:00:00.000Z']})
    result = add_date_partition_column(pdf)
    assert 'p_creation_date' in result.columns