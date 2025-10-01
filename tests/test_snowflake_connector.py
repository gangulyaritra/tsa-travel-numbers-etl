from unittest.mock import patch

import pandas as pd
import pytest
from snowflake.connector import ProgrammingError

from tsa_checkpoint.utils.snowflake_connector import SnowflakeConfig, SnowflakeConnector


@pytest.fixture
def sample_df():
    return pd.DataFrame({"id": [1, 2, 3], "name": ["John", "Jane", "Bob"]})


@pytest.fixture
def snowflake_config():
    return SnowflakeConfig(
        database="test_database",
        schema="test_schema",
        table="test_table",
        unique_keys=["id"],
    )


@pytest.fixture
def snowflake_connector(snowflake_config):
    connection_params = {
        "user": "test_user",
        "password": "test_password",
        "account": "test_account",
    }
    return SnowflakeConnector(connection_params, snowflake_config)


def test_snowflake_create_table(snowflake_connector, sample_df):
    expected_sql = 'CREATE TABLE IF NOT EXISTS test_table ("id" NUMBER, "name" TEXT);'
    sql = snowflake_connector.snowflake_create_table("test_table", sample_df)
    assert sql == expected_sql


@patch("snowflake.connector.connect")
def test_load_dataframe_to_snowflake(mock_connect, snowflake_connector, sample_df):
    mock_ctx = mock_connect.return_value
    mock_cs = mock_ctx.cursor.return_value.__enter__.return_value
    snowflake_connector.load_dataframe_to_snowflake(sample_df)
    mock_connect.assert_called_once()
    assert mock_ctx.cursor.call_count >= 1
    assert mock_cs.execute.call_count >= 1


@patch("snowflake.connector.connect")
def test_load_dataframe_to_snowflake_failure(
    mock_connect, snowflake_connector, sample_df
):
    mock_ctx = mock_connect.return_value
    mock_cs = mock_ctx.cursor.return_value.__enter__.return_value
    mock_cs.execute.side_effect = ProgrammingError("Test Error")

    with pytest.raises(ProgrammingError):
        snowflake_connector.load_dataframe_to_snowflake(sample_df)


@patch("snowflake.connector.connect")
def test_extract_dataframe_from_snowflake(mock_connect, snowflake_connector):
    mock_ctx = mock_connect.return_value
    mock_cs = mock_ctx.cursor.return_value.__enter__.return_value
    expected_df = pd.DataFrame({"id": [1, 2, 3]})
    mock_cs.fetch_pandas_all.return_value = expected_df

    df = snowflake_connector.extract_dataframe_from_snowflake()
    pd.testing.assert_frame_equal(df, expected_df)

    mock_connect.assert_called_once()
    mock_ctx.cursor.assert_called_once()
    mock_cs.execute.assert_called_once()
    mock_ctx.close.assert_called_once()
