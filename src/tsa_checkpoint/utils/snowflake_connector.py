import textwrap
from dataclasses import dataclass
from typing import Any, Dict, List

import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas


@dataclass
class SnowflakeConfig:
    database: str
    schema: str
    table: str
    unique_keys: List[str]


class SnowflakeConnector:
    def __init__(self, connection_params: Dict[str, Any], config: SnowflakeConfig):
        self.connection_params = connection_params
        self.config = config

    @staticmethod
    def snowflake_create_table(table_name: str, df: pd.DataFrame) -> str:
        """
        Generate a CREATE TABLE statement for Snowflake based on DataFrame dtypes.
        """
        # Mapping from pandas dtype to Snowflake SQL types.
        dtype_mapping = {
            "object": "TEXT",
            "int64": "NUMBER",
            "float64": "FLOAT",
            "bool": "BOOLEAN",
            "datetime64[ns]": "TIMESTAMP_NTZ",
        }
        columns = [
            f'"{col}" {dtype_mapping.get(str(dtype), "TEXT")}'
            for col, dtype in df.dtypes.items()
        ]
        columns_sql = ", ".join(columns)
        return f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_sql});"

    def load_dataframe_to_snowflake(self, df: pd.DataFrame) -> None:
        """
        Load a Pandas DataFrame into Snowflake, creating required structures and inserting records.
        """
        ctx = snowflake.connector.connect(**self.connection_params)
        try:
            with ctx.cursor() as cs:
                # Ensure that the database and schema exist and select them.
                cs.execute(f"CREATE DATABASE IF NOT EXISTS {self.config.database}")
                cs.execute(f"USE DATABASE {self.config.database}")
                cs.execute(f"CREATE SCHEMA IF NOT EXISTS {self.config.schema}")
                cs.execute(f"USE SCHEMA {self.config.schema}")

                # Create the target table if it doesn't exist.
                cs.execute(self.snowflake_create_table(self.config.table, df))

                # Create a temporary staging table with the same structure.
                staging_table = f"{self.config.table}_staging"
                cs.execute(
                    f"CREATE OR REPLACE TEMPORARY TABLE {staging_table} LIKE {self.config.table}"
                )

                # Load the DataFrame data into the staging table.
                success, _, nrows, _ = write_pandas(
                    ctx, df, staging_table, quote_identifiers=False
                )
                if not success:
                    raise RuntimeError(
                        "Failed to write DataFrame to the staging table."
                    )

                # Build the MERGE statement using dynamic conditions.
                merge_condition = " AND ".join(
                    f'target."{col}" = source."{col}"'
                    for col in self.config.unique_keys
                )
                columns_list = ", ".join(f'"{col}"' for col in df.columns)
                source_columns_list = ", ".join(f'source."{col}"' for col in df.columns)

                merge_stmt = textwrap.dedent(
                    f"""
                    MERGE INTO {self.config.table} AS target
                    USING {staging_table} AS source
                    ON {merge_condition}
                    WHEN NOT MATCHED THEN
                    INSERT ({columns_list})
                    VALUES ({source_columns_list});
                """
                )
                cs.execute(merge_stmt)
                ctx.commit()
                print(
                    f"Inserted {nrows} rows into "
                    f"{self.config.database}.{self.config.schema}.{self.config.table}"
                )
        finally:
            ctx.close()

    def extract_dataframe_from_snowflake(self) -> pd.DataFrame:
        """
        Extract data from Snowflake and return it as a Pandas DataFrame.
        """
        self.connection_params.update(
            {"database": self.config.database, "schema": self.config.schema}
        )
        ctx = snowflake.connector.connect(**self.connection_params)
        try:
            with ctx.cursor() as cs:
                cs.execute(f"SELECT * FROM {self.config.table};")
                df = cs.fetch_pandas_all()
            return df
        finally:
            ctx.close()
