import pandas as pd

from tsa_checkpoint.utils import Config, parsed_args
from tsa_checkpoint.utils.snowflake_connector import SnowflakeConfig, SnowflakeConnector


class BaseVariables:
    arguments = parsed_args
    environment = arguments.environment
    prefix = arguments.sf_prefix
    year = arguments.start_year
    conn = {
        "account": Config.ACCOUNT,
        "user": Config.USER,
        "password": Config.PASSWORD,
        "warehouse": Config.WAREHOUSE,
        "role": Config.ROLE,
    }
    database = Config.DATABASE
    schema = Config.SCHEMA
    table = Config.TABLE


class DataExtractor:
    base_variables = BaseVariables()
    df = None

    def extract(self) -> pd.DataFrame:
        """Extracts Data from the Source URL."""
        raise NotImplementedError("Extract Method Not Implemented.")

    def transform(self) -> pd.DataFrame:
        """Transforms Data to feed into Snowflake."""
        raise NotImplementedError("Transform Method Not Implemented.")

    def load(self):
        """Load the transformed DataFrame into Snowflake."""
        conf = SnowflakeConfig(
            database=f"{self.base_variables.database}_{self.base_variables.environment.upper()}",
            schema=self.base_variables.schema,
            table=self.base_variables.table,
            unique_keys=["TRAVEL_DATE"],
        )
        snow = SnowflakeConnector(self.base_variables.conn, conf)
        snow.load_dataframe_to_snowflake(self.df)

    def etl(self):
        """Run the ETL process: Extract, Transform, Load."""
        try:
            self.extract()
        except Exception as err:
            raise RuntimeError(
                f"Scraper failed at Extraction. Error was {err}"
            ) from err
        try:
            self.transform()
        except Exception as err:
            raise RuntimeError(
                f"Scraper failed at Transformation. Error was {err}"
            ) from err
        try:
            self.load()
        except Exception as err:
            raise RuntimeError(f"Scraper failed at Upload. Error was {err}") from err
