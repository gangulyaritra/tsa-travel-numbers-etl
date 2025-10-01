import logging
from datetime import datetime
from io import StringIO

import pandas as pd
import requests
from bs4 import BeautifulSoup

from tsa_checkpoint.utils.base_classes import DataExtractor


class TSAETL(DataExtractor):
    # Configure logging.
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Creates a logger.
    logger = logging.getLogger("TSAETL")

    def __init__(self, year):
        self.metadata = {
            "country": "US",
            "frequency": "daily",
            "series_id": f"{self.base_variables.prefix}TSA\\passenger_volumes",
            "source": "https://www.tsa.gov/travel/passenger-volumes",
            "unit": "number_of",
        }

        self.url = (
            self.metadata["source"]
            if year == datetime.now().year
            else f"{self.metadata['source']}/{year}"
        )

    def extract(self):
        self.logger.info("Initiating the Data Extraction Method.")

        response = requests.get(self.url, timeout=15)
        response.raise_for_status()

        # Parse the HTML and extract the first table.
        soup = BeautifulSoup(response.content, "html.parser")
        self.df = pd.read_html(StringIO(str(soup)))[0]

    def transform(self):
        self.logger.info("Initiating the Data Transformation Method.")

        self.df["Date"] = pd.to_datetime(self.df["Date"], format="%m/%d/%Y").dt.date

        if len(self.df.columns) > 2:
            self.df = self.df.loc[:, ["Date", str(datetime.now().year)]]
            self.df.rename(
                columns={"Date": "travel_date", str(datetime.now().year): "value"},
                inplace=True,
            )
        else:
            self.df.rename(
                columns={"Date": "travel_date", "Numbers": "value"}, inplace=True
            )

        # Add SID Metadata.
        self.df = self.df.assign(**self.metadata)

        # Uppercase column names for Snowflake compatibility.
        self.df.columns = self.df.columns.str.upper()


def main():
    current_year = datetime.now().year
    start_year = DataExtractor.base_variables.year

    for year in range(start_year, current_year + 1):
        tsa_etl = TSAETL(year)
        tsa_etl.etl()


if __name__ == "__main__":
    main()
