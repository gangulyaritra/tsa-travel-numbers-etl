from datetime import date, datetime
from unittest.mock import Mock, patch

import pandas as pd
import pytest
from requests.exceptions import HTTPError, Timeout

from tsa_checkpoint.main import TSAETL, DataExtractor, main


@pytest.fixture
def tsa_etl():
    return TSAETL(datetime.now().year)


@pytest.fixture
def sample_html():
    return "<html><body><table><tr><td>Date</td><td>Numbers</td></tr></table></body></html>"


def test_tsa_etl_init(tsa_etl):
    assert tsa_etl.url == "https://www.tsa.gov/travel/passenger-volumes"
    assert tsa_etl.metadata["country"] == "US"
    assert tsa_etl.metadata["frequency"] == "daily"


@patch("requests.get")
def test_tsa_etl_extract_success(mock_get, tsa_etl, sample_html):
    mock_response = Mock()
    mock_response.raise_for_status.return_value = None
    mock_response.content = sample_html
    mock_get.return_value = mock_response

    tsa_etl.extract()

    assert isinstance(tsa_etl.df, pd.DataFrame)
    assert tsa_etl.df.shape[1] >= 2


@pytest.mark.parametrize(
    "exception,exc_class",
    [(Timeout("Timeout error"), Timeout), (HTTPError("HTTP error"), HTTPError)],
)
@patch("requests.get")
def test_tsa_etl_extract_exceptions(mock_get, tsa_etl, exception, exc_class):
    if isinstance(exception, HTTPError):
        mock_response = Mock()
        mock_response.raise_for_status.side_effect = exception
        mock_get.return_value = mock_response
    else:
        mock_get.side_effect = exception

    with pytest.raises(exc_class):
        tsa_etl.extract()


def test_tsa_etl_transform_more_than_two_columns(tsa_etl):
    current_year_str = str(datetime.now().year)
    data = {
        "Date": ["01/01/2022", "01/02/2022"],
        current_year_str: [100, 200],
        "Extra": [999, 888],
    }
    tsa_etl.df = pd.DataFrame(data)

    tsa_etl.transform()

    expected_columns = [
        "TRAVEL_DATE",
        "VALUE",
        "COUNTRY",
        "FREQUENCY",
        "SERIES_ID",
        "SOURCE",
        "UNIT",
    ]
    assert list(tsa_etl.df.columns) == expected_columns
    assert isinstance(tsa_etl.df["TRAVEL_DATE"].iloc[0], (pd.Timestamp, date))


def test_tsa_etl_transform_two_columns(tsa_etl):
    data = {"Date": ["01/01/2022", "01/02/2022"], "Numbers": [100, 200]}
    tsa_etl.df = pd.DataFrame(data)

    tsa_etl.transform()

    expected_columns = [
        "TRAVEL_DATE",
        "VALUE",
        "COUNTRY",
        "FREQUENCY",
        "SERIES_ID",
        "SOURCE",
        "UNIT",
    ]
    assert list(tsa_etl.df.columns) == expected_columns


def test_main(monkeypatch):
    calls = []

    def mock_etl(self):
        calls.append(self.url)

    monkeypatch.setattr(TSAETL, "etl", mock_etl)

    main()

    current_year = datetime.now().year
    start_year = DataExtractor.base_variables.year
    expected_calls = []

    for year in range(start_year, current_year + 1):
        if year == current_year:
            expected_calls.append("https://www.tsa.gov/travel/passenger-volumes")
        else:
            expected_calls.append(
                f"https://www.tsa.gov/travel/passenger-volumes/{year}"
            )

    assert calls == expected_calls
