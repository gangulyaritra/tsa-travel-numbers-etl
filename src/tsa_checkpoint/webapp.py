import pandas as pd
import plotly.graph_objs as go
import streamlit as st
from prophet import Prophet
from prophet.diagnostics import cross_validation, performance_metrics
from prophet.plot import plot_plotly

from tsa_checkpoint.utils import Config
from tsa_checkpoint.utils.base_classes import BaseVariables
from tsa_checkpoint.utils.snowflake_connector import SnowflakeConfig, SnowflakeConnector


# -----------------------------------------------------------------------------
# Utility Functions.
# -----------------------------------------------------------------------------
@st.cache_data(show_spinner=False)
def load_data(connection_params, conf):
    """Load and preprocess data from Snowflake."""
    with st.spinner("Loading Data ....."):
        df = SnowflakeConnector(
            connection_params, conf
        ).extract_dataframe_from_snowflake()
        df["TRAVEL_DATE"] = pd.to_datetime(df["TRAVEL_DATE"])
        df.sort_values("TRAVEL_DATE", inplace=True)
    st.success("Loading Data ..... Successful!")
    return df


def plot_travel_data(df):
    """Plot the TSA travel data using Plotly."""
    fig = go.Figure(
        data=[
            go.Scatter(
                x=df["TRAVEL_DATE"],
                y=df["VALUE"],
                name="passenger_volume",
                line={"color": "teal"},
                opacity=0.8,
            )
        ]
    )
    fig.update_layout(
        title="Time Series data with Rangeslider", xaxis_rangeslider_visible=True
    )
    st.plotly_chart(fig)


def extract_metadata(df):
    """Format Metadata information from the Dataframe."""
    metadata_columns = [
        col for col in df.columns if col not in ["TRAVEL_DATE", "VALUE"]
    ]
    metadata_dict = {
        col: (
            df[col].unique()[0]
            if df[col].nunique() == 1
            else ", ".join(map(str, df[col].unique()))
        )
        for col in metadata_columns
    }
    return pd.DataFrame(
        {
            "Fields": list(metadata_dict.keys()),
            "Metadata": list(metadata_dict.values()),
        }
    )


def covid19_lockdowns():
    """Define and process COVID-19 lockdown periods as holidays for Prophet."""
    lockdowns_data = [
        {"holiday": "lockdown_1", "ds": "2020-03-21", "ds_upper": "2020-06-06"},
        {"holiday": "lockdown_2", "ds": "2020-07-09", "ds_upper": "2020-10-27"},
        {"holiday": "lockdown_3", "ds": "2021-02-13", "ds_upper": "2021-02-17"},
        {"holiday": "lockdown_4", "ds": "2021-05-28", "ds_upper": "2021-06-10"},
    ]
    lockdowns = pd.DataFrame(lockdowns_data)
    for col in ["ds", "ds_upper"]:
        lockdowns[col] = pd.to_datetime(lockdowns[col])
    lockdowns["lower_window"] = 0
    lockdowns["upper_window"] = (lockdowns["ds_upper"] - lockdowns["ds"]).dt.days
    return lockdowns


def forecast_prophet(df, period, lockdowns):
    """Train the Prophet Model, Generate Forecasts, and Evaluate via Cross-Validation."""
    df_train = df[["TRAVEL_DATE", "VALUE"]].rename(
        columns={"TRAVEL_DATE": "ds", "VALUE": "y"}
    )

    # Initialize Prophet with Performance Optimizations.
    m = Prophet(
        seasonality_mode="multiplicative",
        holidays=lockdowns,
        holidays_prior_scale=0.05,
        changepoint_prior_scale=0.05,
        uncertainty_samples=100,
    )
    m.add_country_holidays(country_name="US")
    m.add_seasonality(name="weekly", period=7, fourier_order=3, prior_scale=0.1)

    # Fit the Model.
    m.fit(df_train)

    # Generate forecasts for future periods.
    future = m.make_future_dataframe(periods=period)
    forecast = m.predict(future)

    # Evaluate the Model using Cross-Validation.
    cv_results = cross_validation(
        m,
        initial="730 days",
        period="180 days",
        horizon="365 days",
        parallel="processes",
    )
    cv_metrics = performance_metrics(cv_results)

    return m, forecast, cv_metrics


# -----------------------------------------------------------------------------
# Main Application.
# -----------------------------------------------------------------------------
def main():
    conf = SnowflakeConfig(
        database=f"{Config.DATABASE}_PROD",
        schema=Config.SCHEMA,
        table=Config.TABLE,
        unique_keys=["TRAVEL_DATE"],
    )
    n_years = st.slider("**Years of Prediction:**", 1, 4)
    period = n_years * 365

    st.title("TSA Passenger Volumes Forecasting")

    # Load and display data.
    df = load_data(BaseVariables.conn, conf)

    st.subheader(":blue[TSA Travel Numbers]")
    st.markdown(
        """:blue-background[Passenger travel numbers are updated Monday through Friday by 9 a.m. 
        During holiday weeks, though, they may be slightly delayed.]"""
    )
    st.dataframe(df.tail(7), use_container_width=True, hide_index=True)
    plot_travel_data(df)

    # Display Metadata Information.
    meta_df = extract_metadata(df)
    st.markdown("**Metadata**")
    st.dataframe(meta_df, use_container_width=True, hide_index=True)

    # Forecasting and Cross-Validation.
    lockdowns = covid19_lockdowns()
    m, forecast, _ = forecast_prophet(df, period, lockdowns)

    st.subheader("Forecasting TSA Passenger Volumes")
    st.dataframe(forecast.tail(), use_container_width=True, hide_index=True)

    st.subheader(f"Forecast Plot for {n_years} years")
    fig_forecast = plot_plotly(m, forecast)
    st.plotly_chart(fig_forecast)

    st.subheader("Forecast Components")
    fig1 = m.plot_components(forecast)
    st.write(fig1)


if __name__ == "__main__":
    main()
