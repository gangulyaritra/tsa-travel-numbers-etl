import argparse
import os
from datetime import datetime, timedelta

from dopplersdk import DopplerSDK

# Initialize and authenticate the SDK.
doppler = DopplerSDK()
doppler.set_access_token(os.getenv("DOPPLER_SERVICE_TOKEN"))

secrets = doppler.secrets.list(project="tsa", config="prd").secrets


class Config:
    ACCOUNT = secrets.get("ACCOUNT", {}).get("computed")
    USER = secrets.get("USER", {}).get("computed")
    PASSWORD = secrets.get("PASSWORD", {}).get("computed")
    WAREHOUSE = secrets.get("WAREHOUSE", {}).get("computed")
    ROLE = secrets.get("ROLE", {}).get("computed")
    DATABASE = secrets.get("DATABASE", {}).get("computed")
    SCHEMA = secrets.get("SCHEMA", {}).get("computed")
    TABLE = secrets.get("TABLE", {}).get("computed")


def parse_args() -> argparse.Namespace:
    """
    Parse command-line arguments for the ETL process.

    Returns:
        argparse.Namespace: Parsed command-line arguments.
    """

    parser = argparse.ArgumentParser(description="Parameters for ETL")

    parser.add_argument(
        "--environment",
        help="Which environment to run the project in?",
        default="uat",
        choices=["prod", "uat"],
    )
    parser.add_argument(
        "--start_year",
        type=int,
        help="Year to be selected starting from 2019.",
        choices=list(range(2019, datetime.now().year + 1)),
        default=int((datetime.now() - timedelta(days=365)).strftime("%Y")),
    )

    args = parser.parse_args()
    args.sf_prefix = "" if args.environment == "prod" else "uat\\"
    return args


parsed_args = parse_args()
