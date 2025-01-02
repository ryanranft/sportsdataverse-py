"""
NFL Data Collector: Save each dataset to local Parquet, upload to S3, and store in PostgreSQL.
This version explicitly writes all tables to the `nfl` schema in Postgres.
"""

import os
import io
import logging
from typing import Dict, Any, List

import polars as pl
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

# For Postgres connections
from sqlalchemy import create_engine, text

# Import your local loaders
from nfl_loaders import (
    load_nfl_teams,
    load_nfl_players,
    load_nfl_contracts,
    load_nfl_combine,
    load_nfl_draft_picks,
    load_nfl_officials,
    load_nfl_player_stats,
    load_nfl_ngs_passing,
    load_nfl_ngs_rushing,
    load_nfl_ngs_receiving,
    load_nfl_pfr_pass,
    load_nfl_pfr_rush,
    load_nfl_pfr_rec,
    load_nfl_pfr_def,
    load_nfl_pfr_weekly_pass,
    load_nfl_pfr_weekly_rush,
    load_nfl_pfr_weekly_rec,
    load_nfl_pfr_weekly_def,
    load_nfl_schedule,
    load_nfl_rosters,
    load_nfl_snap_counts,
    load_nfl_injuries,
    load_nfl_depth_charts,
    load_nfl_pbp_participation,
    load_nfl_pbp,
    load_nfl_weekly_rosters
)

# ------------------------------------------------------------------------------
# Configure Logging
# ------------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ------------------------------------------------------------------------------
# Load environment variables
# ------------------------------------------------------------------------------
load_dotenv()
RAW_DATA_DIR = os.getenv("RAW_DATA_DIR", "/Users/ryanranft/0/sportsdataverse/sportsdataverse-py/raw_data")
os.makedirs(RAW_DATA_DIR, exist_ok=True)

# S3 Bucket name
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "sportsdataverse")

# ------------------------------------------------------------------------------
# Postgres connection parameters (from environment)
# ------------------------------------------------------------------------------
DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "sportsdataverse")
DB_USER = os.getenv("POSTGRES_USER", "ryanranft")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "")

# Create a SQLAlchemy engine for Postgres
POSTGRES_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(POSTGRES_URL, future=True)

# ------------------------------------------------------------------------------
# AWS Collector / S3 Helper
# ------------------------------------------------------------------------------
class AWSCollectorConfig:
    """Minimal AWS config class to handle S3 upload of Polars DataFrames."""
    def __init__(self):
        self.s3_bucket_name = S3_BUCKET_NAME
        self.session = None

    def init_session(self) -> None:
        try:
            self.session = boto3.Session()
            logger.info("AWS session created successfully")
        except Exception as e:
            logger.error(f"AWS services initialization failed: {str(e)}")
            raise

    def upload_polars_to_s3(self, df: pl.DataFrame, s3_key: str) -> bool:
        """
        Upload a Polars DataFrame to S3 as Parquet.
        """
        if not self.session:
            logger.error("AWS session not initialized")
            return False

        try:
            s3_client = self.session.client("s3")
            parquet_buffer = io.BytesIO()
            df.write_parquet(parquet_buffer)
            parquet_buffer.seek(0)

            s3_client.upload_fileobj(
                Fileobj=parquet_buffer,
                Bucket=self.s3_bucket_name,
                Key=s3_key,
                ExtraArgs={
                    "ContentType": "application/parquet",
                    "Metadata": {
                        "record_count": str(df.height)
                    }
                }
            )
            logger.info(f"Uploaded {df.height} rows to s3://{self.s3_bucket_name}/{s3_key}")
            return True

        except (ClientError, Exception) as e:
            logger.error(f"Error uploading to S3: {str(e)}")
            return False


# ------------------------------------------------------------------------------
# Main Collector Class
# ------------------------------------------------------------------------------
class NFLCollector:
    """
    Collector that loads data from local `nfl_loaders.py` calls
    and saves them 'as-is'.
    This version also:
      - uploads the Parquet files to S3
      - writes the data to Postgres (in the nfl schema)
    """

    def __init__(self, base_dir: str = RAW_DATA_DIR, use_s3: bool = True, use_postgres: bool = True):
        self.base_dir = base_dir
        self.use_s3 = use_s3
        self.use_postgres = use_postgres
        os.makedirs(self.base_dir, exist_ok=True)

        # Initialize AWS config if uploading to S3
        self.aws_config = AWSCollectorConfig()
        if self.use_s3:
            self.aws_config.init_session()

        # data_mapping: which loaders to call, with any needed parameters
        self.data_mapping = {
            "nfl_teams":        (load_nfl_teams,         {}),
            "nfl_players":      (load_nfl_players,       {}),
            "nfl_contracts":    (load_nfl_contracts,     {}),
            "nfl_combine":      (load_nfl_combine,       {}),
            "nfl_draft_picks":  (load_nfl_draft_picks,   {}),
            "nfl_officials":    (load_nfl_officials,     {}),
            "nfl_player_stats": (load_nfl_player_stats,  {"kicking": False}),

            "nfl_ngs_passing":  (load_nfl_ngs_passing,   {}),
            "nfl_ngs_rushing":  (load_nfl_ngs_rushing,   {}),
            "nfl_ngs_receiving":(load_nfl_ngs_receiving, {}),

            # PFR Season-level
            "nfl_pfr_pass":     (load_nfl_pfr_pass,       {}),
            "nfl_pfr_rush":     (load_nfl_pfr_rush,       {}),
            "nfl_pfr_rec":      (load_nfl_pfr_rec,        {}),
            "nfl_pfr_def":      (load_nfl_pfr_def,        {}),

            # Season-based data
            "nfl_pbp":          (load_nfl_pbp,            {"seasons": list(range(1999, 2024))}),
            "nfl_schedule":     (load_nfl_schedule,       {"seasons": list(range(1999, 2024))}),
            "nfl_weekly_rosters": (load_nfl_weekly_rosters, {"seasons": list(range(2002, 2024))}),
            "nfl_rosters":      (load_nfl_rosters,        {"seasons": list(range(1920, 2024))}),
            "nfl_snap_counts":  (load_nfl_snap_counts,    {"seasons": list(range(2012, 2024))}),
            "nfl_injuries":     (load_nfl_injuries,       {"seasons": list(range(2009, 2024))}),
            "nfl_depth_charts": (load_nfl_depth_charts,   {"seasons": list(range(2001, 2024))}),
            "nfl_pbp_participation": (load_nfl_pbp_participation, {"seasons": list(range(2016, 2024))}),

            # PFR Weekly
            "nfl_pfr_weekly_pass": (load_nfl_pfr_weekly_pass, {"seasons": list(range(2018, 2024))}),
            "nfl_pfr_weekly_rush": (load_nfl_pfr_weekly_rush, {"seasons": list(range(2018, 2024))}),
            "nfl_pfr_weekly_rec":  (load_nfl_pfr_weekly_rec,  {"seasons": list(range(2018, 2024))}),
            "nfl_pfr_weekly_def":  (load_nfl_pfr_weekly_def,  {"seasons": list(range(2018, 2024))}),
        }

    def run_collections(self) -> None:
        """
        Loop over each data mapping and collect it as-is,
        saving locally as Parquet, optionally uploading to S3,
        and optionally writing to Postgres (in the nfl schema).
        """
        # Ensure the "nfl" schema exists in Postgres if use_postgres is True
        if self.use_postgres:
            self.create_nfl_schema()

        for data_type, (loader_func, params) in self.data_mapping.items():
            self.collect_and_store_data(data_type, loader_func, params)

    def collect_and_store_data(self, data_type: str, loader_func, params: dict) -> None:
        """
        - Call the loader function with given params.
        - Store results in:  raw_data/nfl/<data_type>/<data_type>.parquet
        - Optionally upload that parquet to S3.
        - Optionally load the data into Postgres in schema nfl.<table_name>.
        """
        try:
            logger.info(f"Collecting data for: {data_type}")
            df = loader_func(**params)  # May return a Polars or Pandas DataFrame

            # Convert to polars if it returned a pandas DataFrame
            if not isinstance(df, pl.DataFrame):
                df = pl.DataFrame(df)

            if df.is_empty():
                logger.warning(f"No data returned for {data_type}")
                return

            # 1) Save Locally as Parquet
            subdir = os.path.join(self.base_dir, "nfl", data_type)
            os.makedirs(subdir, exist_ok=True)
            local_path = os.path.join(subdir, f"{data_type}.parquet")

            df.write_parquet(local_path)
            logger.info(f"Saved {df.shape[0]} rows locally for {data_type} at {local_path}")

            # 2) Upload to S3
            if self.use_s3:
                s3_key = f"raw_data/nfl/{data_type}/{data_type}.parquet"
                self.aws_config.upload_polars_to_s3(df, s3_key)

            # 3) Write to Postgres
            if self.use_postgres:
                self.save_to_postgres(df, data_type)

        except Exception as e:
            logger.error(f"Error collecting {data_type}: {str(e)}")

    def create_nfl_schema(self):
        """
        Ensure the 'nfl' schema exists in Postgres before writing any tables there.
        """
        try:
            with engine.begin() as conn:
                conn.execute(text("CREATE SCHEMA IF NOT EXISTS nfl"))
            logger.info("Verified/created 'nfl' schema in Postgres.")
        except Exception as e:
            logger.error(f"Error creating nfl schema: {str(e)}")

    def save_to_postgres(self, df: pl.DataFrame, table_name: str):
        """
        Writes the Polars DataFrame to a Postgres table named nfl.<table_name>.
        Replaces the table each time to avoid "UndefinedColumn" issues.
        Also drops/rewrites columns that can't be adapted (like numpy arrays).
        """
        try:
            # Convert Polars -> Pandas
            pd_df = df.to_pandas(use_pyarrow_extension_array=True)

            # Example 1: Remove or rename columns that break insertion
            # if "cols" in pd_df.columns:
            #     pd_df = pd_df.drop(columns=["cols"])

            # Example 2: Convert array fields to JSON
            import json
            if "cols" in pd_df.columns:
                pd_df["cols"] = pd_df["cols"].apply(
                    lambda arr: json.dumps(arr.tolist()) if hasattr(arr, "tolist") else json.dumps(arr)
                )

            with engine.begin() as conn:
                pd_df.to_sql(
                    name=table_name,
                    schema="nfl",
                    con=conn,
                    if_exists="replace",  # <--- Re-create the table each time
                    index=False
                )
            logger.info(f"Inserted {len(pd_df)} rows into Postgres table nfl.{table_name}.")

        except Exception as e:
            logger.error(f"Error writing nfl.{table_name} to Postgres: {str(e)}")

    def run(self) -> None:
        """
        Main method to collect everything 'as is' from nfl_loaders.py,
        then optionally store in local, S3, and Postgres (nfl schema).
        """
        self.run_collections()
        logger.info("NFL data collection completed successfully.")


def main():
    """
    Example usage:
    python3 nfl_collector_s3_pg.py
    or you can set environment variables for S3 and Postgres config first:
      export S3_BUCKET_NAME=my-bucket
      export POSTGRES_HOST=localhost
      ...
    """
    use_s3_env = os.getenv("USE_S3", "True").lower() in ("true", "1", "yes")
    use_postgres_env = os.getenv("USE_POSTGRES", "True").lower() in ("true", "1", "yes")

    collector = NFLCollector(
        base_dir=RAW_DATA_DIR,
        use_s3=use_s3_env,
        use_postgres=use_postgres_env
    )
    collector.run()


if __name__ == "__main__":
    main()