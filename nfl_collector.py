# /Users/ryanranft/0/sportsdataverse/sportsdataverse-py/nfl_collector.py

"""
NFL Data Collector - Now saves each dataset as <data_type>.parquet
matching the local directory name. E.g.:
  /raw_data/nfl/nfl_draft_picks/nfl_draft_picks.parquet
"""

import os
import glob
import io
import logging
from typing import Dict, Any, List, Optional
import sportsdataverse

import polars as pl
from dotenv import load_dotenv

import boto3
from botocore.exceptions import ClientError


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
RAW_DATA_DIR = os.getenv("RAW_DATA_DIR", "./raw_data")
os.makedirs(RAW_DATA_DIR, exist_ok=True)

# ------------------------------------------------------------------------------
# Import from your local `nfl_loaders.py` with all your patches
# ------------------------------------------------------------------------------
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

class AWSCollectorConfig:
    """Minimal AWS config class if you want to store the collected data in S3."""
    def __init__(self):
        self.s3_bucket_name = os.getenv("S3_BUCKET_NAME", "my-default-bucket")
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
        Upload a Polars DataFrame to S3 as Parquet, with minimal assumptions.
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


class NFLCollector:
    """
    Collector that loads data from local `nfl_loaders.py` calls
    and saves them 'as-is'. No schema validation or merges.
    """

    def __init__(self, base_dir: str = RAW_DATA_DIR, use_s3: bool = False):
        self.base_dir = base_dir
        self.use_s3 = use_s3
        os.makedirs(self.base_dir, exist_ok=True)

        self.aws_config = AWSCollectorConfig()
        if self.use_s3:
            self.aws_config.init_session()

        # References the local 'nfl_loaders.py' with your fixes
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
            #"nfl_weekly_rosters": (load_nfl_weekly_rosters, {"seasons": list(range(2002, 2024))}),
            #"nfl_schedule":     (load_nfl_schedule,       {"seasons": list(range(1999, 2024))}),
            #"nfl_rosters":      (load_nfl_rosters,        {"seasons": list(range(1920, 2024))}),
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
        saving locally as Parquet (and optionally to S3).
        """
        for data_type, (loader_func, params) in self.data_mapping.items():
            self.collect_and_store_data(data_type, loader_func, params)

    def collect_and_store_data(self, data_type: str, loader_func, params: dict) -> None:
        """
        Generic method: call the loader function with given params,
        store results in:
            raw_data/nfl/<data_type>/<data_type>.parquet
        (where <data_type> is something like 'nfl_draft_picks')
        """
        try:
            logger.info(f"Collecting data for: {data_type}")
            df = loader_func(**params)

            # Convert to polars if it returned a pandas DataFrame
            if not isinstance(df, pl.DataFrame):
                df = pl.DataFrame(df)

            if df.is_empty():
                logger.warning(f"No data returned for {data_type}")
                return

            subdir = os.path.join(self.base_dir, "nfl", data_type)
            os.makedirs(subdir, exist_ok=True)

            local_path = os.path.join(subdir, f"{data_type}.parquet")
            df.write_parquet(local_path)
            logger.info(f"Saved {df.shape[0]} rows locally for {data_type} at {local_path}")

            if self.use_s3:
                s3_key = f"raw_data/nfl/{data_type}/{data_type}.parquet"
                self.aws_config.upload_polars_to_s3(df, s3_key)

        except Exception as e:
            logger.error(f"Error collecting {data_type}: {str(e)}")

    def run(self) -> None:
        """
        Main method to collect everything we want 'as is'
        from the local nfl_loaders.py code.
        """
        self.run_collections()
        logger.info("NFL data collection completed successfully.")


def main():
    """Example usage: python3 nfl_collector.py"""
    collector = NFLCollector(base_dir=RAW_DATA_DIR, use_s3=False)
    collector.run()


if __name__ == "__main__":
    main()