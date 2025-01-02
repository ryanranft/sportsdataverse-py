"""
NFL Data Loader with AWS Integration
Handles loading, processing, and storing NFL data with AWS services.
Includes logic for master file checks, schema evolution, row-level mismatch detection,
and local subdirectory saving.

NOTE: This version has minimal schemas in NFLSchemaManager so you won't get
"missing required columns" errors if your data has fewer columns than originally expected.
Customize these schemas based on actual columns in your downloaded data.
"""

import os
import glob
import io
import tempfile
import json
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any, Tuple, Set
import logging
import time
from contextlib import contextmanager

# Data processing
import polars as pl
import pandas as pd
import numpy as np
from psycopg2 import pool
from psycopg2.extensions import register_adapter, AsIs

# AWS
import boto3
from botocore.exceptions import ClientError, BotoCoreError
from aws_setup import AWSServiceConfigurator

# Specific sport data
import sportsdataverse as sdv
from dotenv import load_dotenv

# ------------------------------------------------------------------------------
# Load environment variables
# ------------------------------------------------------------------------------
load_dotenv()
base_dir = os.getenv('RAW_DATA_DIR')
if not base_dir:
    raise ValueError("RAW_DATA_DIR environment variable not set")

# ------------------------------------------------------------------------------
# Configure logging
# ------------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ------------------------------------------------------------------------------
# Register numpy types with PostgreSQL
# ------------------------------------------------------------------------------
def adapt_numpy_scalar(numpy_scalar) -> AsIs:
    """Convert numpy scalars to PostgreSQL-friendly format."""
    if isinstance(numpy_scalar, (np.datetime64, pd.Timestamp)):
        return AsIs(f"'{pd.Timestamp(numpy_scalar)}'::timestamp")
    elif isinstance(numpy_scalar, np.bool_):
        return AsIs('true') if numpy_scalar else AsIs('false')
    else:
        return AsIs(str(numpy_scalar))

for numpy_type in [np.int64, np.int32, np.float64, np.float32, np.bool_, np.datetime64]:
    register_adapter(numpy_type, adapt_numpy_scalar)


# ------------------------------------------------------------------------------
# Helper Classes
# ------------------------------------------------------------------------------

class SchemaEvolution:
    """
    Handles schema evolution and tracking.

    NOTE: Polars doesn't have a dedicated 'Schema' object in many releases, so we treat
    schemas as a dict of {column_name: polars.datatypes.DataType}.
    """

    def __init__(self, s3_client, bucket: str):
        self.s3_client = s3_client
        self.bucket = bucket
        self.schema_history_key = "sportsdataverse/schema_history.json"

    def load_schema_history(self) -> Dict:
        """Load schema history from S3 as a dict."""
        try:
            response = self.s3_client.get_object(
                Bucket=self.bucket,
                Key=self.schema_history_key
            )
            return json.loads(response['Body'].read().decode('utf-8'))
        except ClientError:
            return {}

    def save_schema_history(self, history: Dict) -> None:
        """Save schema history back to S3."""
        self.s3_client.put_object(
            Bucket=self.bucket,
            Key=self.schema_history_key,
            Body=json.dumps(history, indent=2).encode('utf-8')
        )

    def evolve_schema(
        self,
        current_schema: Dict[str, pl.datatypes.DataType],
        data_type: str
    ) -> Dict[str, pl.datatypes.DataType]:
        """
        Compare current schema with the stored schema in S3, and evolve if needed.
        """
        history = self.load_schema_history()
        previous_schema = history.get(data_type, {}).get('current_schema')

        if not previous_schema:
            # First time seeing this schema
            history[data_type] = {
                'current_schema': {col: str(dtype) for col, dtype in current_schema.items()},
                'evolution_history': [],
                'last_updated': datetime.now(timezone.utc).isoformat()
            }
            self.save_schema_history(history)
            return current_schema

        # Convert stored strings back to polars dtypes
        previous_schema = {
            col: getattr(pl, dtype_str) for col, dtype_str in previous_schema.items()
        }

        new_columns = set(current_schema.keys()) - set(previous_schema.keys())
        if new_columns:
            # Record schema evolution
            history[data_type]['evolution_history'].append({
                'date': datetime.now(timezone.utc).isoformat(),
                'change_type': 'new_columns',
                'columns': list(new_columns)
            })
            # Update the stored schema
            history[data_type]['current_schema'] = {
                col: str(dtype) for col, dtype in current_schema.items()
            }
            self.save_schema_history(history)

            logger.info(f"Schema evolved for {data_type} with new columns: {new_columns}")

        return current_schema


class NFLSchemaManager:
    """
    Manages NFL data schemas and validation (as dicts).
    -----
    In this minimal version, we keep only the columns you're likely to have,
    so you don't get 'missing required columns' errors. Adjust as needed!
    """

    def __init__(self):
        self._init_schemas()
        self.schema_versions = {}

    def _init_schemas(self):
        """
        Define minimal schemas for each data type.
        Customize these based on actual columns in your data.
        """

        # TEAMS (Example minimal schema)
        self.teams_schema = {
            "team_id": pl.Utf8,
            "team_abbr": pl.Utf8,
            "team_name": pl.Utf8,
        }

        # PLAYERS (Example minimal schema)
        self.players_schema = {
            "game_id": pl.Utf8,
            "player_name": pl.Utf8,
            "position": pl.Utf8,
        }

        # COMBINE (Example minimal)
        self.combine_schema = {
            # If you only have "player_name" for combine, for instance:
            "player_name": pl.Utf8
        }

        # DRAFT PICKS (Example minimal)
        self.draft_picks_schema = {
            "season": pl.Int32,
            "round": pl.Int32,
            "pick": pl.Int32,
            "team": pl.Utf8,
        }

        # OFFICIALS (Example minimal)
        self.officials_schema = {
            "official_name": pl.Utf8,
            "position": pl.Utf8,
        }

        # DEPTH CHARTS (Example minimal)
        self.depth_charts_schema = {
            "season": pl.Int32,
            "player_name": pl.Utf8,
            "position": pl.Utf8,
        }

        # SNAP COUNTS (Example minimal)
        self.snap_counts_schema = {
            "season": pl.Int32,
        }

        # INJURIES (Example minimal)
        self.injuries_schema = {
            "season": pl.Int32
        }

        # SCHEDULE (Example minimal)
        self.schedule_schema = {
            "season": pl.Int32
        }

        # PFR PASS (Example minimal)
        self.pfr_pass_schema = {
            "season": pl.Int32
        }

        # PFR RUSH (Example minimal)
        self.pfr_rush_schema = {
            "season": pl.Int32
        }

        # PFR REC (Example minimal)
        self.pfr_rec_schema = {
            "season": pl.Int32
        }

        # PFR DEF (Example minimal)
        self.pfr_def_schema = {
            "season": pl.Int32
        }

        # PFR WEEKLY PASS (Example minimal)
        self.pfr_weekly_pass_schema = {
            "season": pl.Int32,
            "week": pl.Int32
        }

        # PFR WEEKLY RUSH (Example minimal)
        self.pfr_weekly_rush_schema = {
            "season": pl.Int32,
            "week": pl.Int32
        }

        # PFR WEEKLY REC (Example minimal)
        self.pfr_weekly_rec_schema = {
            "season": pl.Int32,
            "week": pl.Int32
        }

        # PFR WEEKLY DEF (Example minimal)
        self.pfr_weekly_def_schema = {
            "season": pl.Int32,
            "week": pl.Int32
        }

    def get_schema_version(self, data_type: str) -> int:
        """Get current schema version for data type."""
        if data_type not in self.schema_versions:
            self.schema_versions[data_type] = 1
        return self.schema_versions[data_type]

    def validate_schema(
        self,
        df: pl.DataFrame,
        data_type: str
    ) -> Tuple[bool, Optional[str]]:
        """
        Validate a Polars DataFrame against our minimal schema dict.
        """
        try:
            schema_attr = f"{data_type}_schema"
            if not hasattr(self, schema_attr):
                return False, f"No schema defined for data_type: {data_type}"

            schema = getattr(self, schema_attr)
            required_cols = set(schema.keys())
            df_cols = set(df.columns)
            missing_cols = required_cols - df_cols
            if missing_cols:
                return False, f"Missing required columns: {missing_cols}"

            # Attempt casting
            for col, dtype in schema.items():
                if col in df.columns:
                    try:
                        df = df.with_columns(pl.col(col).cast(dtype))
                    except Exception as e:
                        return False, f"Type validation failed for column {col}: {str(e)}"

            return True, None

        except Exception as e:
            return False, f"Schema validation error: {str(e)}"


class DataQualityChecker:
    """Handles data quality checks for NFL data."""

    def __init__(self):
        self.quality_checks = {
            'teams': [
                self.check_required_fields,
                self.check_categorical_values,
                self.check_duplicates
            ],
            'players': [
                self.check_required_fields,
                self.check_categorical_values,
                self.check_duplicates
            ],
            # Add checks for others if you want (pbr, combine, etc.)
        }

    def check_required_fields(self, df: pl.DataFrame, data_type: str) -> List[str]:
        """
        Example required fields. Adjust as needed or remove entirely if you
        want minimal checks.
        """
        required_fields = {
            'teams': ['team_id', 'team_abbr', 'team_name'],
            'players': ['game_id', 'player_name', 'position'],
        }
        missing = [field for field in required_fields.get(data_type, []) if field not in df.columns]
        return [f"Missing required field: {field}" for field in missing]

    def check_categorical_values(self, df: pl.DataFrame, data_type: str) -> List[str]:
        """Example: if you expect 'position' to be certain categories."""
        errors = []
        # Just an example - adjust or remove
        categorical_values = {
            'position': {'QB', 'RB', 'WR', 'TE', 'OL', 'DL', 'LB', 'DB', 'K', 'P'}
        }

        if 'position' in df.columns and data_type in {'players'}:
            invalid = df.filter(~pl.col('position').is_in(categorical_values['position']))
            if invalid.height > 0:
                unique_invalid = invalid.select(pl.col('position').unique()).to_series().to_list()
                errors.append(f"Invalid values in position: {unique_invalid}")

        return errors

    def check_duplicates(self, df: pl.DataFrame, data_type: str) -> List[str]:
        """Example duplicate check."""
        errors = []
        key_fields = {
            'teams': ['team_id'],
            'players': ['player_name']
        }
        if data_type in key_fields:
            duplicate_count = (
                df.groupby(key_fields[data_type])
                  .agg(pl.count('*').alias('count'))
                  .filter(pl.col('count') > 1)
                  .height
            )
            if duplicate_count > 0:
                errors.append(f"Found {duplicate_count} duplicate records")
        return errors

    def run_checks(self, df: pl.DataFrame, data_type: str) -> Tuple[bool, List[str]]:
        """Run all checks for the given data type."""
        all_errors = []
        for check in self.quality_checks.get(data_type, []):
            errors = check(df, data_type)
            all_errors.extend(errors)
        return (len(all_errors) == 0), all_errors


# ------------------------------------------------------------------------------
# Main Manager Class
# ------------------------------------------------------------------------------

class NFLDataManager:
    """
    Manages NFL data loading, processing, and storage.
    Now includes:
    - Subdirectory saving locally for each table.
    - Master file logic in S3.
    - Column-level + row-level mismatch detection.
    - Partial updates to the DB (only new rows).

    This version uses the minimal schemas in NFLSchemaManager,
    so you won't get "missing columns" unless you truly have no overlap.
    """
    def __init__(
            self,
            base_dir: str,
            schema_name: str = "nfl",
            min_connections: int = 1,
            max_connections: int = 10
    ) -> None:
        """Initialize NFL Data Manager with enhancements."""
        self.base_dir = base_dir
        self.schema_name = schema_name
        self.min_connections = min_connections
        self.max_connections = max_connections

        # Initialize AWS services, DB connection pool, data mappings
        self._init_aws_services()
        self._init_connection_pool()
        self._init_data_mappings()

        # Enhanced features
        self.schema_manager = NFLSchemaManager()
        self.schema_evolution = SchemaEvolution(
            self.aws_config.session.client('s3'),
            self.aws_config.s3_bucket_name
        )
        self.data_quality = DataQualityChecker()

        # Cache for partition metadata
        self.partition_cache = {}

        # Ensure local directory
        os.makedirs(self.base_dir, exist_ok=True)

    def _init_aws_services(self) -> None:
        """Initialize AWS services with proper error handling."""
        try:
            self.aws_config = AWSServiceConfigurator()
            self.aws_config.create_session()

            services = {
                'S3': self.aws_config.setup_s3(),
                'Firehose': self.aws_config.setup_firehose(),
                'Athena': self.aws_config.setup_athena(),
                'CloudWatch': self.aws_config.setup_cloudwatch()
            }

            for service, success in services.items():
                if success:
                    logger.info(f"{service} setup successful")
                else:
                    logger.error(f"{service} setup failed")

        except Exception as e:
            logger.error(f"AWS services initialization failed: {str(e)}")
            raise

    def _init_connection_pool(self) -> None:
        """Initialize PostgreSQL connection pool with retry logic."""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                self._connection_pool = pool.SimpleConnectionPool(
                    self.min_connections,
                    self.max_connections,
                    dbname=os.getenv('PGDATABASE'),
                    user=os.getenv('PGUSER'),
                    password=os.getenv('PGPASSWORD'),
                    host=os.getenv('PGHOST'),
                    port=int(os.getenv('PGPORT', 5432))
                )
                logger.info("Database connection pool initialized")
                return
            except Exception as e:
                if attempt == max_retries - 1:
                    logger.error(f"Database pool initialization failed: {str(e)}")
                    raise
                logger.warning(f"Database connection attempt {attempt + 1} failed")
                time.sleep(2 ** attempt)

    def _init_data_mappings(self) -> None:
        """Initialize data -> loader function mappings (Polars-based)."""
        current_year = datetime.now().year

        # Static data loaders
        self.data_mapping: Dict[str, Any] = {
            'teams': (
                sdv.nfl.load_nfl_teams,
                {'return_as_pandas': False}
            ),
            'players': (
                sdv.nfl.load_nfl_players,
                {'return_as_pandas': False}
            ),
            'combine': (
                sdv.nfl.load_nfl_combine,
                {'return_as_pandas': False}
            ),
            'draft_picks': (
                sdv.nfl.load_nfl_draft_picks,
                {'return_as_pandas': False}
            ),
            'officials': (
                sdv.nfl.load_nfl_officials,
                {'return_as_pandas': False}
            ),
        }

        # Season-based data loaders
        self.season_data_mapping: Dict[str, Any] = {
            'rosters': (
                sdv.nfl.load_nfl_rosters,
                {'seasons': range(1920, current_year + 1), 'return_as_pandas': False}
            ),
            'depth_charts': (
                sdv.nfl.load_nfl_depth_charts,
                {'seasons': range(2001, current_year + 1), 'return_as_pandas': False}
            ),
            'snap_counts': (
                sdv.nfl.load_nfl_snap_counts,
                {'seasons': range(2012, current_year + 1), 'return_as_pandas': False}
            ),
            'injuries': (
                sdv.nfl.load_nfl_injuries,
                {'seasons': range(2009, current_year + 1), 'return_as_pandas': False}
            ),
            'schedule': (
                sdv.nfl.load_nfl_schedule,
                {'seasons': range(1999, current_year + 1), 'return_as_pandas': False}
            ),
        }

        # Pro Football Reference data loaders
        self.pfr_mapping: Dict[str, Any] = {
            'pfr_pass': (sdv.nfl.load_nfl_pfr_pass, {'return_as_pandas': False}),
            'pfr_rush': (sdv.nfl.load_nfl_pfr_rush, {'return_as_pandas': False}),
            'pfr_rec':  (sdv.nfl.load_nfl_pfr_rec,  {'return_as_pandas': False}),
            'pfr_def':  (sdv.nfl.load_nfl_pfr_def,  {'return_as_pandas': False}),
        }

        # Weekly Pro Football Reference data loaders
        self.pfr_weekly_mapping: Dict[str, Any] = {
            'pfr_weekly_pass': (
                sdv.nfl.load_nfl_pfr_weekly_pass,
                {'seasons': range(2018, current_year + 1), 'return_as_pandas': False}
            ),
            'pfr_weekly_rush': (
                sdv.nfl.load_nfl_pfr_weekly_rush,
                {'seasons': range(2018, current_year + 1), 'return_as_pandas': False}
            ),
            'pfr_weekly_rec': (
                sdv.nfl.load_nfl_pfr_weekly_rec,
                {'seasons': range(2018, current_year + 1), 'return_as_pandas': False}
            ),
            'pfr_weekly_def': (
                sdv.nfl.load_nfl_pfr_weekly_def,
                {'seasons': range(2018, current_year + 1), 'return_as_pandas': False}
            ),
        }

    @contextmanager
    def get_connection(self):
        """Context manager for database connections."""
        conn = None
        try:
            conn = self._connection_pool.getconn()
            yield conn
        finally:
            if conn:
                self._connection_pool.putconn(conn)

    def _standardize_datatypes(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Convert Pandas DataFrame columns to consistent types for PostgreSQL.
        """
        for col in df.columns:
            try:
                # Handle boolean columns
                if pd.api.types.is_bool_dtype(df[col]):
                    df[col] = df[col].astype(bool)

                # Handle numeric columns
                elif pd.api.types.is_numeric_dtype(df[col]):
                    if df[col].isna().any() or pd.api.types.is_float_dtype(df[col]):
                        df[col] = pd.to_numeric(df[col], errors='coerce').astype('float64')
                    else:
                        df[col] = pd.to_numeric(df[col], errors='coerce').astype('int64')

                # Handle datetime columns
                elif pd.api.types.is_datetime64_any_dtype(df[col]):
                    df[col] = pd.to_datetime(df[col]).dt.tz_localize(None)

                else:
                    # Force to string
                    df[col] = df[col].astype(str)
                    df[col] = df[col].replace({'': None})

            except Exception as e:
                logger.warning(f"Error converting column {col}: {str(e)}. Defaulting to string type.")
                df[col] = df[col].astype(str).replace({'': None})

        return df

    def _create_table_if_not_exists(self, df: pd.DataFrame, table_name: str) -> None:
        """
        Create PostgreSQL table if it doesn't exist, using each column's dtype.
        """
        type_mapping = {
            'object': 'TEXT',
            'int64': 'BIGINT',
            'float64': 'DOUBLE PRECISION',
            'bool': 'BOOLEAN',
            'datetime64[ns]': 'TIMESTAMP',
            'category': 'TEXT'
        }

        columns_sql = []
        for col, dtype in df.dtypes.items():
            sql_type = type_mapping.get(str(dtype), 'TEXT')
            columns_sql.append(f'"{col}" {sql_type}')

        create_table_query = f"""
        CREATE SCHEMA IF NOT EXISTS {self.schema_name};
        CREATE TABLE IF NOT EXISTS {self.schema_name}.{table_name} (
            {', '.join(columns_sql)}
        );
        """

        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(create_table_query)
                conn.commit()

    def _get_local_table_path(self, table_name: str) -> str:
        """
        Return the local directory path for a given table and ensure it exists.
        E.g. /Users/.../raw_data/nfl/<table_name>
        """
        local_table_dir = os.path.join(self.base_dir, "nfl", table_name)
        os.makedirs(local_table_dir, exist_ok=True)
        return local_table_dir

    # --------------------------------------------------------------------------
    # _compare_and_update_master_s3 (row-level mismatch detection)
    # --------------------------------------------------------------------------
    def _compare_and_update_master_s3(
        self,
        new_data: pl.DataFrame,
        table_name: str
    ) -> Tuple[bool, str, Optional[pl.DataFrame], Dict[str, List[dict]]]:
        """
        1) Checks for existing master file on S3 (master.parquet).
        2) If no master, we can create a new one from new_data.
        3) If master exists, compare columns & row-level changes.
        4) Return (True/False, message, updated_master, mismatch_details).
        """
        s3 = self.aws_config.session.client("s3")
        s3_key_master = f"sportsdataverse/raw_data/nfl/{table_name}/master.parquet"

        mismatch_details = {"row_conflicts": []}

        # Try downloading existing master
        try:
            buffer = io.BytesIO()
            s3.download_fileobj(
                Bucket=self.aws_config.s3_bucket_name,
                Key=s3_key_master,
                Fileobj=buffer
            )
            buffer.seek(0)
            master_df = pl.read_parquet(buffer)
            logger.info(f"Existing master found for {table_name}, rows={master_df.height}")
        except ClientError:
            # Master file does not exist
            return True, "No master file found, safe to create a new one.", None, mismatch_details
        except BotoCoreError as e:
            msg = f"Error reading master from S3: {str(e)}"
            logger.error(msg)
            return False, msg, None, mismatch_details

        # 1) Compare columns
        master_cols = set(master_df.columns)
        new_cols = set(new_data.columns)
        extra_cols_in_new = new_cols - master_cols
        missing_cols_in_new = master_cols - new_cols

        if missing_cols_in_new:
            mismatch_msg = (
                f"New data for {table_name} is missing columns from master: {missing_cols_in_new}"
            )
            return False, mismatch_msg, None, mismatch_details

        # If new_data has extra columns, fill them with null in master
        if extra_cols_in_new:
            for c in extra_cols_in_new:
                master_df = master_df.with_column(pl.lit(None).alias(c))

        # Align columns in sorted order
        master_df = master_df.select(sorted(master_df.columns))
        new_data = new_data.select(sorted(new_data.columns))

        # Try casting new_data to master dtypes
        for col in master_df.columns:
            try:
                desired_dtype = master_df.schema[col]
                new_data = new_data.with_columns(pl.col(col).cast(desired_dtype))
            except Exception as e:
                mismatch_msg = (
                    f"Column {col} in new data cannot be cast to master's dtype. "
                    f"Error: {str(e)}"
                )
                return False, mismatch_msg, None, mismatch_details

        # 2) Row-level mismatch detection: if existing rows changed
        unique_cols = self._get_id_columns(new_data)
        if unique_cols:
            # find common IDs
            master_key_df = master_df.select(unique_cols).unique()
            new_key_df = new_data.select(unique_cols).unique()
            common_ids = master_key_df.join(new_key_df, on=unique_cols, how="inner")

            if common_ids.height > 0:
                common_tuples = set(tuple(r) for r in common_ids.rows())

                master_common = master_df.filter(
                    pl.struct(unique_cols).apply(lambda x: tuple(x.values()) in common_tuples)
                )
                new_common = new_data.filter(
                    pl.struct(unique_cols).apply(lambda x: tuple(x.values()) in common_tuples)
                )

                # Sort by unique keys so row i aligns
                sort_order = unique_cols
                master_common = master_common.sort(by=sort_order)
                new_common = new_common.sort(by=sort_order)

                # Compare row by row
                for idx in range(master_common.height):
                    master_row = master_common.row(idx)
                    new_row = new_common.row(idx)
                    columns_ordered = master_common.columns

                    changed_cols = {}
                    for c_idx, c_name in enumerate(columns_ordered):
                        val_master = master_row[c_idx]
                        val_new = new_row[c_idx]
                        if val_master != val_new:
                            changed_cols[c_name] = (val_master, val_new)

                    if changed_cols:
                        key_vals = {}
                        for c in unique_cols:
                            c_index = columns_ordered.index(c)
                            key_vals[c] = master_row[c_index]

                        mismatch_details["row_conflicts"].append({
                            "unique_key": key_vals,
                            "changed_columns": changed_cols,
                        })

                if mismatch_details["row_conflicts"]:
                    conflict_count = len(mismatch_details["row_conflicts"])
                    mismatch_msg = (
                        f"Detected {conflict_count} changed rows for {table_name}."
                    )
                    return False, mismatch_msg, None, mismatch_details

        # Merge them
        updated_master = pl.concat([master_df, new_data], how="vertical").unique()
        return True, "merged", updated_master, mismatch_details

    def _safe_s3_upload(self, buffer: io.BytesIO, key: str) -> bool:
        """
        Helper to upload a BytesIO buffer to S3 with metadata.
        """
        s3 = self.aws_config.session.client("s3")
        buffer.seek(0)
        try:
            s3.upload_fileobj(
                Fileobj=buffer,
                Bucket=self.aws_config.s3_bucket_name,
                Key=key,
                ExtraArgs={
                    'ContentType': 'application/parquet',
                    'Metadata': {
                        'last_modified': datetime.now(timezone.utc).isoformat()
                    }
                }
            )
            logger.info(f"Successfully updated {key} in S3.")
            return True
        except (ClientError, BotoCoreError) as e:
            logger.error(f"Failed to upload {key}: {str(e)}")
            return False

    def _insert_new_rows_to_db(self, new_rows_df: pl.DataFrame, table_name: str) -> None:
        """Insert only new rows into the DB (no truncation)."""
        try:
            pdf = new_rows_df.to_pandas()
            pdf = self._standardize_datatypes(pdf)
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cols_str = ', '.join(f'"{col}"' for col in pdf.columns)
                    placeholders = ', '.join(['%s'] * len(pdf.columns))

                    data_tuples = [tuple(x) for x in pdf.replace({pd.NA: None}).values]
                    insert_query = f"""
                        INSERT INTO {self.schema_name}.{table_name}
                        ({cols_str}) VALUES ({placeholders})
                    """
                    cur.executemany(insert_query, data_tuples)
                conn.commit()
            logger.info(f"Inserted {len(pdf)} new records into {self.schema_name}.{table_name}.")
        except Exception as e:
            logger.error(f"Error inserting new rows into {table_name}: {str(e)}")

    def _get_id_columns(self, df: pl.DataFrame) -> List[str]:
        """
        Identify columns that might serve as a unique key for deduplication.
        Adjust as needed for your data.
        """
        candidate_keys = ["game_id", "player_id"]  # or add 'season', etc.
        return [col for col in candidate_keys if col in df.columns]

    # --------------------------------------------------------------------------
    # Upload Dataframe: merges into Master, or saves separately if mismatch
    # --------------------------------------------------------------------------
    def upload_dataframe(self, df: pl.DataFrame, table_name: str) -> bool:
        """
        Insert Polars DataFrame into Postgres + store in S3 (with "master" logic).
        If mismatch (row-level changes), store new data separately, log conflict,
        and don't overwrite master. Otherwise merge new rows into master + DB.
        """
        try:
            # 1) Schema evolution
            current_schema = df.schema
            evolved_schema = self.schema_evolution.evolve_schema(current_schema, table_name)
            casted_cols = []
            for name, dtype in evolved_schema.items():
                if name in df.columns:
                    casted_cols.append(pl.col(name).cast(dtype))
            if casted_cols:
                df = df.with_columns(casted_cols)

            # 2) Data quality checks
            is_valid, errors = self.data_quality.run_checks(df, table_name)
            if not is_valid:
                logger.error(f"Data quality check failed for {table_name}:")
                for error in errors:
                    logger.error(f"  - {error}")
                return False

            # 3) Compare with S3 master
            can_merge, msg, updated_master, mismatch_details = self._compare_and_update_master_s3(df, table_name)
            if not can_merge:
                # Save new data separately with timestamp
                timestamp_str = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
                separate_key = (
                    f"sportsdataverse/raw_data/nfl/{table_name}/"
                    f"{table_name}_{timestamp_str}.parquet"
                )

                parquet_buffer = io.BytesIO()
                df.write_parquet(parquet_buffer)
                parquet_buffer.seek(0)
                success = self._safe_s3_upload(parquet_buffer, separate_key)

                if success:
                    logger.error(
                        f"Mismatch for {table_name}. The new data was saved to {separate_key}. "
                        f"Reason: {msg}"
                    )
                    if mismatch_details.get("row_conflicts"):
                        for conflict in mismatch_details["row_conflicts"]:
                            logger.error(
                                f"Changed row for key={conflict['unique_key']}, "
                                f"Columns changed={list(conflict['changed_columns'].keys())}"
                            )
                return False

            # If no master or can merge
            if updated_master is None:
                updated_master = df

            # 4) Identify new rows
            unique_cols = self._get_id_columns(df)
            if unique_cols:
                merged_ids = updated_master.select(unique_cols).unique()
                df_ids = df.select(unique_cols).unique()
                new_only_ids = df_ids.join(merged_ids, how="anti", on=unique_cols)

                if new_only_ids.height > 0:
                    new_only_tuples = set(tuple(r) for r in new_only_ids.rows())
                    new_rows_df = df.filter(
                        pl.struct(unique_cols).apply(lambda x: tuple(x.values()) in new_only_tuples)
                    )
                else:
                    new_rows_df = pl.DataFrame()
            else:
                new_rows_df = df

            # 5) Overwrite master if merge is safe
            s3_key_master = f"sportsdataverse/raw_data/nfl/{table_name}/master.parquet"
            master_parquet_buffer = io.BytesIO()
            updated_master.write_parquet(master_parquet_buffer)
            master_parquet_buffer.seek(0)
            if not self._safe_s3_upload(master_parquet_buffer, s3_key_master):
                return False
            logger.info(f"Updated master saved for {table_name}, row count={updated_master.height}")

            # 6) Insert new rows into DB
            if new_rows_df.height > 0:
                self._insert_new_rows_to_db(new_rows_df, table_name)

            # 7) Local copy of updated master
            local_dir = self._get_local_table_path(table_name)
            local_parquet_path = os.path.join(local_dir, f"{table_name}.parquet")
            updated_master.write_parquet(local_parquet_path)
            logger.info(f"Local master parquet saved to {local_parquet_path}")

            # 8) Log the time
            upload_time = datetime.now(timezone.utc).isoformat()
            logger.info(f"New data appended for {table_name} at {upload_time}")

            return True

        except Exception as e:
            logger.error(f"Error uploading data to {table_name}: {str(e)}")
            return False

    # --------------------------------------------------------------------------
    # Partitioned Master Logic
    # --------------------------------------------------------------------------
    def get_partition_config(self, data_type: str) -> List[str]:
        """
        Return a list of columns to use as partition keys in S3 (for partitioned master).
        """
        if data_type == "pbp":
            return ["season"]
        return ["season"]

    def optimize_partition_pruning(self, data_type: str, filters: Dict[str, Any]) -> Set[str]:
        """
        Identify relevant partition paths in S3 based on filter conditions.
        """
        s3 = self.aws_config.session.client('s3')
        base_path = f"sportsdataverse/raw_data/nfl/{data_type}"

        cache_key = f"{data_type}_{hash(frozenset(filters.items()))}"
        if cache_key in self.partition_cache:
            return self.partition_cache[cache_key]

        relevant_partitions = set()
        try:
            paginator = s3.get_paginator('list_objects_v2')
            for page in paginator.paginate(
                Bucket=self.aws_config.s3_bucket_name,
                Prefix=base_path
            ):
                for obj in page.get('Contents', []):
                    partition_path = obj['Key']
                    matches = True
                    for field, value in filters.items():
                        if isinstance(value, (list, range)):
                            pattern = f"{field}=({','.join(map(str, value))})"
                        else:
                            pattern = f"{field}={value}"
                        if pattern not in partition_path:
                            matches = False
                            break
                    if matches:
                        relevant_partitions.add(partition_path)
            self.partition_cache[cache_key] = relevant_partitions
            return relevant_partitions
        except Exception as e:
            logger.error(f"Error in partition pruning: {str(e)}")
            return set()

    def clear_partition_cache(self) -> None:
        """Clear partition metadata cache."""
        self.partition_cache.clear()
        logger.info("Partition cache cleared")

    def append_to_master_file_partitioned(
        self,
        new_data: pl.DataFrame,
        master_key: str,
        partition_cols: List[str],
        data_type: str
    ) -> None:
        """
        Append new_data to a partitioned master file in S3.
        Each partition is a separate Parquet file.
        (No row-level mismatch detection here.)
        """
        try:
            # Validate minimal schema
            is_valid, error_msg = self.schema_manager.validate_schema(new_data, data_type)
            if not is_valid:
                raise ValueError(f"Schema validation failed: {error_msg}")

            s3 = self.aws_config.session.client("s3")
            base_path = os.path.dirname(master_key)

            filter_vals = {
                col: new_data.select(pl.col(col).unique()).to_series().to_list()
                for col in partition_cols
            }
            existing_parts = self.optimize_partition_pruning(data_type, filter_vals)

            # Partition the new data
            partitions = new_data.partition_by(partition_cols, as_dict=True)
            for part_keys, partition_df in partitions.items():
                partition_values = {
                    col: str(val) for col, val in zip(partition_cols, part_keys)
                }
                partition_path = '/'.join(f"{col}={val}" for col, val in partition_values.items())
                partition_key = f"{base_path}/{partition_path}/data.parquet"

                # Merge with existing partition
                if partition_key in existing_parts:
                    try:
                        buffer = io.BytesIO()
                        s3.download_fileobj(
                            Bucket=self.aws_config.s3_bucket_name,
                            Key=partition_key,
                            Fileobj=buffer
                        )
                        buffer.seek(0)
                        existing_df = pl.read_parquet(buffer)

                        id_cols = self._get_id_columns(partition_df)
                        if id_cols:
                            combined = pl.concat([existing_df, partition_df], how="vertical")
                            partition_df = combined.unique(subset=id_cols, keep='last')
                    except Exception as e:
                        logger.warning(f"Error merging existing partition {partition_key}: {str(e)}")

                # Write partition out
                out_buffer = io.BytesIO()
                partition_df.write_parquet(out_buffer, compression="snappy", statistics=True, use_pyarrow=True)
                out_buffer.seek(0)

                # Upload
                s3.upload_fileobj(
                    Fileobj=out_buffer,
                    Bucket=self.aws_config.s3_bucket_name,
                    Key=partition_key,
                    ExtraArgs={
                        'ContentType': 'application/parquet',
                        'Metadata': {
                            'record_count': str(partition_df.height),
                            'last_updated': datetime.now(timezone.utc).isoformat(),
                            'partition_values': json.dumps(partition_values),
                            'schema_version': str(self.schema_manager.get_schema_version(data_type))
                        }
                    }
                )
                logger.info(f"Updated partition {partition_path} with {partition_df.height} records")

        except Exception as e:
            logger.error(f"Error updating partitioned master file: {str(e)}")
            raise

    # --------------------------------------------------------------------------
    # Data Loading Methods
    # --------------------------------------------------------------------------
    def load_static_data(self) -> None:
        """Load static NFL data (teams, players, etc.)."""
        for data_type, (loader_func, params) in self.data_mapping.items():
            try:
                logger.info(f"Loading {data_type} data")
                df = loader_func(**params)

                if df is not None and not df.is_empty():
                    is_valid, errors = self.data_quality.run_checks(df, data_type)
                    if not errors:
                        logger.info(f"Data quality checks passed for {data_type}")
                    else:
                        for error in errors:
                            logger.warning(f"Data quality issue in {data_type}: {error}")

                    table_name = f"nfl_{data_type}"
                    self.upload_dataframe(df, table_name)

                    master_key = f"sportsdataverse/raw_data/nfl/{data_type}/master"
                    partition_cols = self.get_partition_config(data_type)
                    self.append_to_master_file_partitioned(df, master_key, partition_cols, data_type)

            except Exception as e:
                logger.error(f"Error loading {data_type}: {str(e)}")

    def load_season_data(self) -> None:
        """Load season-based data (rosters, injuries, schedule, etc.)."""
        for data_type, (loader_func, params) in self.season_data_mapping.items():
            try:
                logger.info(f"Loading {data_type} data")
                df = loader_func(**params)

                if df is not None and not df.is_empty():
                    is_valid, errors = self.data_quality.run_checks(df, data_type)
                    if not errors:
                        logger.info(f"Data quality checks passed for {data_type}")
                    else:
                        for error in errors:
                            logger.warning(f"Data quality issue in {data_type}: {error}")

                    table_name = f"nfl_{data_type}"
                    self.upload_dataframe(df, table_name)

                    master_key = f"sportsdataverse/raw_data/nfl/{data_type}/master"
                    partition_cols = self.get_partition_config(data_type)
                    self.append_to_master_file_partitioned(df, master_key, partition_cols, data_type)

            except Exception as e:
                logger.error(f"Error loading {data_type}: {str(e)}")

    def load_pfr_data(self) -> None:
        """Load Pro Football Reference data (season-level and weekly)."""
        # Season-level
        for data_type, (loader_func, params) in self.pfr_mapping.items():
            try:
                logger.info(f"Loading {data_type} data")
                df = loader_func(**params)

                if df is not None and not df.is_empty():
                    is_valid, errors = self.data_quality.run_checks(df, data_type)
                    if not errors:
                        logger.info(f"Data quality checks passed for {data_type}")
                    else:
                        for error in errors:
                            logger.warning(f"Data quality issue in {data_type}: {error}")

                    table_name = f"nfl_{data_type}"
                    self.upload_dataframe(df, table_name)

                    master_key = f"sportsdataverse/raw_data/nfl/pfr/{data_type}/master"
                    partition_cols = self.get_partition_config(data_type)
                    self.append_to_master_file_partitioned(df, master_key, partition_cols, data_type)

            except Exception as e:
                logger.error(f"Error loading {data_type}: {str(e)}")

        # Weekly
        for data_type, (loader_func, params) in self.pfr_weekly_mapping.items():
            try:
                logger.info(f"Loading {data_type} data")
                df = loader_func(**params)

                if df is not None and not df.is_empty():
                    is_valid, errors = self.data_quality.run_checks(df, data_type)
                    if not errors:
                        logger.info(f"Data quality checks passed for {data_type}")
                    else:
                        for error in errors:
                            logger.warning(f"Data quality issue in {data_type}: {error}")

                    table_name = f"nfl_{data_type}"
                    self.upload_dataframe(df, table_name)

                    master_key = f"sportsdataverse/raw_data/nfl/pfr/{data_type}/master"
                    partition_cols = self.get_partition_config(data_type)
                    self.append_to_master_file_partitioned(df, master_key, partition_cols, data_type)

            except Exception as e:
                logger.error(f"Error loading {data_type}: {str(e)}")

    def load_pbp_data(self) -> bool:
        """
        Load play-by-play data from local Parquet files in a subdirectory:
        /[RAW_DATA_DIR]/nfl/nfl_pbp_participation
        """
        try:
            nfl_dir = os.path.join(self.base_dir, "nfl")
            pbp_subfolder = "nfl_pbp_participation"
            pbp_dir = os.path.join(nfl_dir, pbp_subfolder)
            os.makedirs(pbp_dir, exist_ok=True)

            pattern = os.path.join(pbp_dir, "nfl_pbp_*.parquet")
            files = sorted(glob.glob(pattern))

            if not files:
                logger.error(f"No PBP parquet files found matching pattern: {pattern}")
                return False

            logger.info(f"Found {len(files)} PBP parquet files to process")

            for file_path in files:
                try:
                    df = pl.read_parquet(file_path)
                    if not df.is_empty():
                        season = os.path.basename(file_path).split('_')[-1].split('.')[0]

                        is_valid, check_errors = self.data_quality.run_checks(df, 'pbp')
                        if not check_errors:
                            logger.info(f"Data quality checks passed for PBP season {season}")
                        else:
                            for error in check_errors:
                                logger.warning(f"Data quality issue in PBP season {season}: {error}")

                        table_name = f"nfl_pbp_{season}"
                        self.upload_dataframe(df, table_name)

                        master_key = "sportsdataverse/raw_data/nfl/pbp/master"
                        partition_cols = self.get_partition_config('pbp')
                        self.append_to_master_file_partitioned(df, master_key, partition_cols, 'pbp')

                        # Send metadata to Firehose
                        metadata = {
                            'sport': 'nfl',
                            'data_type': 'pbp',
                            'season': season,
                            'record_count': df.height,
                            'timestamp': datetime.now(timezone.utc).isoformat(),
                            'quality_check_passed': is_valid,
                            'quality_issues': check_errors if check_errors else None
                        }

                        firehose = self.aws_config.session.client('firehose')
                        firehose.put_record(
                            DeliveryStreamName=self.aws_config.firehose_stream_name,
                            Record={'Data': json.dumps(metadata) + '\n'}
                        )

                except Exception as e:
                    logger.error(f"Error processing {file_path}: {str(e)}")
                    continue

            return True

        except Exception as e:
            logger.error(f"Error processing PBP files: {str(e)}")
            return False

    def create_athena_views(self) -> bool:
        """Create Athena views with partitioning support."""
        try:
            athena = self.aws_config.session.client('athena')
            database = 'sportsdataverse'
            output_location = f's3://{self.aws_config.s3_bucket_name}/athena-results/'

            # Minimal example queries. Ensure underlying tables exist.
            views = {
                'nfl_season_stats': """
                    CREATE OR REPLACE VIEW nfl_season_stats AS
                    SELECT season, COUNT(*) AS total_plays
                    FROM "nfl_pbp"
                    GROUP BY season
                """,
                'nfl_efficiency_stats': """
                    CREATE OR REPLACE VIEW nfl_efficiency_stats AS
                    SELECT season, play_type, COUNT(*) AS play_count
                    FROM "nfl_pbp"
                    GROUP BY season, play_type
                """
            }

            for view_name, query in views.items():
                max_retries = 3
                for attempt in range(max_retries):
                    try:
                        response = athena.start_query_execution(
                            QueryString=query,
                            QueryExecutionContext={'Database': database},
                            ResultConfiguration={
                                'OutputLocation': output_location,
                                'EncryptionConfiguration': {'EncryptionOption': 'SSE_S3'}
                            },
                            WorkGroup='primary'
                        )

                        query_execution_id = response['QueryExecutionId']
                        while True:
                            status = athena.get_query_execution(QueryExecutionId=query_execution_id)['QueryExecution']['Status']['State']
                            if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                                break
                            time.sleep(1)

                        if status == 'SUCCEEDED':
                            logger.info(f"Successfully created Athena view: {view_name}")
                            break
                        else:
                            raise Exception(f"Query failed with status: {status}")

                    except ClientError as e:
                        if attempt == max_retries - 1:
                            logger.error(f"Error creating Athena view {view_name}: {str(e)}")
                            continue
                        logger.warning(f"Athena view creation attempt {attempt + 1} failed")
                        time.sleep(2 ** attempt)

            return True

        except Exception as e:
            logger.error(f"Error creating Athena views: {str(e)}")
            return False

    def close(self) -> None:
        """Clean up resources."""
        try:
            if hasattr(self, '_connection_pool') and self._connection_pool:
                self._connection_pool.closeall()
                logger.info("Database connections closed")
            self.clear_partition_cache()
        except Exception as e:
            logger.error(f"Error during cleanup: {str(e)}")


# ------------------------------------------------------------------------------
# Main Execution
# ------------------------------------------------------------------------------
def main():
    """Main execution function with minimal schemas."""
    manager = None
    try:
        # Initialize NFL Data Manager
        manager = NFLDataManager(base_dir)
        logger.info("Starting NFL data upload process")

        # Load PBP data first
        logger.info("Loading play-by-play data")
        if not manager.load_pbp_data():
            logger.warning("Play-by-play data upload incomplete")

        # Load static data
        logger.info("Loading static data")
        manager.load_static_data()

        # Load season data
        logger.info("Loading season data")
        manager.load_season_data()

        # Load PFR data
        logger.info("Loading Pro Football Reference data")
        manager.load_pfr_data()

        # Create Athena views
        logger.info("Creating Athena views")
        if not manager.create_athena_views():
            logger.warning("Athena views creation incomplete")

        logger.info("NFL data processing completed successfully")

    except Exception as e:
        logger.error(f"Error in main execution: {str(e)}")
        raise
    finally:
        if manager:
            manager.close()


if __name__ == "__main__":
    main()