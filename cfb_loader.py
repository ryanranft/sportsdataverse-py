"""
CFB Data Loader with AWS Integration
Now includes immediate drop of any "count" columns after data load,
plus final rename logic for safety if you'd rather rename "count".
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
import random

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
# Example forced types dictionary to unify known conflicting columns
# ------------------------------------------------------------------------------
FORCED_TYPES = {
    "year": pl.Int32,
    "notes": pl.Utf8,
    "away_post_win_prob": pl.Float64,
    "attendance": pl.Int32,
    # Add more if you find repeated type conflicts in logs
}

# ------------------------------------------------------------------------------
# Desired columns (Optional). List the subset you actually need
# ------------------------------------------------------------------------------
DESIRED_COLUMNS = None  # e.g. ["season", "game_id", "play_id", ...]

# ------------------------------------------------------------------------------
# Helper: rename all "count" columns to "count_plays" (if you prefer rename).
# In this version, we'll drop them at load. But we keep this if you'd like rename instead.
# ------------------------------------------------------------------------------
def rename_count_cols(df: pl.DataFrame) -> pl.DataFrame:
    """
    Renames "count" -> "count_plays" repeatedly if multiple appear.
    """
    while "count" in df.columns:
        df = df.rename({"count": "count_plays"})
    return df

# ------------------------------------------------------------------------------
# Helper: drop all "count" columns
# ------------------------------------------------------------------------------
def drop_count_cols(df: pl.DataFrame) -> pl.DataFrame:
    """
    Drops every column literally named "count" and common variants.
    Also handles columns that end with these terms.
    """
    count_variants = ["count", "Count", "COUNT", "_count", ".count"]
    if df is None:
        return df

    cols_to_drop = []
    for col in df.columns:
        if any(col.endswith(v) for v in count_variants) or any(v in col.lower() for v in count_variants):
            cols_to_drop.append(col)

    if cols_to_drop:
        df = df.drop(cols_to_drop)
        logger.info(f"Dropped {len(cols_to_drop)} count-related columns: {cols_to_drop}")  # Changed to INFO level

    return df


def safe_project(df: pl.DataFrame, cols: List[str]) -> pl.DataFrame:
    """Safe projection that ensures no duplicate column names and handles count columns."""
    # Log columns before operation
    logger.debug(f"Columns before projection: {df.columns}")

    # Drop any count columns first
    df = drop_count_cols(df)
    logger.debug(f"Columns after count drop: {df.columns}")

    # Get unique column names while preserving order
    seen = set()
    unique_cols = []
    for col in cols:
        if col not in seen and not any(v in col.lower() for v in ["count", "Count", "COUNT"]):
            seen.add(col)
            unique_cols.append(col)

    # Ensure all columns exist
    existing_cols = [col for col in unique_cols if col in df.columns]

    return df.select(existing_cols)

# ------------------------------------------------------------------------------
# Helper function to unify columns, handle nested structs, forced types, etc.
# ------------------------------------------------------------------------------
def _unify_column_schema(
    df1: pl.DataFrame,
    df2: pl.DataFrame,
    rename_conflicting_cols: bool = True
) -> Tuple[pl.DataFrame, pl.DataFrame]:
    """
    Ensures df1 and df2 have the same columns and data types:
      1) Drop count columns first
      2) (Optional) limit columns to DESIRED_COLUMNS
      3) add missing columns as None
      4) cast forced types or fallback to string if mismatched
      5) handle struct columns (drop or flatten to JSON)
      6) sort columns
    """
    # First drop any count columns
    df1 = drop_count_cols(df1)
    df2 = drop_count_cols(df2)

    global DESIRED_COLUMNS
    if DESIRED_COLUMNS is not None:
        df1 = safe_project(df1, [c for c in DESIRED_COLUMNS if c in df1.columns])
        df2 = safe_project(df2, [c for c in DESIRED_COLUMNS if c in df2.columns])

    all_cols = set(df1.columns).union(df2.columns)
    missing_in_df1 = all_cols - set(df1.columns)
    missing_in_df2 = all_cols - set(df2.columns)

    if missing_in_df1:
        df1 = df1.with_columns([pl.lit(None).alias(col) for col in missing_in_df1])
    if missing_in_df2:
        df2 = df2.with_columns([pl.lit(None).alias(col) for col in missing_in_df2])

    sorted_cols = sorted(all_cols)

    for col in sorted_cols:
        d1 = df1.schema[col]
        d2 = df2.schema[col]

        # Handle struct columns
        if d1 == pl.Struct or d2 == pl.Struct:
            # Flatten to JSON string
            for df_ in (df1, df2):
                if df_.schema[col] == pl.Struct:
                    df_ = df_.with_columns([
                        pl.col(col).apply(lambda x: json.dumps(x) if x is not None else None).alias(col)
                    ])
                    df_ = df_.with_columns([pl.col(col).cast(pl.Utf8, strict=False)])
            df1, df2 = df_ if df_ is df1 else df1, df_ if df_ is df2 else df2

        if col in FORCED_TYPES:
            forced = FORCED_TYPES[col]
            df1 = df1.with_columns([pl.col(col).cast(forced)])
            df2 = df2.with_columns([pl.col(col).cast(forced)])
        else:
            if d1 != d2:
                df1 = df1.with_columns([pl.col(col).cast(pl.Utf8, strict=False)])
                df2 = df2.with_columns([pl.col(col).cast(pl.Utf8, strict=False)])

    df1 = safe_project(df1, sorted_cols)
    df2 = safe_project(df2, sorted_cols)

    return df1, df2

# ------------------------------------------------------------------------------
# SchemaEvolution Class
# ------------------------------------------------------------------------------
class SchemaEvolution:
    """
    Handles schema evolution and tracking for CFB data.
    """
    def __init__(self, s3_client, bucket: str):
        self.s3_client = s3_client
        self.bucket = bucket
        self.schema_history_key = "sportsdataverse/schema_history_cfb.json"

    def load_schema_history(self) -> Dict:
        try:
            response = self.s3_client.get_object(
                Bucket=self.bucket,
                Key=self.schema_history_key
            )
            return json.loads(response['Body'].read().decode('utf-8'))
        except ClientError:
            return {}

    def save_schema_history(self, history: Dict) -> None:
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
        history = self.load_schema_history()
        previous_schema = history.get(data_type, {}).get('current_schema')

        if not previous_schema:
            history[data_type] = {
                'current_schema': {col: str(dtype) for col, dtype in current_schema.items()},
                'evolution_history': [],
                'last_updated': datetime.now(timezone.utc).isoformat()
            }
            self.save_schema_history(history)
            return current_schema

        # convert stored strings back to polars dtypes
        previous_schema = {
            col: getattr(pl, dtype_str) for col, dtype_str in previous_schema.items()
        }

        new_columns = set(current_schema.keys()) - set(previous_schema.keys())
        if new_columns:
            history[data_type]['evolution_history'].append({
                'date': datetime.now(timezone.utc).isoformat(),
                'change_type': 'new_columns',
                'columns': list(new_columns)
            })
            history[data_type]['current_schema'] = {
                col: str(dtype) for col, dtype in current_schema.items()
            }
            self.save_schema_history(history)

            logger.info(f"Schema evolved for {data_type} with new columns: {new_columns}")

        return current_schema

# ------------------------------------------------------------------------------
# CFBSchemaManager Class
# ------------------------------------------------------------------------------
class CFBSchemaManager:
    """
    Manages CFB data schemas and validation (as dicts).
    """
    def __init__(self):
        self._init_schemas()
        self.schema_versions = {}

    def _init_schemas(self):
        # Minimal example schemas
        self.teams_schema = {
            "team_id": pl.Utf8,
            "school": pl.Utf8,
            "mascot": pl.Utf8,
        }
        self.team_info_schema = {
            "season": pl.Int32,
            "team_id": pl.Utf8,
            "school": pl.Utf8,
        }
        self.betting_lines_schema = {
            "game_id": pl.Utf8,
            "home_team": pl.Utf8,
            "away_team": pl.Utf8,
            "spread": pl.Float64,
            "total": pl.Float64,
        }
        self.rosters_schema = {
            "season": pl.Int32,
            "athlete_id": pl.Utf8,
            "first_name": pl.Utf8,
            "last_name": pl.Utf8,
            "position": pl.Utf8
        }
        self.schedule_schema = {
            "season": pl.Int32,
            "game_id": pl.Utf8,
            "home_team": pl.Utf8,
            "away_team": pl.Utf8
        }
        self.pbp_schema = {
            "season": pl.Int32,
            "game_id": pl.Utf8,
            "drive_id": pl.Int32,
            "play_id": pl.Int32,
            "down": pl.Int32,
            "distance": pl.Int32
        }

    def get_schema_version(self, data_type: str) -> int:
        if data_type not in self.schema_versions:
            self.schema_versions[data_type] = 1
        return self.schema_versions[data_type]

    def validate_schema(self, df: pl.DataFrame, data_type: str) -> Tuple[bool, Optional[str]]:
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

            # Attempt casting to minimal expected types
            for col, dtype in schema.items():
                if col in df.columns:
                    try:
                        df = df.with_columns([pl.col(col).cast(dtype)])
                    except Exception as e:
                        return False, f"Type validation failed for column {col}: {str(e)}"

            return True, None
        except Exception as e:
            return False, f"Schema validation error: {str(e)}"

# ------------------------------------------------------------------------------
# DataQualityChecker Class
# ------------------------------------------------------------------------------
class DataQualityChecker:
    """
    Basic data quality checks for each data type.
    """
    def __init__(self):
        self.quality_checks = {
            'teams': [
                self.check_required_fields,
                self.check_duplicates
            ],
            'team_info': [
                self.check_required_fields,
                self.check_duplicates
            ],
            'betting_lines': [
                self.check_required_fields,
                self.check_duplicates
            ],
            'rosters': [
                self.check_required_fields,
                self.check_duplicates
            ],
            'schedule': [
                self.check_required_fields,
                self.check_duplicates
            ],
            'pbp': [
                self.check_required_fields,
                self.check_duplicates
            ],
        }

    def check_required_fields(self, df: pl.DataFrame, data_type: str) -> List[str]:
        required_fields = {
            'teams': ['team_id', 'school', 'mascot'],
            'team_info': ['team_id', 'season'],
            'betting_lines': ['game_id', 'home_team', 'away_team'],
            'rosters': ['season', 'athlete_id', 'position'],
            'schedule': ['season', 'game_id', 'home_team', 'away_team'],
            'pbp': ['season', 'game_id', 'play_id']
        }
        missing = [
            field for field in required_fields.get(data_type, [])
            if field not in df.columns
        ]
        return [f"Missing required field: {field}" for field in missing]

    def check_duplicates(self, df: pl.DataFrame, data_type: str) -> List[str]:
        errors = []
        key_fields = {
            'teams': ['team_id'],
            'team_info': ['team_id', 'season'],
            'betting_lines': ['game_id'],
            'rosters': ['athlete_id', 'season'],
            'schedule': ['game_id'],
            'pbp': ['game_id', 'play_id']
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
        all_errors = []
        for check in self.quality_checks.get(data_type, []):
            errors = check(df, data_type)
            all_errors.extend(errors)
        return (len(all_errors) == 0), all_errors

# ------------------------------------------------------------------------------
# CFBDataManager Class
# ------------------------------------------------------------------------------
class CFBDataManager:
    """
    Manages CFB data loading, processing, and storage.
    Includes immediate drop of any "count" columns right after loading each dataset.
    """

    def __init__(
            self,
            base_dir: str,
            schema_name: str = "cfb",
            min_connections: int = 1,
            max_connections: int = 10
    ) -> None:
        self.base_dir = base_dir
        self.schema_name = schema_name
        self.min_connections = min_connections
        self.max_connections = max_connections

        # Initialize AWS services, DB connection pool, data mappings
        self._init_aws_services()
        self._init_connection_pool()
        self._init_data_mappings()

        self.schema_manager = CFBSchemaManager()
        self.schema_evolution = SchemaEvolution(
            self.aws_config.session.client('s3'),
            self.aws_config.s3_bucket_name
        )
        self.data_quality = DataQualityChecker()

        self.partition_cache = {}

        os.makedirs(self.base_dir, exist_ok=True)

    def _init_aws_services(self) -> None:
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
        current_year = datetime.now().year
        self.static_data_mapping: Dict[str, Any] = {
            'teams': (
                sdv.cfb.get_cfb_teams,
                {'return_as_pandas': False}
            ),
            'team_info': (
                sdv.cfb.load_cfb_team_info,
                {'seasons': range(2002, current_year + 1), 'return_as_pandas': False}
            ),
            'betting_lines': (
                sdv.cfb.load_cfb_betting_lines,
                {'return_as_pandas': False}
            ),
        }
        self.season_data_mapping: Dict[str, Any] = {
            'rosters': (
                sdv.cfb.load_cfb_rosters,
                {'seasons': range(2004, current_year + 1), 'return_as_pandas': False}
            ),
            'schedule': (
                sdv.cfb.load_cfb_schedule,
                {'seasons': range(2002, current_year + 1), 'return_as_pandas': False}
            ),
        }
        self.pbp_mapping: Dict[str, Any] = {
            'pbp': (
                sdv.cfb.load_cfb_pbp,
                {'seasons': range(2003, current_year + 1), 'return_as_pandas': False}
            )
        }

    @contextmanager
    def get_connection(self):
        conn = None
        try:
            conn = self._connection_pool.getconn()
            yield conn
        finally:
            if conn:
                self._connection_pool.putconn(conn)

    def _standardize_datatypes(self, df: pd.DataFrame) -> pd.DataFrame:
        for col in df.columns:
            try:
                if pd.api.types.is_bool_dtype(df[col]):
                    df[col] = df[col].astype(bool)
                elif pd.api.types.is_numeric_dtype(df[col]):
                    if df[col].isna().any() or pd.api.types.is_float_dtype(df[col]):
                        df[col] = pd.to_numeric(df[col], errors='coerce').astype('float64')
                    else:
                        df[col] = pd.to_numeric(df[col], errors='coerce').astype('int64')
                elif pd.api.types.is_datetime64_any_dtype(df[col]):
                    df[col] = pd.to_datetime(df[col]).dt.tz_localize(None)
                else:
                    df[col] = df[col].astype(str)
                    df[col] = df[col].replace({'': None})
            except Exception as e:
                logger.warning(f"Error converting column {col}: {str(e)}. Defaulting to string type.")
                df[col] = df[col].astype(str).replace({'': None})

        return df

    def _create_table_if_not_exists(self, df: pd.DataFrame, table_name: str) -> None:
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
        local_table_dir = os.path.join(self.base_dir, "cfb", table_name)
        os.makedirs(local_table_dir, exist_ok=True)
        return local_table_dir

    # --------------------------------------------------------------------------
    # Compare and Update Master in S3
    # --------------------------------------------------------------------------
    def _compare_and_update_master_s3(
        self,
        new_data: pl.DataFrame,
        table_name: str
    ) -> Tuple[bool, str, Optional[pl.DataFrame], Dict[str, List[dict]]]:
        s3 = self.aws_config.session.client("s3")
        s3_key_master = f"sportsdataverse/raw_data/cfb/{table_name}/master.parquet"

        mismatch_details = {"row_conflicts": []}
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
            return True, "No master file found, safe to create a new one.", None, mismatch_details
        except BotoCoreError as e:
            msg = f"Error reading master from S3: {str(e)}"
            logger.error(msg)
            return False, msg, None, mismatch_details

        # unify columns
        master_df, new_data = _unify_column_schema(master_df, new_data)

        unique_cols = self._get_id_columns(new_data, table_name)
        if unique_cols:
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
                master_common = master_common.sort(by=unique_cols)
                new_common = new_common.sort(by=unique_cols)

                for idx in range(master_common.height):
                    master_row = master_common.row(idx)
                    new_row = new_common.row(idx)
                    columns_ordered = master_common.columns

                    row_diff = {}
                    for c_idx, c_name in enumerate(columns_ordered):
                        val_master = master_row[c_idx]
                        val_new = new_row[c_idx]
                        if val_master != val_new:
                            row_diff[c_name] = (val_master, val_new)

                    if row_diff:
                        key_vals = {c: master_row[columns_ordered.index(c)] for c in unique_cols}
                        mismatch_details["row_conflicts"].append({
                            "unique_key": key_vals,
                            "changed_columns": row_diff,
                        })

                if mismatch_details["row_conflicts"]:
                    conflict_count = len(mismatch_details["row_conflicts"])
                    mismatch_msg = f"Detected {conflict_count} changed rows for {table_name}."
                    return False, mismatch_msg, None, mismatch_details

        # final concat
        updated_master = pl.concat([master_df, new_data], how="vertical").unique()
        return True, "merged", updated_master, mismatch_details

    def _safe_s3_upload(self, buffer: io.BytesIO, key: str) -> bool:
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
        try:
            pdf = new_rows_df.to_pandas()
            pdf = self._standardize_datatypes(pdf)
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    self._create_table_if_not_exists(pdf, table_name)

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

    def _get_id_columns(self, df: pl.DataFrame, data_type: str) -> List[str]:
        possible_keys = {
            'teams': ["team_id"],
            'team_info': ["team_id", "season"],
            'betting_lines': ["game_id"],
            'rosters': ["athlete_id", "season"],
            'schedule': ["game_id"],
            'pbp': ["game_id", "play_id"]
        }
        return [col for col in possible_keys.get(data_type, []) if col in df.columns]

    def _unify_column_schema(
            df1: pl.DataFrame,
            df2: pl.DataFrame,
            rename_conflicting_cols: bool = True
    ) -> Tuple[pl.DataFrame, pl.DataFrame]:
        # First drop any count columns
        df1 = drop_count_cols(df1)
        df2 = drop_count_cols(df2)

        # Additional check for any count-like columns
        count_cols1 = [col for col in df1.columns if "count" in col.lower()]
        count_cols2 = [col for col in df2.columns if "count" in col.lower()]

        if count_cols1:
            df1 = df1.drop(count_cols1)
            logger.info(f"Dropped additional count-like columns from df1: {count_cols1}")
        if count_cols2:
            df2 = df2.drop(count_cols2)
            logger.info(f"Dropped additional count-like columns from df2: {count_cols2}")

    # --------------------------------------------------------------------------
    # Upload DataFrame + Master Logic
    # --------------------------------------------------------------------------
    def upload_dataframe(self, df: pl.DataFrame, table_name: str) -> bool:
        try:
            # schema evolution
            current_schema = df.schema
            evolved_schema = self.schema_evolution.evolve_schema(current_schema, table_name)
            casted_cols = []
            for name, dtype in evolved_schema.items():
                if name in df.columns:
                    casted_cols.append(pl.col(name).cast(dtype))
            if casted_cols:
                df = df.with_columns(casted_cols)

            # run data checks
            is_valid, errors = self.data_quality.run_checks(df, table_name)
            if not is_valid:
                logger.error(f"Data quality check failed for {table_name}:")
                for error in errors:
                    logger.error(f"  - {error}")
                return False

            # unify/merge with S3 master
            can_merge, msg, updated_master, mismatch_details = self._compare_and_update_master_s3(df, table_name)
            if not can_merge:
                # store separately
                timestamp_str = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
                separate_key = f"sportsdataverse/raw_data/cfb/{table_name}/{table_name}_{timestamp_str}.parquet"
                parquet_buffer = io.BytesIO()
                df.write_parquet(parquet_buffer)
                parquet_buffer.seek(0)
                if self._safe_s3_upload(parquet_buffer, separate_key):
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

            if updated_master is None:
                updated_master = df

            # identify new rows
            unique_cols = self._get_id_columns(df, table_name)
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

            # save master to S3
            s3_key_master = f"sportsdataverse/raw_data/cfb/{table_name}/master.parquet"
            master_parquet_buffer = io.BytesIO()
            updated_master.write_parquet(master_parquet_buffer)
            master_parquet_buffer.seek(0)
            if not self._safe_s3_upload(master_parquet_buffer, s3_key_master):
                return False
            logger.info(f"Updated master saved for {table_name}, row count={updated_master.height}")

            # insert new rows
            if new_rows_df.height > 0:
                self._insert_new_rows_to_db(new_rows_df, table_name)

            # local copy
            local_dir = self._get_local_table_path(table_name)
            local_parquet_path = os.path.join(local_dir, f"{table_name}.parquet")
            updated_master.write_parquet(local_parquet_path)
            logger.info(f"Local master parquet saved to {local_parquet_path}")

            # final log
            upload_time = datetime.now(timezone.utc).isoformat()
            logger.info(f"New data appended for {table_name} at {upload_time}")
            return True
        except Exception as e:
            logger.error(f"Error uploading data to {table_name}: {str(e)}")
            return False

    # --------------------------------------------------------------------------
    # Partitioned Master (Optional)
    # --------------------------------------------------------------------------
    def get_partition_config(self, data_type: str) -> List[str]:
        return ["season"]

    def optimize_partition_pruning(self, data_type: str, filters: Dict[str, Any]) -> Set[str]:
        s3 = self.aws_config.session.client('s3')
        base_path = f"sportsdataverse/raw_data/cfb/{data_type}"
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
        self.partition_cache.clear()
        logger.info("Partition cache cleared")

    def append_to_master_file_partitioned(
        self,
        new_data: pl.DataFrame,
        master_key: str,
        partition_cols: List[str],
        data_type: str
    ) -> None:
        try:
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

            # partition the data
            partitions = new_data.partition_by(partition_cols, as_dict=True)
            for part_keys, partition_df in partitions.items():
                # drop "count" columns if they exist
                partition_df = drop_count_cols(partition_df)

                partition_values = {
                    col: str(val) for col, val in zip(partition_cols, part_keys)
                }
                partition_path = '/'.join(f"{col}={val}" for col, val in partition_values.items())
                partition_key = f"{base_path}/{partition_path}/data.parquet"

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
                        existing_df = drop_count_cols(existing_df)
                        existing_df, partition_df = _unify_column_schema(existing_df, partition_df)
                        id_cols = self._get_id_columns(partition_df, data_type)
                        if id_cols:
                            combined = pl.concat([existing_df, partition_df], how="vertical")
                            partition_df = combined.unique(subset=id_cols, keep='last')
                    except Exception as e:
                        logger.warning(f"Error merging existing partition {partition_key}: {str(e)}")

                out_buffer = io.BytesIO()
                partition_df.write_parquet(out_buffer, compression="snappy", statistics=True, use_pyarrow=True)
                out_buffer.seek(0)

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

    # ------------------------------------------------------------------------------
    # Data Loading Methods
    # ------------------------------------------------------------------------------
    def load_static_data(self) -> None:
        for data_type, (loader_func, params) in self.static_data_mapping.items():
            try:
                logger.info(f"Loading {data_type} data")
                df = loader_func(**params)

                # Immediately drop any count columns after loading
                if df is not None and not df.is_empty():
                    df = drop_count_cols(df)

                    # Example: skip if "season" is missing in team_info
                    if data_type == "team_info" and "season" not in df.columns:
                        logger.warning("No 'season' column in team_info data; skipping this file.")
                        continue

                    is_valid, errors = self.data_quality.run_checks(df, data_type)
                    if not errors:
                        logger.info(f"Data quality checks passed for {data_type}")
                    else:
                        for error in errors:
                            logger.warning(f"Data quality issue in {data_type}: {error}")

                    table_name = f"cfb_{data_type}"
                    self.upload_dataframe(df, table_name)

                    master_key = f"sportsdataverse/raw_data/cfb/{data_type}/master"
                    partition_cols = self.get_partition_config(data_type)
                    self.append_to_master_file_partitioned(df, master_key, partition_cols, data_type)
            except Exception as e:
                logger.error(f"Error loading {data_type}: {str(e)}")

    def load_season_data(self) -> None:
        for data_type, (loader_func, params) in self.season_data_mapping.items():
            try:
                logger.info(f"Loading {data_type} data")
                all_data = pl.DataFrame()
                seasons = params.get('seasons', [])
                for season in seasons:
                    try:
                        i_data = loader_func(seasons=season, return_as_pandas=False)
                        # Drop count columns immediately
                        if i_data is not None and not i_data.is_empty():
                            i_data = drop_count_cols(i_data)

                        if all_data.is_empty():
                            all_data = i_data
                        else:
                            all_data, i_data = _unify_column_schema(all_data, i_data)
                            all_data = pl.concat([all_data, i_data], how="vertical")
                    except Exception as season_ex:
                        logger.warning(f"Skipping season {season} for {data_type}: {season_ex}")

                if not all_data.is_empty():
                    # Drop count columns one final time before proceeding
                    all_data = drop_count_cols(all_data)

                    is_valid, errors = self.data_quality.run_checks(all_data, data_type)
                    if not errors:
                        logger.info(f"Data quality checks passed for {data_type}")
                    else:
                        for error in errors:
                            logger.warning(f"Data quality issue in {data_type}: {error}")

                    table_name = f"cfb_{data_type}"
                    self.upload_dataframe(all_data, table_name)

                    master_key = f"sportsdataverse/raw_data/cfb/{data_type}/master"
                    partition_cols = self.get_partition_config(data_type)
                    self.append_to_master_file_partitioned(all_data, master_key, partition_cols, data_type)
            except Exception as e:
                logger.error(f"Error loading {data_type}: {str(e)}")

    def load_pbp_data(self) -> None:
        for data_type, (loader_func, params) in self.pbp_mapping.items():
            try:
                logger.info(f"Loading {data_type} data")
                all_data = pl.DataFrame()
                seasons = params.get('seasons', [])
                for season in seasons:
                    try:
                        i_data = loader_func(seasons=season, return_as_pandas=False)
                        # Drop count columns immediately and aggressively
                        if i_data is not None and not i_data.is_empty():
                            i_data = drop_count_cols(i_data)
                            # Double check for any count-like columns
                            count_cols = [col for col in i_data.columns if "count" in col.lower()]
                            if count_cols:
                                i_data = i_data.drop(count_cols)
                                logger.info(f"Dropped additional count-like columns: {count_cols}")

                        if all_data.is_empty():
                            all_data = i_data
                        else:
                            all_data, i_data = _unify_column_schema(all_data, i_data)
                            all_data = pl.concat([all_data, i_data], how="vertical")
                            # Drop count columns after concat
                            all_data = drop_count_cols(all_data)

    def create_athena_views(self) -> bool:
        try:
            athena = self.aws_config.session.client('athena')
            database = 'sportsdataverse'
            output_location = f's3://{self.aws_config.s3_bucket_name}/athena-results/'

            views = {
                'cfb_season_stats': """
                    CREATE OR REPLACE VIEW cfb_season_stats AS
                    SELECT season, COUNT(*) AS total_plays
                    FROM "cfb_pbp"
                    GROUP BY season
                """,
                'cfb_down_distance': """
                    CREATE OR REPLACE VIEW cfb_down_distance AS
                    SELECT season, down, distance, COUNT(*) AS count_of_plays
                    FROM "cfb_pbp"
                    GROUP BY season, down, distance
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
    manager = None
    try:
        manager = CFBDataManager(base_dir)
        logger.info("Starting CFB data upload process")

        # 1) Play-by-play
        logger.info("Loading play-by-play data")
        manager.load_pbp_data()

        # 2) Static data
        logger.info("Loading static data")
        manager.load_static_data()

        # 3) Season data
        logger.info("Loading season data")
        manager.load_season_data()

        # 4) Athena views
        logger.info("Creating Athena views")
        if not manager.create_athena_views():
            logger.warning("Athena views creation incomplete")

        logger.info("CFB data processing completed successfully")

    except Exception as e:
        logger.error(f"Error in main execution: {str(e)}")
        raise
    finally:
        if manager:
            manager.close()


if __name__ == "__main__":
    main()