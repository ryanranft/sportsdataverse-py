import os
import glob
import psutil
import time
from typing import Optional, Dict
from tqdm import tqdm
import polars as pl
import numpy as np
from concurrent.futures import ThreadPoolExecutor, TimeoutError
import threading
from contextlib import contextmanager
import logging
from logging.handlers import RotatingFileHandler
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from psycopg2 import pool
from psycopg2.extensions import register_adapter, AsIs
import sportsdataverse as sdv
from aws_setup import AWSServiceConfigurator

dotenv_path="/Users/ryanranft/0/sportsdataverse/sportsdataverse-py/.env"
load_dotenv(dotenv_path)
base_dir = os.getenv('RAW_DATA_DIR')

# Register numpy types
def addapt_numpy_float64(numpy_float64):
    return AsIs(numpy_float64)
def addapt_numpy_int64(numpy_int64):
    return AsIs(numpy_int64)
def addapt_numpy_int32(numpy_int32):
    return AsIs(numpy_int32)
def addapt_numpy_float32(numpy_float32):
    return AsIs(numpy_float32)

register_adapter(np.float64, addapt_numpy_float64)
register_adapter(np.int64, addapt_numpy_int64)
register_adapter(np.int32, addapt_numpy_int32)
register_adapter(np.float32, addapt_numpy_float32)


class ResourceMonitor:
    """Monitor system resources during data loading"""

    def __init__(self):
        self.process = psutil.Process()
        self.start_time = time.time()
        self.start_memory = self.process.memory_info().rss

    def get_stats(self) -> Dict:
        """Get current resource usage statistics"""
        current_memory = self.process.memory_info().rss
        memory_diff = current_memory - self.start_memory
        cpu_percent = self.process.cpu_percent()
        elapsed_time = time.time() - self.start_time

        return {
            'memory_used_mb': memory_diff / (1024 * 1024),
            'cpu_percent': cpu_percent,
            'elapsed_seconds': elapsed_time
        }


class NFLDataManager:
    def __init__(
        self,
        base_dir: str,
        schema_name: str = "nfl",
        min_connections: int = 1,
        max_connections: int = 10,
        timeout: int = 3600  # 1 hour default timeout
    ):
        """
        Initialize NFL Data Manager with AWS services and PostgreSQL connection

        Args:
            base_dir: Base directory for parquet files
            schema_name: PostgreSQL schema name
            min_connections: Minimum number of database connections
            max_connections: Maximum number of database connections
            timeout: Maximum time in seconds for operations
        """
        # Check if base_dir is properly set
        if base_dir is None:
            raise ValueError("RAW_DATA_DIR environment variable is not set")

        self.setup_logging()
        self.logger.info(f"Initializing with base_dir: {base_dir}")

        self.monitor = ResourceMonitor()
        self.timeout = timeout
        self.base_dir = base_dir
        self.schema_name = schema_name

        # Create directory if it doesn't exist
        try:
            os.makedirs(base_dir, exist_ok=True)
            self.logger.info(f"Created/verified base directory: {base_dir}")
        except Exception as e:
            self.logger.error(f"Error creating directory {base_dir}: {str(e)}")
            raise

        # Initialize core components
        self._init_connection_pool(min_connections, max_connections)
        self._setup_aws()
        self._init_data_mappings()

    def setup_logging(self):
        """Set up enhanced logging with file and console handlers"""
        self.logger = logging.getLogger('NFLDataManager')
        self.logger.setLevel(logging.INFO)

        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s'
        ))

        # File handler with rotation
        file_handler = RotatingFileHandler(
            'nfl_loader.log',
            maxBytes=10 * 1024 * 1024,  # 10MB
            backupCount=5
        )
        file_handler.setFormatter(logging.Formatter(
            '%(asctime)s - %(levelname)s - [%(threadName)s] %(message)s'
        ))

        self.logger.addHandler(console_handler)
        self.logger.addHandler(file_handler)

    @contextmanager
    def timeout_context(self, operation: str):
        """Context manager for operation timeouts"""
        timer = threading.Timer(self.timeout, lambda: self.logger.error(f"{operation} timed out"))
        timer.start()
        try:
            yield
        finally:
            timer.cancel()

    def log_progress(self, operation: str, current: int, total: int, extra_info: Dict = None):
        """Log progress with resource usage"""
        stats = self.monitor.get_stats()
        progress = (current / total) * 100 if total > 0 else 0

        msg = (f"{operation}: {progress:.1f}% complete. "
               f"Memory: {stats['memory_used_mb']:.1f}MB, "
               f"CPU: {stats['cpu_percent']:.1f}%, "
               f"Time: {stats['elapsed_seconds']:.1f}s")

        if extra_info:
            msg += f" | {extra_info}"

        self.logger.info(msg)

    def _init_connection_pool(self, min_connections, max_connections):
        """Initialize PostgreSQL connection pool"""
        try:
            self._connection_pool = pool.SimpleConnectionPool(
                min_connections,
                max_connections,
                dbname=os.getenv('PGDATABASE'),
                user=os.getenv('PGUSER'),
                password=os.getenv('PGPASSWORD'),
                host=os.getenv('PGHOST'),
                port=int(os.getenv('PGPORT', 5432))
            )
            self.logger.info("Database connection pool initialized")
        except Exception as e:
            self.logger.error(f"Error initializing database pool: {str(e)}")
            raise

    def _setup_aws(self):
        """Set up AWS services"""
        self.aws_config = AWSServiceConfigurator()
        self.aws_config.create_session()
        services = {
            'S3': self.aws_config.setup_s3(),
            'Firehose': self.aws_config.setup_firehose(),
            'Athena': self.aws_config.setup_athena(),
            'CloudWatch': self.aws_config.setup_cloudwatch()
        }

        # Log setup results
        for service, success in services.items():
            if success:
                self.logger.info(f"{service} setup successful")
            else:
                self.logger.error(f"{service} setup failed")

    def _init_data_mappings(self):
        """Initialize data mappings for NFL data sources"""
        self.data_mapping = {
            'teams': ('nfl_teams', sdv.nfl.load_nfl_teams),
            'rosters': ('nfl_rosters', sdv.nfl.load_nfl_rosters),
            'player_stats': ('nfl_player_stats', sdv.nfl.load_nfl_player_stats),
            'combine': ('nfl_combine', sdv.nfl.load_nfl_combine),
            'draft_picks': ('nfl_draft_picks', sdv.nfl.load_nfl_draft_picks),
            'officials': ('nfl_officials', sdv.nfl.load_nfl_officials),
            'players': ('nfl_players', sdv.nfl.load_nfl_players),
            'contracts': ('nfl_contracts', sdv.nfl.load_nfl_contracts),
        }

        self.season_data_mapping = {
            'depth_charts': ('nfl_depth_charts', sdv.nfl.load_nfl_depth_charts, 2001),
            'snap_counts': ('nfl_snap_counts', sdv.nfl.load_nfl_snap_counts, 2012),
            'pbp_participation': ('nfl_pbp_participation', sdv.nfl.load_nfl_pbp_participation, 2016),
            'ngs_passing': ('nfl_ngs_passing', sdv.nfl.load_nfl_ngs_passing, 2016),
            'ngs_rushing': ('nfl_ngs_rushing', sdv.nfl.load_nfl_ngs_rushing, 2016),
            'ngs_receiving': ('nfl_ngs_receiving', sdv.nfl.load_nfl_ngs_receiving, 2016),
            'weekly_rosters': ('nfl_weekly_rosters', sdv.nfl.load_nfl_weekly_rosters, 2002),
            'injuries': ('nfl_injuries', sdv.nfl.load_nfl_injuries, 2009),
            'schedule': ('nfl_schedule', sdv.nfl.load_nfl_schedule, 1999),
        }

        self.pfr_data_mapping = {
            'pfr_pass': ('nfl_pfr_passing', sdv.nfl.load_nfl_pfr_pass),
            'pfr_rush': ('nfl_pfr_rushing', sdv.nfl.load_nfl_pfr_rush),
            'pfr_rec': ('nfl_pfr_receiving', sdv.nfl.load_nfl_pfr_rec),
            'pfr_def': ('nfl_pfr_defense', sdv.nfl.load_nfl_pfr_def),
        }

        self.pfr_weekly_data_mapping = {
            'pfr_weekly_pass': ('nfl_pfr_weekly_passing', sdv.nfl.load_nfl_pfr_weekly_pass),
            'pfr_weekly_rush': ('nfl_pfr_weekly_rushing', sdv.nfl.load_nfl_pfr_weekly_rush),
            'pfr_weekly_rec': ('nfl_pfr_weekly_receiving', sdv.nfl.load_nfl_pfr_weekly_rec),
            'pfr_weekly_def': ('nfl_pfr_weekly_defense', sdv.nfl.load_nfl_pfr_weekly_def),
        }

    def load_pbp_from_files(self):
        """Enhanced play-by-play data loading with progress tracking"""
        try:
            pattern = os.path.join(self.base_dir, "nfl/nfl_pbp_*.parquet")
            files = sorted(glob.glob(pattern))

            if not files:
                self.logger.error(f"No PBP files found: {pattern}")
                return

            total_files = len(files)
            self.logger.info(f"Processing {total_files} PBP files")

            # Process first file to set up schema
            with tqdm(total=total_files, desc="Loading PBP files") as pbar:
                for idx, file_path in enumerate(files):
                    try:
                        with self.timeout_context(f"Processing {file_path}"):
                            df = pl.read_parquet(file_path)
                            season = os.path.basename(file_path).split('_')[-1].split('.')[0]

                            # Upload with progress tracking
                            total_rows = len(df)
                            batch_size = 1000
                            uploaded_rows = 0

                            for i in range(0, total_rows, batch_size):
                                batch = df.slice(i, min(batch_size, total_rows - i))
                                self.upload_dataframe_batch(
                                    batch,
                                    "nfl_pbp",
                                    if_exists='append' if idx > 0 else 'replace'
                                )
                                uploaded_rows += len(batch)

                                self.log_progress(
                                    f"Processing season {season}",
                                    uploaded_rows,
                                    total_rows,
                                    {"file": f"{idx + 1}/{total_files}"}
                                )

                            # Send metadata to Firehose
                            self.upload_to_firehose({
                                'sport': 'nfl',
                                'data_type': 'pbp',
                                'season': season,
                                'record_count': total_rows,
                                'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
                            })

                            pbar.update(1)

                    except Exception as e:
                        self.logger.error(f"Error processing {file_path}: {str(e)}")
                        continue

        except Exception as e:
            self.logger.error(f"Error in PBP loading: {str(e)}")
            raise

    def load_all_data(self, end_year: int = 2024):
        """Load all NFL data with enhanced progress tracking"""
        operations = [
            ("Loading PBP data", self.load_pbp_from_files),
            ("Loading static data", self.load_static_data),
            ("Loading season data", lambda: self.load_season_data(end_year)),
            ("Loading PFR data", self.load_pfr_data)
        ]

        with tqdm(total=len(operations), desc="Overall Progress") as pbar:
            for desc, operation in operations:
                try:
                    self.logger.info(f"Starting: {desc}")
                    with self.timeout_context(desc):
                        operation()
                    self.logger.info(f"Completed: {desc}")
                    pbar.update(1)
                except Exception as e:
                    self.logger.error(f"Error in {desc}: {str(e)}")
                    raise
                finally:
                    stats = self.monitor.get_stats()
                    self.logger.info(
                        f"Operation stats - Memory: {stats['memory_used_mb']:.1f}MB, "
                        f"CPU: {stats['cpu_percent']:.1f}%, "
                        f"Time: {stats['elapsed_seconds']:.1f}s"
                    )

    @contextmanager
    def get_connection(self, max_retries: int = 3):
        """Get a database connection from the pool with retry logic"""
        attempt = 0
        while attempt < max_retries:
            try:
                connection = self._connection_pool.getconn()
                try:
                    yield connection
                    return
                finally:
                    self._connection_pool.putconn(connection)
            except Exception as e:
                attempt += 1
                if attempt == max_retries:
                    self.logger.error(f"Failed to get connection after {max_retries} attempts")
                    raise
                self.logger.warning(f"Connection attempt {attempt} failed, retrying... Error: {str(e)}")
                time.sleep(1)

        def upload_to_s3(self, df: pl.DataFrame, key: str):
            """Upload DataFrame to S3 as parquet file"""
            try:
                s3 = self.aws_config.session.client('s3')
                parquet_buffer = df.write_parquet(None)
                s3.put_object(
                    Bucket=self.aws_config.s3_bucket_name,
                    Key=key,
                    Body=parquet_buffer
                )
                self.logger.info(f"Successfully uploaded data to s3://{self.aws_config.s3_bucket_name}/{key}")
            except Exception as e:
                self.logger.error(f"Error uploading to S3: {str(e)}")
                raise

        def upload_to_firehose(self, data: Dict):
            """Upload data to Kinesis Firehose with partition keys"""
            try:
                firehose = self.aws_config.session.client('firehose')
                response = firehose.put_record(
                    DeliveryStreamName=self.aws_config.firehose_stream_name,
                    Record={'Data': str(data)}
                )
                self.logger.info(f"Successfully sent data to Firehose stream: {self.aws_config.firehose_stream_name}")
                return response
            except Exception as e:
                self.logger.error(f"Error sending to Firehose: {str(e)}")
                raise

        def upload_dataframe_batch(self, df: pl.DataFrame, table_name: str, if_exists: str = 'append'):
            """Upload a batch of data with timeout handling"""
            try:
                with self.timeout_context(f"Uploading batch to {table_name}"):
                    # Upload to PostgreSQL
                    with self.get_connection() as conn:
                        with conn.cursor() as cur:
                            # Create schema if it doesn't exist
                            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {self.schema_name};")

                            # Convert to pandas for easier PostgreSQL handling
                            pdf = df.to_pandas()

                            # Get column definitions
                            columns = []
                            for col in pdf.columns:
                                dtype = pdf[col].dtype
                                if 'int' in str(dtype) or 'float' in str(dtype):
                                    sql_type = 'DOUBLE PRECISION'
                                elif 'bool' in str(dtype):
                                    sql_type = 'BOOLEAN'
                                elif 'datetime' in str(dtype):
                                    sql_type = 'TIMESTAMP'
                                else:
                                    sql_type = 'TEXT'
                                columns.append(f'"{col}" {sql_type}')

                            # Create or replace table
                            if if_exists == 'replace':
                                cur.execute(f"DROP TABLE IF EXISTS {self.schema_name}.{table_name};")
                                create_table_query = f"""
                                CREATE TABLE IF NOT EXISTS {self.schema_name}.{table_name} (
                                    {', '.join(columns)}
                                );
                                """
                                cur.execute(create_table_query)

                            # Upload data
                            columns_str = ', '.join(f'"{col}"' for col in pdf.columns)
                            values_str = ', '.join(['%s'] * len(pdf.columns))
                            insert_query = f"""
                            INSERT INTO {self.schema_name}.{table_name} ({columns_str})
                            VALUES ({values_str})
                            """

                            records = pdf.replace({np.nan: None}).to_records(index=False)
                            data = list(records)
                            cur.executemany(insert_query, data)
                            conn.commit()

                    # Upload to S3 with timeout
                    s3_key = f"sports-data/nfl/{table_name}/{time.strftime('%Y/%m/%d')}/{table_name}.parquet"
                    self.upload_to_s3(df, s3_key)

            except TimeoutError:
                self.logger.error(f"Timeout uploading batch to {table_name}")
                raise
            except Exception as e:
                self.logger.error(f"Error uploading batch: {str(e)}")
                raise

        def cleanup(self):
            """Enhanced cleanup with resource logging"""
            try:
                stats = self.monitor.get_stats()
                self.logger.info(
                    f"Final stats - Total Memory: {stats['memory_used_mb']:.1f}MB, "
                    f"Avg CPU: {stats['cpu_percent']:.1f}%, "
                    f"Total Time: {stats['elapsed_seconds']:.1f}s"
                )
                if self._connection_pool:
                    self._connection_pool.closeall()
                self.logger.info("Cleanup completed successfully")
            except Exception as e:
                self.logger.error(f"Error during cleanup: {str(e)}")

        def load_static_data(self):
            """Load static NFL data (data not tied to specific seasons)"""
            try:
                for data_type, (table_name, loader_func) in self.data_mapping.items():
                    try:
                        self.logger.info(f"Loading {data_type} data...")
                        df = loader_func(return_as_pandas=False)  # Get as polars DataFrame

                        # Upload data
                        self.upload_dataframe_batch(
                            df,
                            table_name,
                            if_exists='replace'
                        )

                        # Send metadata to Firehose
                        self.upload_to_firehose({
                            'sport': 'nfl',
                            'data_type': data_type,
                            'record_count': len(df),
                            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
                        })

                        self.logger.info(f"Successfully loaded {data_type} data")

                    except Exception as e:
                        self.logger.error(f"Error loading {data_type} data: {str(e)}")
                        raise

            except Exception as e:
                self.logger.error(f"Error in static data loading: {str(e)}")
                raise

        def load_season_data(self, end_year: int):
            """Load NFL data that varies by season"""
            try:
                for data_type, (table_name, loader_func, min_year) in self.season_data_mapping.items():
                    try:
                        self.logger.info(f"Loading {data_type} data...")
                        seasons = range(min_year, end_year + 1)
                        df = loader_func(seasons=seasons, return_as_pandas=False)

                        if len(df) > 0:
                            self.upload_dataframe_batch(
                                df,
                                table_name,
                                if_exists='replace'
                            )

                            self.upload_to_firehose({
                                'sport': 'nfl',
                                'data_type': data_type,
                                'seasons': f"{min_year}-{end_year}",
                                'record_count': len(df),
                                'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
                            })

                        self.logger.info(f"Successfully loaded {data_type} data")

                    except Exception as e:
                        self.logger.error(f"Error loading {data_type} data: {str(e)}")
                        continue

            except Exception as e:
                self.logger.error(f"Error in season data loading: {str(e)}")
                raise

        def load_pfr_data(self):
            """Load Pro Football Reference (PFR) data"""
            try:
                # Load regular PFR data
                for data_type, (table_name, loader_func) in self.pfr_data_mapping.items():
                    try:
                        self.logger.info(f"Loading {data_type} data...")
                        df = loader_func(return_as_pandas=False)

                        if len(df) > 0:
                            self.upload_dataframe_batch(
                                df,
                                table_name,
                                if_exists='replace'
                            )

                            self.upload_to_firehose({
                                'sport': 'nfl',
                                'data_type': data_type,
                                'record_count': len(df),
                                'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
                            })

                    except Exception as e:
                        self.logger.error(f"Error loading {data_type} data: {str(e)}")
                        continue

                # Load weekly PFR data
                for data_type, (table_name, loader_func) in self.pfr_weekly_data_mapping.items():
                    try:
                        self.logger.info(f"Loading {data_type} data...")
                        df = loader_func(seasons=range(2018, 2024), return_as_pandas=False)

                        if len(df) > 0:
                            self.upload_dataframe_batch(
                                df,
                                table_name,
                                if_exists='replace'
                            )

                            self.upload_to_firehose({
                                'sport': 'nfl',
                                'data_type': data_type,
                                'record_count': len(df),
                                'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
                            })

                    except Exception as e:
                        self.logger.error(f"Error loading {data_type} data: {str(e)}")
                        continue

            except Exception as e:
                self.logger.error(f"Error in PFR data loading: {str(e)}")
                raise

        def cleanup(self):
            """Enhanced cleanup with resource logging"""
            try:
                stats = self.monitor.get_stats()
                self.logger.info(
                    f"Final stats - Total Memory: {stats['memory_used_mb']:.1f}MB, "
                    f"Avg CPU: {stats['cpu_percent']:.1f}%, "
                    f"Total Time: {stats['elapsed_seconds']:.1f}s"
                )
                if hasattr(self, '_connection_pool') and self._connection_pool:
                    self._connection_pool.closeall()
                self.logger.info("Cleanup completed successfully")
            except Exception as e:
                self.logger.error(f"Error during cleanup: {str(e)}")

def main():
    """Enhanced main execution function"""
    base_dir = os.getenv('RAW_DATA_DIR')
    manager = None

    try:
        manager = NFLDataManager(
            base_dir=base_dir,
            timeout=3600  # 1 hour timeout
        )

        start_time = time.time()
        manager.load_all_data()
        elapsed_time = time.time() - start_time

        print(f"\nExecution completed in {elapsed_time:.2f} seconds")
        print("\nResource Usage Summary:")
        stats = manager.monitor.get_stats()
        print(f"  - Total Memory Used: {stats['memory_used_mb']:.1f} MB")
        print(f"  - Average CPU Usage: {stats['cpu_percent']:.1f}%")
        print(f"  - Total Time: {stats['elapsed_seconds']:.1f} seconds")

    except Exception as e:
        print(f"\nError in execution: {str(e)}")
        raise
    finally:
        if manager:
            manager.cleanup()


if __name__ == "__main__":
    main()