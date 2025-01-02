# /Users/ryanranft/0/sportsdataverse/sportsdataverse-py/sportsdataverse/nfl/nfl_loaders.py

import sportsdataverse
import polars as pl
import pandas as pd
import tempfile
import urllib.error
import requests
from typing import List, Optional
import logging
from pyreadr import download_file, read_r
from tqdm import tqdm

from sportsdataverse.config import (
    NFL_BASE_URL,
    NFL_COMBINE_URL,
    NFL_CONTRACTS_URL,
    NFL_DEPTH_CHARTS_URL,
    NFL_DRAFT_PICKS_URL,
    NFL_INJURIES_URL,
    NFL_NGS_PASSING_URL,
    NFL_NGS_RECEIVING_URL,
    NFL_NGS_RUSHING_URL,
    NFL_OFFICIALS_URL,
    NFL_PBP_PARTICIPATION_URL,
    NFL_PFR_SEASON_DEF_URL,
    NFL_PFR_SEASON_PASS_URL,
    NFL_PFR_SEASON_REC_URL,
    NFL_PFR_SEASON_RUSH_URL,
    NFL_PFR_WEEK_DEF_URL,
    NFL_PFR_WEEK_PASS_URL,
    NFL_PFR_WEEK_REC_URL,
    NFL_PFR_WEEK_RUSH_URL,
    NFL_PLAYER_KICKING_STATS_URL,
    NFL_PLAYER_STATS_URL,
    NFL_PLAYER_URL,
    NFL_ROSTER_URL,
    NFL_SNAP_COUNTS_URL,
    NFL_TEAM_LOGO_URL,
    NFL_TEAM_SCHEDULE_URL,
    NFL_WEEKLY_ROSTER_URL,
    NFLVERSEGITHUB,
    NFLVERSEGITHUBPBP,
)
from sportsdataverse.errors import season_not_found_error

NFL_SCHEDULE_URL = "https://nflverse.sportsdataverse.org/schedules/schedules_{season}.parquet"

def load_nfl_pbp(seasons: List[int], return_as_pandas=False) -> pl.DataFrame:
    data = pl.DataFrame()
    if isinstance(seasons, int):
        seasons = [seasons]
    for i in tqdm(seasons):
        season_not_found_error(int(i), 1999)
        try:
            i_data = pl.read_parquet(NFL_BASE_URL.format(season=i), use_pyarrow=True, columns=None)

            # Force columns to consistent dtypes if needed
            if "goal_to_go" in i_data.columns:
                i_data = i_data.with_columns(pl.col("goal_to_go").cast(pl.Float64))
            if "xyac_median_yardage" in i_data.columns:
                i_data = i_data.with_columns(pl.col("xyac_median_yardage").cast(pl.Float64))

            data = pl.concat([data, i_data], how="vertical")

        except urllib.error.HTTPError as e:
            if e.code == 404:
                print(f"No PBP data found for season {i}. Skipping.")
                continue
            else:
                raise
        except Exception as e:
            print(f"Error processing season {i}: {str(e)}")
            continue

    return data.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else data

def load_nfl_schedule(seasons: List[int], return_as_pandas=False) -> pl.DataFrame:
    """
    Loads NFL schedule data from parquet or, if needed, from an older RDS fallback.
    Also unifies columns and casts them to consistent dtypes (e.g., Int64).
    Drops/renames any conflicting columns (e.g. 'away_team') to avoid mismatch with 'game_id'.
    """
    data = pl.DataFrame()
    if isinstance(seasons, int):
        seasons = [seasons]

    for i in tqdm(seasons):
        season_not_found_error(int(i), 1999)
        try:
            # ---------------------------------------------------------
            # 1) Attempt to read from the new Parquet format
            # ---------------------------------------------------------
            schedule_url = NFL_SCHEDULE_URL.format(season=i)
            try:
                i_data = pl.read_parquet(schedule_url, use_pyarrow=True)

                # Cast gametime to string if needed
                if "gametime" in i_data.columns:
                    i_data = i_data.with_columns(pl.col("gametime").cast(pl.Utf8))

                # Cast score and result columns to Int64 if they exist
                if "away_score" in i_data.columns:
                    i_data = i_data.with_columns(pl.col("away_score").cast(pl.Int64))
                if "home_score" in i_data.columns:
                    i_data = i_data.with_columns(pl.col("home_score").cast(pl.Int64))
                if "home_result" in i_data.columns:
                    i_data = i_data.with_columns(pl.col("home_result").cast(pl.Int64))

            except Exception as e:
                print(f"Error parsing parquet for season {i}, trying RDS format...")

                # -----------------------------------------------------
                # 2) Fallback: read from older RDS data
                # -----------------------------------------------------
                schedule_url = NFL_TEAM_SCHEDULE_URL.format(season=i)
                with tempfile.TemporaryDirectory() as tempdirname:
                    i_data_pd = read_r(download_file(schedule_url, f"{tempdirname}/nfl_sched_{i}.rds"))[None]
                    i_data = pl.DataFrame(i_data_pd)

                    # Cast gametime to string if needed
                    if "gametime" in i_data.columns:
                        i_data = i_data.with_columns(pl.col("gametime").cast(pl.Utf8))

                    # Also cast away_score, home_score, home_result to Int64 if they exist
                    if "away_score" in i_data.columns:
                        i_data = i_data.with_columns(pl.col("away_score").cast(pl.Int64))
                    if "home_score" in i_data.columns:
                        i_data = i_data.with_columns(pl.col("home_score").cast(pl.Int64))
                    if "home_result" in i_data.columns:
                        i_data = i_data.with_columns(pl.col("home_result").cast(pl.Int64))

        except urllib.error.HTTPError as e:
            # 404 => no schedule for this season; skip
            if e.code == 404:
                print(f"Skipping season {i} because schedule file not found (404).")
                continue
            else:
                # Some other HTTP error => re-raise
                raise
        except Exception as e:
            print(f"Error loading season {i}: {str(e)}")
            continue

        # ---------------------------------------------------------
        # 3) Check if i_data is valid; unify columns
        # ---------------------------------------------------------
        if "i_data" not in locals() or i_data.is_empty():
            print(f"No schedule data found for season {i}. Skipping.")
            continue

        # (A) Drop or rename the problematic 'away_team' column if you don't need it.
        if "away_team" in i_data.columns:
            i_data = i_data.drop("away_team")

        # (B) Gather all columns from both data (accumulated) and i_data (this season)
        all_cols = set(data.columns).union(set(i_data.columns))
        for col in all_cols:
            if col not in i_data.columns:
                i_data = i_data.with_columns(pl.lit(None).alias(col))
            if col not in data.columns:
                data = data.with_columns(pl.lit(None).alias(col))

        # ---------------------------------------------------------
        # 4) Concatenate
        # ---------------------------------------------------------
        try:
            data = pl.concat([data, i_data], how="vertical")
        except Exception as e:
            print(f"Error stacking season {i} schedule data: {str(e)}")
            continue

    # Return final data
    return data.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else data


def load_nfl_ngs_passing(return_as_pandas=False) -> pl.DataFrame:
    return (
        pl.read_parquet(NFL_NGS_PASSING_URL, use_pyarrow=True).to_pandas(use_pyarrow_extension_array=True)
        if return_as_pandas
        else pl.read_parquet(NFL_NGS_PASSING_URL, use_pyarrow=True)
    )


def load_nfl_ngs_rushing(return_as_pandas=False) -> pl.DataFrame:
    return (
        pl.read_parquet(NFL_NGS_RUSHING_URL, use_pyarrow=True).to_pandas(use_pyarrow_extension_array=True)
        if return_as_pandas
        else pl.read_parquet(NFL_NGS_RUSHING_URL, use_pyarrow=True)
    )


def load_nfl_ngs_receiving(return_as_pandas=False) -> pl.DataFrame:
    return (
        pl.read_parquet(NFL_NGS_RECEIVING_URL, use_pyarrow=True).to_pandas(use_pyarrow_extension_array=True)
        if return_as_pandas
        else pl.read_parquet(NFL_NGS_RECEIVING_URL, use_pyarrow=True)
    )


def load_nfl_pfr_pass(return_as_pandas=False) -> pl.DataFrame:
    return (
        pl.read_parquet(NFL_PFR_SEASON_PASS_URL, use_pyarrow=True).to_pandas(use_pyarrow_extension_array=True)
        if return_as_pandas
        else pl.read_parquet(NFL_PFR_SEASON_PASS_URL, use_pyarrow=True)
    )


def load_nfl_pfr_weekly_pass(seasons: List[int], return_as_pandas=False) -> pl.DataFrame:
    data = pl.DataFrame()
    if isinstance(seasons, int):
        seasons = [seasons]
    for i in tqdm(seasons):
        season_not_found_error(int(i), 2018)
        i_data = pl.read_parquet(NFL_PFR_WEEK_PASS_URL.format(season=i), use_pyarrow=True)
        data = pl.concat([data, i_data], how="vertical")
    return data.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else data


def load_nfl_pfr_rush(return_as_pandas=False) -> pl.DataFrame:
    return (
        pl.read_parquet(NFL_PFR_SEASON_RUSH_URL, use_pyarrow=True).to_pandas(use_pyarrow_extension_array=True)
        if return_as_pandas
        else pl.read_parquet(NFL_PFR_SEASON_RUSH_URL, use_pyarrow=True)
    )


def load_nfl_pfr_weekly_rush(seasons: List[int], return_as_pandas=False) -> pl.DataFrame:
    data = pl.DataFrame()
    if isinstance(seasons, int):
        seasons = [seasons]
    for i in tqdm(seasons):
        season_not_found_error(int(i), 2018)
        i_data = pl.read_parquet(NFL_PFR_WEEK_RUSH_URL.format(season=i), use_pyarrow=True)
        data = pl.concat([data, i_data], how="vertical")
    return data.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else data


def load_nfl_pfr_rec(return_as_pandas=False) -> pl.DataFrame:
    return (
        pl.read_parquet(NFL_PFR_SEASON_REC_URL, use_pyarrow=True).to_pandas(use_pyarrow_extension_array=True)
        if return_as_pandas
        else pl.read_parquet(NFL_PFR_SEASON_REC_URL, use_pyarrow=True)
    )


def load_nfl_pfr_weekly_rec(seasons: List[int], return_as_pandas=False) -> pl.DataFrame:
    data = pl.DataFrame()
    if isinstance(seasons, int):
        seasons = [seasons]
    for i in tqdm(seasons):
        season_not_found_error(int(i), 2018)
        i_data = pl.read_parquet(NFL_PFR_WEEK_REC_URL.format(season=i), use_pyarrow=True)
        data = pl.concat([data, i_data], how="vertical")
    return data.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else data


def load_nfl_pfr_def(return_as_pandas=False) -> pl.DataFrame:
    return (
        pl.read_parquet(NFL_PFR_SEASON_DEF_URL, use_pyarrow=True).to_pandas(use_pyarrow_extension_array=True)
        if return_as_pandas
        else pl.read_parquet(NFL_PFR_SEASON_DEF_URL, use_pyarrow=True)
    )


def load_nfl_pfr_weekly_def(seasons: List[int], return_as_pandas=False) -> pl.DataFrame:
    data = pl.DataFrame()
    if isinstance(seasons, int):
        seasons = [seasons]
    for i in tqdm(seasons):
        season_not_found_error(int(i), 2018)
        i_data = pl.read_parquet(NFL_PFR_WEEK_DEF_URL.format(season=i), use_pyarrow=True)
        data = pl.concat([data, i_data], how="vertical")
    return data.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else data


def load_nfl_rosters(seasons: List[int], return_as_pandas=False) -> pl.DataFrame:
    """
    Load historical rosters data, unifying columns across different years.
    Also demonstrates dropping or renaming columns that don't match modern data.
    """
    data = pl.DataFrame()
    if isinstance(seasons, int):
        seasons = [seasons]

    for i in tqdm(seasons):
        try:
            i_data = pl.read_parquet(NFL_ROSTER_URL.format(season=i), use_pyarrow=True, columns=None)

            # 1) Drop unwanted or conflicting columns if you don't need them:
            if "draft_club" in i_data.columns:
                i_data = i_data.drop(["draft_club"])
            if "esb_id" in i_data.columns:
                i_data = i_data.drop(["esb_id"])

            # 2) Unify column names across years
            all_cols = set(data.columns) | set(i_data.columns)
            for col in all_cols:
                if col not in data.columns:
                    data = data.with_columns(pl.lit(None).alias(col))
                if col not in i_data.columns:
                    i_data = i_data.with_columns(pl.lit(None).alias(col))

            # 3) Handle specific mismatches like "rookie_year" -> "season"
            if "rookie_year" in i_data.columns and "season" not in i_data.columns:
                i_data = i_data.rename({"rookie_year": "season"})

            # 4) Now concatenate
            data = pl.concat([data, i_data], how="vertical")

        except Exception as e:
            print(f"Error processing season {i}: {str(e)}")
            continue

    return data.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else data


def load_nfl_weekly_rosters(seasons: List[int], return_as_pandas=False) -> pl.DataFrame:
    """
    Load weekly rosters data, forcing certain columns to consistent types.
    Also unifies columns across seasons so Polars can concatenate them without errors.
    Renames "status" -> "player_status" to avoid conflicts with "season".
    """
    data = pl.DataFrame()
    if isinstance(seasons, int):
        seasons = [seasons]

    for i in tqdm(seasons):
        season_not_found_error(int(i), 2002)

        # 1) Read this season's weekly roster parquet
        i_data = pl.read_parquet(
            NFL_WEEKLY_ROSTER_URL.format(season=i),
            use_pyarrow=True,
            columns=None
        )

        # 2) Rename "status" to "player_status" if it exists
        if "status" in i_data.columns:
            i_data = i_data.rename({"status": "player_status"})

        # 3) Gather all columns from both the existing 'data' and the new i_data
        all_cols = set(data.columns).union(i_data.columns)
        for col in all_cols:
            # If 'col' is missing in i_data, add it as None
            if col not in i_data.columns:
                i_data = i_data.with_columns(pl.lit(None).alias(col))
            # If 'col' is missing in data, add it as None
            if col not in data.columns:
                data = data.with_columns(pl.lit(None).alias(col))

        # 4) Ensure certain columns have consistent dtypes
        if "draft_number" in i_data.columns:
            i_data = i_data.with_columns(pl.col("draft_number").cast(pl.Utf8))
        if "jersey_number" in i_data.columns:
            i_data = i_data.with_columns(pl.col("jersey_number").cast(pl.Utf8))

        # 5) Concatenate this season's data into the main DataFrame
        data = pl.concat([data, i_data], how="vertical")

    # Return as Pandas or Polars, based on user preference
    return data.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else data


def load_nfl_teams(return_as_pandas=False) -> pl.DataFrame:
    return (
        pl.read_csv(NFL_TEAM_LOGO_URL).to_pandas(use_pyarrow_extension_array=True)
        if return_as_pandas
        else pl.read_csv(NFL_TEAM_LOGO_URL)
    )


def load_nfl_players(return_as_pandas=False) -> pl.DataFrame:
    return (
        pl.read_parquet(NFL_PLAYER_URL, use_pyarrow=True, columns=None).to_pandas(use_pyarrow_extension_array=True)
        if return_as_pandas
        else pl.read_parquet(NFL_PLAYER_URL, use_pyarrow=True, columns=None)
    )


def load_nfl_player_stats(kicking: bool = False, return_as_pandas=False) -> pl.DataFrame:
    """
    Load NFL player stats data.

    Parameters:
    kicking (bool): If True, loads kicking stats, otherwise loads regular player stats
    return_as_pandas (bool): If True, returns pandas DataFrame, otherwise returns polars DataFrame

    Returns:
    DataFrame containing NFL player statistics
    """
    url = NFL_PLAYER_KICKING_STATS_URL if kicking else NFL_PLAYER_STATS_URL
    return (
        pl.read_parquet(url, use_pyarrow=True, columns=None).to_pandas(use_pyarrow_extension_array=True)
        if return_as_pandas
        else pl.read_parquet(url, use_pyarrow=True, columns=None)
    )


def load_nfl_snap_counts(seasons: List[int], return_as_pandas=False) -> pl.DataFrame:
    data = pl.DataFrame()
    if isinstance(seasons, int):
        seasons = [seasons]
    for i in tqdm(seasons):
        season_not_found_error(int(i), 2012)
        i_data = pl.read_parquet(NFL_SNAP_COUNTS_URL.format(season=i), use_pyarrow=True, columns=None)
        data = pl.concat([data, i_data], how="vertical")
    return data.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else data


def load_nfl_pbp_participation(seasons: List[int], return_as_pandas=False) -> pl.DataFrame:
    data = pl.DataFrame()
    if isinstance(seasons, int):
        seasons = [seasons]
    for i in tqdm(seasons):
        season_not_found_error(int(i), 2016)
        i_data = pl.read_parquet(NFL_PBP_PARTICIPATION_URL.format(season=i), use_pyarrow=True, columns=None)
        data = pl.concat([data, i_data], how="vertical")
    return data.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else data


def load_nfl_injuries(seasons: List[int], return_as_pandas=False) -> pl.DataFrame:
    """
    Load NFL injuries data for selected seasons
    Also fix 'season' mismatch by forcing it to Int32 if present.
    And fix 'week' mismatch by forcing it to Int32 as well.
    """
    data = pl.DataFrame()
    if isinstance(seasons, int):
        seasons = [seasons]
    for i in tqdm(seasons):
        season_not_found_error(int(i), 2009)
        i_data = pl.read_parquet(NFL_INJURIES_URL.format(season=i), use_pyarrow=True, columns=None)

        # Force 'season' to Int32 if it exists
        if "season" in i_data.columns:
            i_data = i_data.with_columns(pl.col("season").cast(pl.Int32))

        # Force 'week' to Int32 if it exists
        if "week" in i_data.columns:
            i_data = i_data.with_columns(pl.col("week").cast(pl.Int32))

        data = pl.concat([data, i_data], how="vertical")

    return data.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else data


def load_nfl_depth_charts(seasons: List[int], return_as_pandas=False) -> pl.DataFrame:
    data = pl.DataFrame()
    if isinstance(seasons, int):
        seasons = [seasons]
    for i in tqdm(seasons):
        season_not_found_error(int(i), 2001)
        i_data = pl.read_parquet(NFL_DEPTH_CHARTS_URL.format(season=i), use_pyarrow=True, columns=None)
        data = pl.concat([data, i_data], how="vertical")
    return data.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else data


def load_nfl_contracts(return_as_pandas=False) -> pl.DataFrame:
    return (
        pl.read_parquet(NFL_CONTRACTS_URL, use_pyarrow=True, columns=None).to_pandas(use_pyarrow_extension_array=True)
        if return_as_pandas
        else pl.read_parquet(NFL_CONTRACTS_URL, use_pyarrow=True, columns=None)
    )


def load_nfl_combine(return_as_pandas=False) -> pl.DataFrame:
    return (
        pl.read_parquet(NFL_COMBINE_URL, use_pyarrow=True, columns=None).to_pandas(use_pyarrow_extension_array=True)
        if return_as_pandas
        else pl.read_parquet(NFL_COMBINE_URL, use_pyarrow=True, columns=None)
    )


def load_nfl_draft_picks(return_as_pandas=False) -> pl.DataFrame:
    return (
        pl.read_parquet(NFL_DRAFT_PICKS_URL, use_pyarrow=True, columns=None).to_pandas(use_pyarrow_extension_array=True)
        if return_as_pandas
        else pl.read_parquet(NFL_DRAFT_PICKS_URL, use_pyarrow=True, columns=None)
    )


def load_nfl_officials(return_as_pandas=False) -> pl.DataFrame:
    return (
        pl.read_parquet(NFL_OFFICIALS_URL, use_pyarrow=True, columns=None).to_pandas(use_pyarrow_extension_array=True)
        if return_as_pandas
        else pl.read_parquet(NFL_OFFICIALS_URL, use_pyarrow=True, columns=None)
    )