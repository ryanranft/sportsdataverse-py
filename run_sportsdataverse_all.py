import sportsdataverse
import sportsdataverse as sdv
import os
import logging
import polars as pl

# Configure logging properties
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Set the output directory to save all data
BASE_DIR = "raw_data/nfl"


def save_data_parquet(data, filepath):
    """
    Save data to a Parquet file with proper directory handling.
    """
    if data is None:
        logger.warning(f"No data to save for {filepath}")
        return

    try:
        # Ensure directory exists
        full_path = os.path.join(BASE_DIR, filepath)
        os.makedirs(os.path.dirname(os.path.abspath(full_path)), exist_ok=True)

        # Handle different DataFrame types
        if isinstance(data, pl.DataFrame):
            data.write_parquet(full_path)
        elif hasattr(data, "to_parquet"):  # pandas DataFrame
            data.to_parquet(full_path, index=False)
        else:
            logger.warning(f"Unsupported data type for {filename}")
            return

        logger.info(f"Successfully saved data to {full_path}")
    except Exception as e:
        logger.error(f"Failed to save data to {filepath}: {e}")


def save_data_csv(data, filename):
    """
    Save data to a CSV file within the raw_data directory.

    Args:
        data: DataFrame-like object (Polars or Pandas).
        filename: Name of the output file (string).
    """
    if data is None:
        logger.warning(f"No data to save for {filename}")
        return

    try:
        os.makedirs(BASE_DIR, exist_ok=True)
        filepath = os.path.join(BASE_DIR, filename)

        # Pandas DataFrame
        if hasattr(data, "to_csv"):
            data.to_csv(filepath, index=False)
            logger.info(f"Saved data to {filepath}")

        # Polars DataFrame
        elif hasattr(data, "write_csv"):
            data.write_csv(filepath)
            logger.info(f"Saved data to {filepath}")

        else:
            logger.warning(f"Unsupported data type; could not save: {filename}")
    except Exception as e:
        logger.error(f"Failed to save data to {filename}: {e}")


def execute_nfl():
    """
    Execute NFL functions and save collected data (Parquet, return_as_pandas=False).
    Gathers a comprehensive set of NFL data from 1999+ using various sportsdataverse.nfl loaders.
    """
    try:
        logger.info("Running NFL data collection")

        # --------------------------------------------------------------------
        # 1) Basic year-to-year data
        # --------------------------------------------------------------------
        # NFL Play-by-Play Data (1999–2024)
        pbp_data = sdv.load_nfl_pbp(seasons=range(1999, 2025), return_as_pandas=False)
        save_data_parquet(pbp_data, "nfl_pbp_1999_2024.parquet")

        # NFL Schedule (1999-present)
        schedule_data = sdv.load_nfl_schedule(seasons=range(1999, 2024), return_as_pandas=False)
        save_data_parquet(schedule_data, "nfl_schedule_1999_2024.parquet")

        # NFL Player Stats
        nflPlayerStats = sportsdataverse.nfl.load_nfl_player_stats(return_as_pandas=False)
        save_data_parquet(nflPlayerStats, "player_stats.parquet")

        # --------------------------------------------------------------------
        # 2) Next Gen Stats (2016-present)
        # --------------------------------------------------------------------
        # NFL Next Gen Stats Passing (2016-present)
        ngs_passing = sdv.load_nfl_ngs_passing(seasons=range(2016, 2024), return_as_pandas=False)
        save_data_parquet(ngs_passing, "nfl_ngs_passing_2016_2023.parquet")

        # NFL Next Gen Stats Rushing (2016-present)
        ngs_rushing = sdv.load_nfl_ngs_rushing(seasons=range(2016, 2024), return_as_pandas=False)
        save_data_parquet(ngs_rushing, "nfl_ngs_rushing_2016_2023.parquet")

        # NFL Next Gen Stats Receiving (2016-present)
        ngs_receiving = sdv.load_nfl_ngs_receiving(seasons=range(2016, 2024), return_as_pandas=False)
        save_data_parquet(ngs_receiving, "nfl_ngs_receiving_2016_2023.parquet")

        # --------------------------------------------------------------------
        # 3) Pro-Football-Reference (Advanced)
        # --------------------------------------------------------------------
        # Advanced Passing (2018-present)
        pfr_adv_passing = sdv.load_nfl_pfr_pass(seasons=range(2018, 2024), return_as_pandas=False)
        save_data_parquet(pfr_adv_passing, "nfl_pfr_adv_passing_2018_2023.parquet")

        # Advanced Rushing (All available)
        pfr_adv_rushing = sdv.load_nfl_pfr_rush(return_as_pandas=False)
        save_data_parquet(pfr_adv_rushing, "nfl_pfr_adv_rushing.parquet")

        # Advanced Receiving (All available)
        pfr_adv_receiving = sdv.load_nfl_pfr_rec(return_as_pandas=False)
        save_data_parquet(pfr_adv_receiving, "nfl_pfr_adv_receiving.parquet")

        # Advanced Defensive (All available)
        pfr_adv_defensive = sdv.load_nfl_pfr_def(return_as_pandas=False)
        save_data_parquet(pfr_adv_defensive, "nfl_pfr_adv_defensive.parquet")

        # --------------------------------------------------------------------
        # 4) Pro-Football-Reference (Weekly Advanced) (2018-present)
        # --------------------------------------------------------------------
        # Weekly Advanced Passing (2018-present)
        pfr_adv_weekly_pass = sdv.load_nfl_pfr_weekly_pass(seasons=range(2018, 2024), return_as_pandas=False)
        save_data_parquet(pfr_adv_weekly_pass, "nfl_pfr_adv_weekly_pass_2018_2023.parquet")

        # Weekly Advanced Rushing (2018-present)
        pfr_adv_weekly_rush = sdv.load_nfl_pfr_weekly_rush(seasons=range(2018, 2024), return_as_pandas=False)
        save_data_parquet(pfr_adv_weekly_rush, "nfl_pfr_adv_weekly_rush_2018_2023.parquet")

        # Weekly Advanced Receiving (2018-present)
        pfr_adv_weekly_rec = sdv.load_nfl_pfr_weekly_rec(seasons=range(2018, 2024), return_as_pandas=False)
        save_data_parquet(pfr_adv_weekly_rec, "nfl_pfr_adv_weekly_rec_2018_2023.parquet")

        # Weekly Advanced Defensive (2018-present)
        pfr_adv_weekly_def = sdv.load_nfl_pfr_weekly_def(seasons=range(2018, 2024), return_as_pandas=False)
        save_data_parquet(pfr_adv_weekly_def, "nfl_pfr_adv_weekly_def_2018_2023.parquet")

        # --------------------------------------------------------------------
        # 5) Additional Data
        # --------------------------------------------------------------------
        # NFL Teams (Current)
        teams_data = sdv.load_nfl_teams(return_as_pandas=False)
        save_data_parquet(teams_data, "nfl_teams.parquet")

        # NFL Rosters (1920-present)
        rosters_data = sdv.load_nfl_rosters(seasons=range(1920, 2024), return_as_pandas=False)
        save_data_parquet(rosters_data, "nfl_rosters_1920_2023.parquet")

        # NFL Weekly Rosters (2002-present)
        weekly_rosters = sdv.load_nfl_weekly_rosters(seasons=range(2002, 2024), return_as_pandas=False)
        save_data_parquet(weekly_rosters, "nfl_weekly_rosters_2002_2023.parquet")

        # NFL Players (All available)
        players_data = sdv.load_nfl_players(return_as_pandas=False)
        save_data_parquet(players_data, "nfl_players.parquet")

        # NFL Snap Counts (2012-present)
        snap_counts = sdv.load_nfl_snap_counts(seasons=range(2012, 2024), return_as_pandas=False)
        save_data_parquet(snap_counts, "nfl_snap_counts_2012_2023.parquet")

        # NFL PBP Participation (2016-present)
        pbp_participation = sdv.load_nfl_pbp_participation(seasons=range(2016, 2024), return_as_pandas=False)
        save_data_parquet(pbp_participation, "nfl_pbp_participation_2016_2023.parquet")

        # NFL Draft Picks (All available)
        draft_picks_data = sdv.load_nfl_draft_picks(return_as_pandas=False)
        save_data_parquet(draft_picks_data, "nfl_draft_picks.parquet")

        # NFL Depth Charts (2001-present)
        depth_charts = sdv.load_nfl_depth_charts(seasons=range(2001, 2024))
        save_data_parquet(depth_charts, "nfl_depth_charts_2001_2023.parquet")

        # NFL Combine (All available)
        combine_data = sdv.load_nfl_combine(return_as_pandas=False)
        save_data_parquet(combine_data, "nfl_combine.parquet")

        # NFL Officials (All available)
        officials_data = sdv.load_nfl_officials(return_as_pandas=False)
        save_data_parquet(officials_data, "nfl_officials.parquet")

        logger.info("Finished running NFL data functions!")
    except Exception as e:
        logger.error(f"Error in NFL module: {e}")



def execute_cfb():
    """
    Execute College Football (CFB) functions and save collected data (Parquet, return_as_pandas=False).
    """
    try:
        logger.info("Running College Football data collection")

        # College Football Play-by-Play Data
        pbp_data = sdv.load_cfb_pbp(seasons=[2002,2024], return_as_pandas=False)
        save_data_parquet(pbp_data, "cfb_pbp_2022.parquet")

        # CFB Schedule
        schedule_data = sdv.load_cfb_schedule(seasons=[2023], return_as_pandas=False)
        save_data_parquet(schedule_data, "cfb_schedule_2023.parquet")

        # CFB Rosters
        rosters_data = sdv.load_cfb_game_rosters(seasons=[2023], return_as_pandas=False)
        save_data_parquet(rosters_data, "cfb_rosters_2023.parquet")

        # CFB Teams
        teams_data = sdv.load_cfb_teams(return_as_pandas=False)
        save_data_parquet(teams_data, "cfb_teams.parquet")

        logger.info("Finished running CFB data functions!")
    except Exception as e:
        logger.error(f"Error in CFB module: {e}")


def execute_nba():
    """
    Execute NBA functions and save collected data (Parquet, return_as_pandas=False).
    """
    try:
        logger.info("Running NBA data collection")

        # NBA Play-by-Play Data
        pbp_data = sdv.load_nba_pbp(seasons=[2023], return_as_pandas=False)
        save_data_parquet(pbp_data, "nba_pbp_2023.parquet")

        # NBA Schedule
        schedule_data = sdv.load_nba_schedule(seasons=[2023], return_as_pandas=False)
        save_data_parquet(schedule_data, "nba_schedule_2023.parquet")

        # NBA Team Box Score Data
        box_score_data = sdv.load_nba_team_box(seasons=[2023], return_as_pandas=False)
        save_data_parquet(box_score_data, "nba_team_box_2023.parquet")

        logger.info("Finished running NBA data functions!")
    except Exception as e:
        logger.error(f"Error in NBA module: {e}")


def execute_remaining_sports():
    """
    Execute NHL, WBB, WNBA, and MBB functions and save collected data (Parquet, return_as_pandas=False).
    """
    try:
        logger.info("Running NHL, MBB, WBB, and WNBA data collection")

        # Example for NHL
        nhl_pbp = sdv.load_nhl_pbp(seasons=[2022], return_as_pandas=False)
        save_data_parquet(nhl_pbp, "nhl_pbp_2022.parquet")

        nhl_schedule = sdv.load_nhl_schedule(seasons=[2022], return_as_pandas=False)
        save_data_parquet(nhl_schedule, "nhl_schedule_2022.parquet")

        # More sports can be added systematically...
    except Exception as e:
        logger.error(f"Error while processing remaining sports: {e}")


######################################################################################################
##                                              CFB Section                                         ##
######################################################################################################
def getCfbEspnTeams():
    """
    Load college football team ID information and logos, return_as_pandas=False.
    """
    logger.info("sportsdataverse.cfb.espn_cfb_teams() - Load CFB team info in Polars or other, not pandas.")
    try:
        cfbEspnTeams_df = sportsdataverse.cfb.espn_cfb_teams(return_as_pandas=False)
        print(cfbEspnTeams_df)
        save_data_parquet(cfbEspnTeams_df, "cfb_espn_teams.parquet")
    except Exception as e:
        logger.error(f"Failed to load ESPN CFB teams: {e}")


def getEspnPbp():
    """
    Pull the game by ID (espn cfb pbp), return_as_pandas=False.
    """
    logger.info("sportsdataverse.cfb.CFBPlayProcess(gameId=401256137, return_as_pandas=False).espn_cfb_pbp()")
    try:
        espnPbp = sportsdataverse.cfb.CFBPlayProcess(gameId=401256137, return_as_pandas=False).espn_cfb_pbp()
        print(espnPbp)
        save_data_parquet(espnPbp, "espn_cfb_pbp_401256137.parquet")
    except Exception as e:
        logger.error(f"Failed to load ESPN CFB PBP data: {e}")


def getCfbPbp():
    """
    Load CFB PBP data (2009), return_as_pandas=False.
    """
    logger.info("sportsdataverse.cfb.load_cfb_pbp(seasons=range(2009,2010), return_as_pandas=False)")
    try:
        cfbPbp_df = sportsdataverse.cfb.load_cfb_pbp(seasons=range(2009, 2010), return_as_pandas=False)
        print(cfbPbp_df)
        save_data_parquet(cfbPbp_df, "cfb_pbp_2009.parquet")
    except Exception as e:
        logger.error(f"Failed to load CFB PBP data: {e}")


def getEspnCfbCalendar():
    """
    ESPN CFB calendar (2020), return_as_pandas=False.
    """
    logger.info("sportsdataverse.cfb.espn_cfb_calendar(season=2020, groups=80, return_as_pandas=False)")
    try:
        espnCfbCalendar = sportsdataverse.cfb.espn_cfb_calendar(season=2020, groups=80, return_as_pandas=False)
        print(espnCfbCalendar)
        save_data_parquet(espnCfbCalendar, "espn_cfb_calendar_2020.parquet")
    except Exception as e:
        logger.error(f"Failed to load ESPN CFB calendar: {e}")


def getEspnSchedule():
    """
    ESPN CFB schedule (2020, week 5), return_as_pandas=False.
    """
    logger.info("sportsdataverse.cfb.espn_cfb_schedule(dates=2020, week=5, season_type=2, groups=80, return_as_pandas=False)")
    try:
        espnSchedule = sportsdataverse.cfb.espn_cfb_schedule(
            dates=2020, week=5, season_type=2, groups=80, return_as_pandas=False
        )
        print(espnSchedule)
        save_data_parquet(espnSchedule, "espn_cfb_schedule_2020_week5.parquet")
    except Exception as e:
        logger.error(f"Failed to load ESPN CFB schedule: {e}")


def loadCfbPbp():
    """
    Range-based CFB PBP data (2009), return_as_pandas=False.
    """
    logger.info("sportsdataverse.cfb.load_cfb_pbp(seasons=range(2009,2010), return_as_pandas=False)")
    try:
        loadedCfbPbp = sportsdataverse.cfb.load_cfb_pbp(seasons=range(2009, 2010), return_as_pandas=False)
        print(loadedCfbPbp)
        save_data_parquet(loadedCfbPbp, "cfb_pbp_range_2009.parquet")
    except Exception as e:
        logger.error(f"Failed to load range-based CFB PBP data: {e}")


def loadCfbRosters():
    """
    Load CFB roster data (empty in this range?), return_as_pandas=False.
    """
    logger.info("sportsdataverse.cfb.load_cfb_rosters(seasons=range(2014,2010), return_as_pandas=False)")
    try:
        loadedCfbRosters = sportsdataverse.cfb.load_cfb_rosters(seasons=range(2014, 2010), return_as_pandas=False)
        print(loadedCfbRosters)
        save_data_parquet(loadedCfbRosters, "cfb_rosters_2014_to_2010.parquet")
    except Exception as e:
        logger.error(f"Failed to load CFB rosters: {e}")


def loadCfbSchedule():
    """
    CFB schedule data (2009), return_as_pandas=False.
    """
    logger.info("sportsdataverse.cfb.load_cfb_schedule(seasons=range(2009,2010), return_as_pandas=False)")
    try:
        loadedCfbSchedule = sportsdataverse.cfb.load_cfb_schedule(seasons=range(2009, 2010), return_as_pandas=False)
        print(loadedCfbSchedule)
        save_data_parquet(loadedCfbSchedule, "cfb_schedule_2009.parquet")
    except Exception as e:
        logger.error(f"Failed to load CFB schedule: {e}")


def loadCfbTeamInfo():
    """
    CFB team info (2009), return_as_pandas=False.
    """
    logger.info("sportsdataverse.cfb.load_cfb_team_info(seasons=range(2009,2010), return_as_pandas=False)")
    try:
        loadedCfbTeamInfo = sportsdataverse.cfb.load_cfb_team_info(seasons=range(2009, 2010), return_as_pandas=False)
        print(loadedCfbTeamInfo)
        save_data_parquet(loadedCfbTeamInfo, "cfb_team_info_2009.parquet")
    except Exception as e:
        logger.error(f"Failed to load CFB team info: {e}")

######################################################################################################
##                                              MBB Section                                         ##
######################################################################################################
def loadMbbPbp():
    """
    Men's college basketball PBP data (2020–2021), return_as_pandas=False.
    """
    logger.info("sportsdataverse.mbb.load_mbb_pbp(seasons=range(2020,2022), return_as_pandas=False)")
    try:
        mbbPbp = sportsdataverse.mbb.load_mbb_pbp(seasons=range(2020, 2022), return_as_pandas=False)
        print(mbbPbp)
        save_data_parquet(mbbPbp, "mbb_pbp_2020_2021.parquet")
    except Exception as e:
        logger.error(f"Failed to load MBB PBP data: {e}")


def loadMbbPlayerBoxscore():
    """
    Men's college basketball player boxscore data (2020–2021), return_as_pandas=False.
    """
    logger.info("sportsdataverse.mbb.load_mbb_player_boxscore(seasons=range(2020,2022), return_as_pandas=False)")
    try:
        mbbPlayerBoxscore = sportsdataverse.mbb.load_mbb_player_boxscore(seasons=range(2020, 2022), return_as_pandas=False)
        print(mbbPlayerBoxscore)
        save_data_parquet(mbbPlayerBoxscore, "mbb_player_boxscore_2020_2021.parquet")
    except Exception as e:
        logger.error(f"Failed to load MBB player boxscore data: {e}")


def loadMbbSchedule():
    """
    Men's college basketball schedule data (2020–2021), return_as_pandas=False.
    """
    logger.info("sportsdataverse.mbb.load_mbb_schedule(seasons=range(2020,2022), return_as_pandas=False)")
    try:
        mbbSchedule = sportsdataverse.mbb.load_mbb_schedule(seasons=range(2020, 2022), return_as_pandas=False)
        print(mbbSchedule)
        save_data_parquet(mbbSchedule, "mbb_schedule_2020_2021.parquet")
    except Exception as e:
        logger.error(f"Failed to load MBB schedule: {e}")


def loadMbbTeamBoxscore():
    """
    Men's college basketball team boxscore data (2002–2021), return_as_pandas=False.
    """
    logger.info("sportsdataverse.mbb.load_mbb_team_boxscore(seasons=range(2002,2022), return_as_pandas=False)")
    try:
        mbbTeamBoxscore = sportsdataverse.mbb.load_mbb_team_boxscore(seasons=range(2002, 2022), return_as_pandas=False)
        print(mbbTeamBoxscore)
        save_data_parquet(mbbTeamBoxscore, "mbb_team_boxscore_2002_2021.parquet")
    except Exception as e:
        logger.error(f"Failed to load MBB team boxscore data: {e}")


def loadEspnMbbPbp():
    """
    Pull ESPN MBB PBP by game_id=401265031, return_as_pandas=False.
    """
    logger.info("sportsdataverse.mbb.espn_mbb_pbp(game_id=401265031, return_as_pandas=False)")
    try:
        espnMbbPbp = sportsdataverse.mbb.espn_mbb_pbp(game_id=401265031, return_as_pandas=False)
        print(espnMbbPbp)
        save_data_parquet(espnMbbPbp, "espn_mbb_pbp_401265031.parquet")
    except Exception as e:
        logger.error(f"Failed to load ESPN MBB PBP data: {e}")


def loadEspnMbbSchedule():
    """
    ESPN MBB schedule (2020), return_as_pandas=False.
    """
    logger.info("sportsdataverse.mbb.espn_mbb_schedule(dates=2020, groups=50, season_type=2, return_as_pandas=False)")
    try:
        espnMbbSchedule = sportsdataverse.mbb.espn_mbb_schedule(dates=2020, groups=50, season_type=2, return_as_pandas=False)
        print(espnMbbSchedule)
        save_data_parquet(espnMbbSchedule, "espn_mbb_schedule_2020.parquet")
    except Exception as e:
        logger.error(f"Failed to load ESPN MBB schedule: {e}")


######################################################################################################
##                                              NBA Section                                         ##
######################################################################################################
def loadNbaPbp():
    """
    NBA play by play data (2002–2024), return_as_pandas=False.
    """
    logger.info("sportsdataverse.nba.load_nba_pbp(seasons=range(2002,2024), return_as_pandas=False)")
    try:
        nbaPbp = sportsdataverse.nba.load_nba_pbp(seasons=range(2002, 2024), return_as_pandas=False)
        print(nbaPbp)
        save_data_parquet(nbaPbp, "nba_pbp_2002_2024.parquet")
    except Exception as e:
        logger.error(f"Failed to load NBA PBP data: {e}")


def loadNbaPlayerBoxscores():
    """
    NBA player boxscore data (2002–2024), return_as_pandas=False.
    """
    logger.info("sportsdataverse.nba.load_nba_player_boxscore(seasons=range(2002,2024), return_as_pandas=False)")
    try:
        nbaBoxscore = sportsdataverse.nba.load_nba_player_boxscore(seasons=range(2002, 2024), return_as_pandas=False)
        print(nbaBoxscore)
        save_data_parquet(nbaBoxscore, "nba_player_boxscore_2002_2024.parquet")
    except Exception as e:
        logger.error(f"Failed to load NBA player boxscore data: {e}")


def loadNbaSchedule():
    """
    NBA schedule data (2002–2024), return_as_pandas=False.
    """
    logger.info("sportsdataverse.nba.load_nba_schedule(seasons=range(2002,2024), return_as_pandas=False)")
    try:
        nbaSchedule = sportsdataverse.nba.load_nba_schedule(seasons=range(2002, 2024), return_as_pandas=False)
        print(nbaSchedule)
        save_data_parquet(nbaSchedule, "nba_schedule_2002_2024.parquet")
    except Exception as e:
        logger.error(f"Failed to load NBA schedule: {e}")


def loadNbaTeamBoxscores():
    """
    NBA team boxscore data (2002–2024), return_as_pandas=False.
    """
    logger.info("sportsdataverse.nba.load_nba_team_boxscore(seasons=range(2002,2024), return_as_pandas=False)")
    try:
        nbaTeamBoxscores = sportsdataverse.nba.load_nba_team_boxscore(seasons=range(2002, 2024), return_as_pandas=False)
        print(nbaTeamBoxscores)
        save_data_parquet(nbaTeamBoxscores, "nba_team_boxscore_2002_2024.parquet")
    except Exception as e:
        logger.error(f"Failed to load NBA team boxscore data: {e}")


def loadEspnNbaPbp():
    """
    Pull ESPN NBA PBP data (2002-2024), return_as_pandas=False.
    """
    logger.info("sportsdataverse.nba.espn_nba_pbp(seasons=range(2002,2024), return_as_pandas=False)")
    try:
        espnNbaPbp = sportsdataverse.nba.espn_nba_pbp(seasons=range(2002,2024), return_as_pandas=False)
        print(espnNbaPbp)
        save_data_parquet(espnNbaPbp, "espn_nba_pbp_2002_2024.parquet")
    except Exception as e:
        logger.error(f"Failed to load ESPN NBA PBP data: {e}")


def loadEspnNbaCalendar():
    """
    ESPN NBA calendar, multiple date range, return_as_pandas=False.
    """
    logger.info("sportsdataverse.nba.espn_nba_schedule(dates=range(2002,2024), season_type=2, return_as_pandas=False)")
    try:
        espnNbaCalendar = sportsdataverse.nba.espn_nba_schedule(dates=range(2002, 2024), season_type=2, return_as_pandas=False)
        print(espnNbaCalendar)
        save_data_parquet(espnNbaCalendar, "espn_nba_calendar_2002_2024.parquet")
    except Exception as e:
        logger.error(f"Failed to load ESPN NBA calendar: {e}")


######################################################################################################
##                                              NFL Section                                         ##
######################################################################################################

def loadNflPbp():
    """
    NFL play by play data (1999-2024), return_as_pandas=False.
    Downloads and saves individual season files.
    """
    logger.info("sportsdataverse.nfl.load_nfl_pbp(seasons=range(1999,2024), return_as_pandas=False)")
    try:
        # Create necessary directories if they don't exist
        nfl_dir = os.path.join(BASE_DIR, "nfl")
        pbp_dir = os.path.join(nfl_dir, "nfl_pbp")
        os.makedirs(nfl_dir, exist_ok=True)
        os.makedirs(pbp_dir, exist_ok=True)

        # Process each season individually
        for season in range(1999, 2025):
            try:
                logger.info(f"Processing season {season}")
                season_data = sportsdataverse.nfl.load_nfl_pbp(seasons=[season], return_as_pandas=False)

                # Cast problematic columns to consistent types
                if "goal_to_go" in season_data.columns:
                    season_data = season_data.with_columns(
                        pl.col("goal_to_go").cast(pl.Int32)
                    )
                if "xyac_median_yardage" in season_data.columns:
                    season_data = season_data.with_columns(
                        pl.col("xyac_median_yardage").cast(pl.Float64)
                    )

                # Save individual season data
                season_filename = f"nfl_pbp_{season}.parquet"
                season_filepath = os.path.join(pbp_dir, season_filename)
                save_data_parquet(season_data, season_filepath)
                logger.info(f"Successfully saved season {season} to {season_filepath}")

            except Exception as e:
                logger.warning(f"Error processing season {season}: {e}")
                continue

        logger.info("Finished processing all NFL seasons")

    except Exception as e:
        logger.error(f"Failed to load NFL PBP data: {e}")

def loadNflPlayerStats():
    """
    NFL player stats data, return_as_pandas=False.
    """
    logger.info("sportsdataverse.nfl.load_nfl_player_stats(return_as_pandas=False)")
    try:
        nflPlayerStats = sportsdataverse.nfl.load_nfl_player_stats(return_as_pandas=False)
        print(nflPlayerStats)
        save_data_parquet(nflPlayerStats, "nfl_player_stats.parquet")
    except Exception as e:
        logger.error(f"Failed to load NFL player stats: {e}")


def loadNflRosters():
    """
    NFL roster data (1920-present), return_as_pandas=False.
    """
    try:
        # Create empty DataFrame with consistent schema
        combined_rosters = pl.DataFrame()

        for season in range(1920, 2025):
            try:
                season_data = sdv.load_nfl_rosters(seasons=[season], return_as_pandas=False)
                if combined_rosters.is_empty():
                    combined_rosters = season_data
                else:
                    # Get common columns between DataFrames
                    common_cols = list(set(combined_rosters.columns) & set(season_data.columns))
                    combined_rosters = pl.concat([
                        combined_rosters.select(common_cols),
                        season_data.select(common_cols)
                    ])
            except Exception as e:
                logger.warning(f"Error loading season {season}: {e}")
                continue

        save_data_parquet(combined_rosters, os.path.join(BASE_DIR, "nfl_rosters_1920_2023.parquet"))
    except Exception as e:
        logger.error(f"Failed to load NFL rosters: {e}")

def loadNflSchedules():
    """
    NFL schedule data (1999-present), return_as_pandas=False.
    """
    logger.info("sportsdataverse.nfl.load_nfl_schedule(seasons=range(1999,2024), return_as_pandas=False)")
    try:
        nflSchedules = sportsdataverse.nfl.load_nfl_schedule(seasons=range(1999, 2024), return_as_pandas=False)
        print(nflSchedules)
        save_data_parquet(nflSchedules, "nfl_schedule_1999_2023.parquet")
    except Exception as e:
        logger.error(f"Failed to load NFL schedule: {e}")


def loadNflTeams():
    """
    NFL team ID info & logos (Current), return_as_pandas=False.
    """
    logger.info("sportsdataverse.nfl.load_nfl_teams(return_as_pandas=False)")
    try:
        nflTeams = sportsdataverse.nfl.load_nfl_teams(return_as_pandas=False)
        print(nflTeams)
        save_data_parquet(nflTeams, "nfl_teams.parquet")
    except Exception as e:
        logger.error(f"Failed to load NFL teams: {e}")


def loadNflDraftPicks():
    """
    NFL draft picks data (All available), return_as_pandas=False.
    """
    logger.info("sportsdataverse.nfl.load_nfl_draft_picks(return_as_pandas=False)")
    try:
        nflDraftPicks = sportsdataverse.nfl.load_nfl_draft_picks(return_as_pandas=False)
        print(nflDraftPicks)
        save_data_parquet(nflDraftPicks, "nfl_draft_picks.parquet")
    except Exception as e:
        logger.error(f"Failed to load NFL draft picks: {e}")


def loadNflDepthCharts():
    """
    NFL depth charts (2001-present), return_as_pandas=False.
    """
    logger.info("sportsdataverse.nfl.load_nfl_depth_charts(seasons=range(2001,2024))")
    try:
        nflDepthCharts = sportsdataverse.nfl.load_nfl_depth_charts(seasons=range(2001, 2024))
        print(nflDepthCharts)
        save_data_parquet(nflDepthCharts, "nfl_depth_charts_2001_2023.parquet")
    except Exception as e:
        logger.error(f"Failed to load NFL depth charts: {e}")


def loadNflDriveCharts():
    """
    NFL drive charts for specified game_id, return_as_pandas=False.
    """
    logger.info("sportsdataverse.nfl.load_nfl_drive_charts(return_as_pandas=False)")
    try:
        nflDriveCharts = sportsdataverse.nfl.load_nfl_drive_charts(return_as_pandas=False)
        print(nflDriveCharts)
        save_data_parquet(nflDriveCharts, "nfl_drive_charts_401220403.parquet")
    except Exception as e:
        logger.error(f"Failed to load NFL drive charts: {e}")


def loadNflGameBooks():
    """
    NFL game books for specified game_id, return_as_pandas=False.
    """
    logger.info("sportsdataverse.nfl.load_nfl_game_books(game_id=401220403, return_as_pandas=False)")
    try:
        nflGameBooks = sportsdataverse.nfl.load_nfl_game_books(game_id=401220403, return_as_pandas=False)
        print(nflGameBooks)
        save_data_parquet(nflGameBooks, "nfl_game_books_401220403.parquet")
    except Exception as e:
        logger.error(f"Failed to load NFL game books: {e}")


def isNflPlayInProgress():
    """
    Placeholder. Implementation pending.
    """
    logger.info("Checking if NFL play is in progress - Implementation pending.")
    pass


def loadEspnNflPbp():
    """
    Pull ESPN NFL PBP data (1999-present), return_as_pandas=False.
    """
    logger.info("sportsdataverse.nfl.NFLPlayProcess(seasons=range(1999,2024), return_as_pandas=False).espn_nfl_pbp()")
    try:
        espnNflPbp = sportsdataverse.nfl.NFLPlayProcess(game_id=401220403, return_as_pandas=False).espn_nfl_pbp()
        print(espnNflPbp)
        save_data_parquet(espnNflPbp, "espn_nfl_pbp_401220403.parquet")
    except Exception as e:
        logger.error(f"Failed to load ESPN NFL PBP: {e}")

def loadEspnNflCalendar():
    """
    ESPN NFL calendar (2002-present), return_as_pandas=False.
    """
    logger.info("sportsdataverse.nfl.espn_nfl_calendar(season=2020, return_as_pandas=False)")
    try:
        espnNflCalendar = sportsdataverse.nfl.espn_nfl_calendar(season=2020, return_as_pandas=False)
        print(espnNflCalendar)
        save_data_parquet(espnNflCalendar, "espn_nfl_calendar_2020.parquet")
    except Exception as e:
        logger.error(f"Failed to load ESPN NFL calendar: {e}")


def loadEspnNflSchedule():
    """
    ESPN NFL schedule (2002-present), return_as_pandas=False.
    Weekly schedule data includes season_type:
    1=preseason, 2=regular season, 3=postseason, 4=offseason
    """
    logger.info("sportsdataverse.nfl.espn_nfl_schedule(dates=2020, week=1, season_type=2, return_as_pandas=False)")
    try:
        espnNflSchedule = sportsdataverse.nfl.espn_nfl_schedule(dates=2020, week=1, season_type=2, return_as_pandas=False)
        print(espnNflSchedule)
        save_data_parquet(espnNflSchedule, "espn_nfl_schedule_2020_week1.parquet")
    except Exception as e:
        logger.error(f"Failed to load ESPN NFL schedule: {e}")


def loadEspnNflTeamInfo():
    """
    ESPN NFL team information (Current), return_as_pandas=False.
    """
    logger.info("sportsdataverse.nfl.load_espn_nfl_team_info(return_as_pandas=False)")
    try:
        espnNflTeamInfo = sportsdataverse.nfl.load_espn_nfl_team_info(return_as_pandas=False)
        print(espnNflTeamInfo)
        save_data_parquet(espnNflTeamInfo, "espn_nfl_team_info.parquet")
    except Exception as e:
        logger.error(f"Failed to load ESPN NFL team info: {e}")


def loadEspnNflRosters():
    """
    ESPN NFL rosters (Current), return_as_pandas=False.
    """
    logger.info("sportsdataverse.nfl.load_espn_nfl_rosters(return_as_pandas=False)")
    try:
        espnNflRosters = sportsdataverse.nfl.load_espn_nfl_rosters(return_as_pandas=False)
        print(espnNflRosters)
        save_data_parquet(espnNflRosters, "espn_nfl_rosters.parquet")
    except Exception as e:
        logger.error(f"Failed to load ESPN NFL rosters: {e}")


def loadEspnNflStats():
    """
    ESPN NFL statistics (2002-present), return_as_pandas=False.
    """
    logger.info("sportsdataverse.nfl.load_espn_nfl_stats(season=2020, return_as_pandas=False)")
    try:
        espnNflStats = sportsdataverse.nfl.load_espn_nfl_stats(season=2020, return_as_pandas=False)
        print(espnNflStats)
        save_data_parquet(espnNflStats, "espn_nfl_stats_2020.parquet")
    except Exception as e:
        logger.error(f"Failed to load ESPN NFL stats: {e}")


def loadNflGameRosters():
    """
    NFL game rosters (2002-present), return_as_pandas=False.
    """
    logger.info("sportsdataverse.nfl.espn_nfl_game_rosters(gameId=401220403, return_as_pandas=False)")
    try:
        gameRosters = sportsdataverse.nfl.espn_nfl_game_rosters(gameId=401220403, return_as_pandas=False)
        print(gameRosters)
        save_data_parquet(gameRosters, "nfl_game_rosters_401220403.parquet")
    except Exception as e:
        logger.error(f"Failed to load NFL game rosters: {e}")


def loadNflWeeklyRosters():
    """
    NFL weekly rosters (2002-present), return_as_pandas=False.
    """
    logger.info("sportsdataverse.nfl.load_nfl_weekly_rosters(seasons=range(2002,2024), return_as_pandas=False)")
    try:
        weeklyRosters = sportsdataverse.nfl.load_nfl_weekly_rosters(seasons=range(2002, 2024), return_as_pandas=False)
        print(weeklyRosters)
        save_data_parquet(weeklyRosters, "nfl_weekly_rosters_2002_2023.parquet")
    except Exception as e:
        logger.error(f"Failed to load NFL weekly rosters: {e}")


def loadNflSnapCounts():
    """
    NFL snap counts (2012-present), return_as_pandas=False.
    """
    logger.info("sportsdataverse.nfl.load_nfl_snap_counts(seasons=range(2012,2024), return_as_pandas=False)")
    try:
        snapCounts = sportsdataverse.nfl.load_nfl_snap_counts(seasons=range(2012, 2024), return_as_pandas=False)
        print(snapCounts)
        save_data_parquet(snapCounts, "nfl_snap_counts_2012_2023.parquet")
    except Exception as e:
        logger.error(f"Failed to load NFL snap counts: {e}")


def loadNflPbpParticipation():
    """
    NFL play-by-play participation (2016-present), return_as_pandas=False.
    """
    logger.info("sportsdataverse.nfl.load_nfl_pbp_participation(seasons=range(2016,2024), return_as_pandas=False)")
    try:
        pbpParticipation = sportsdataverse.nfl.load_nfl_pbp_participation(seasons=range(2016, 2024), return_as_pandas=False)
        print(pbpParticipation)
        save_data_parquet(pbpParticipation, "nfl_pbp_participation_2016_2023.parquet")
    except Exception as e:
        logger.error(f"Failed to load NFL PBP participation data: {e}")


def loadNflCombine():
    """
    NFL Combine data (All available), return_as_pandas=False.
    """
    logger.info("sportsdataverse.nfl.load_nfl_combine(return_as_pandas=False)")
    try:
        combineData = sportsdataverse.nfl.load_nfl_combine(return_as_pandas=False)
        print(combineData)
        save_data_parquet(combineData, "nfl_combine.parquet")
    except Exception as e:
        logger.error(f"Failed to load NFL combine data: {e}")


def loadNflOfficialsData():
    """
    NFL Officials data (All available), return_as_pandas=False.
    """
    logger.info("sportsdataverse.nfl.load_nfl_officials(return_as_pandas=False)")
    try:
        officialsData = sportsdataverse.nfl.load_nfl_officials(return_as_pandas=False)
        print(officialsData)
        save_data_parquet(officialsData, "nfl_officials.parquet")
    except Exception as e:
        logger.error(f"Failed to load NFL officials data: {e}")


def loadNflNgsData():
    """
    NFL Next Gen Stats (2016-present), return_as_pandas=False.
    Includes passing, rushing, and receiving data.
    """
    try:
        # Passing NGS
        logger.info("Loading NFL NGS Passing Data (2016-present)")
        ngsPassingData = sportsdataverse.nfl.load_nfl_ngs_passing(seasons=range(2016, 2024), return_as_pandas=False)
        save_data_parquet(ngsPassingData, "nfl_ngs_passing_2016_2023.parquet")

        # Rushing NGS
        logger.info("Loading NFL NGS Rushing Data (2016-present)")
        ngsRushingData = sportsdataverse.nfl.load_nfl_ngs_rushing(seasons=range(2016, 2024), return_as_pandas=False)
        save_data_parquet(ngsRushingData, "nfl_ngs_rushing_2016_2023.parquet")

        # Receiving NGS
        logger.info("Loading NFL NGS Receiving Data (2016-present)")
        ngsReceivingData = sportsdataverse.nfl.load_nfl_ngs_receiving(seasons=range(2016, 2024), return_as_pandas=False)
        save_data_parquet(ngsReceivingData, "nfl_ngs_receiving_2016_2023.parquet")

    except Exception as e:
        logger.error(f"Failed to load NFL NGS data: {e}")


def loadNflPfrData():
    """
    NFL Pro Football Reference Data (2018-present for weekly data), return_as_pandas=False.
    Includes advanced stats for passing, rushing, receiving, and defensive categories.
    """
    try:
        # Advanced Passing
        logger.info("Loading NFL PFR Advanced Passing Data (Season)")
        pfrPassingData = sportsdataverse.nfl.load_nfl_pfr_pass(return_as_pandas=False)
        save_data_parquet(pfrPassingData, "nfl_pfr_season_passing.parquet")

        # Advanced Weekly Passing (2018-present)
        logger.info("Loading NFL PFR Weekly Passing Data (2018-present)")
        pfrWeeklyPassingData = sportsdataverse.nfl.load_nfl_pfr_weekly_pass(seasons=range(2018, 2024), return_as_pandas=False)
        save_data_parquet(pfrWeeklyPassingData, "nfl_pfr_weekly_passing_2018_2023.parquet")

        # Advanced Rushing
        logger.info("Loading NFL PFR Advanced Rushing Data (Season)")
        pfrRushingData = sportsdataverse.nfl.load_nfl_pfr_rush(return_as_pandas=False)
        save_data_parquet(pfrRushingData, "nfl_pfr_season_rushing.parquet")

        # Advanced Weekly Rushing (2018-present)
        logger.info("Loading NFL PFR Weekly Rushing Data (2018-present)")
        pfrWeeklyRushingData = sportsdataverse.nfl.load_nfl_pfr_weekly_rush(seasons=range(2018, 2024), return_as_pandas=False)
        save_data_parquet(pfrWeeklyRushingData, "nfl_pfr_weekly_rushing_2018_2023.parquet")

        # Advanced Receiving
        logger.info("Loading NFL PFR Advanced Receiving Data (Season)")
        pfrReceivingData = sportsdataverse.nfl.load_nfl_pfr_rec(return_as_pandas=False)
        save_data_parquet(pfrReceivingData, "nfl_pfr_season_receiving.parquet")

        # Advanced Weekly Receiving (2018-present)
        logger.info("Loading NFL PFR Weekly Receiving Data (2018-present)")
        pfrWeeklyReceivingData = sportsdataverse.nfl.load_nfl_pfr_weekly_rec(seasons=range(2018, 2024), return_as_pandas=False)
        save_data_parquet(pfrWeeklyReceivingData, "nfl_pfr_weekly_receiving_2018_2023.parquet")

        # Advanced Defensive
        logger.info("Loading NFL PFR Advanced Defensive Data (Season)")
        pfrDefensiveData = sportsdataverse.nfl.load_nfl_pfr_def(return_as_pandas=False)
        save_data_parquet(pfrDefensiveData, "nfl_pfr_season_defensive.parquet")

        # Advanced Weekly Defensive (2018-present)
        logger.info("Loading NFL PFR Weekly Defensive Data (2018-present)")
        pfrWeeklyDefensiveData = sportsdataverse.nfl.load_nfl_pfr_weekly_def(seasons=range(2018, 2024), return_as_pandas=False)
        save_data_parquet(pfrWeeklyDefensiveData, "nfl_pfr_weekly_defensive_2018_2023.parquet")

    except Exception as e:
        logger.error(f"Failed to load NFL PFR data: {e}")

######################################################################################################
##                                              NHL Section                                         ##
######################################################################################################
def loadNhlPbp():
    """
    NHL play by play data (2019–2020), return_as_pandas=False.
    """
    logger.info("sportsdataverse.nhl.load_nhl_pbp(seasons=range(2019,2021), return_as_pandas=False)")
    try:
        nhlPbp = sportsdataverse.nhl.load_nhl_pbp(seasons=range(2019, 2021), return_as_pandas=False)
        print(nhlPbp)
        save_data_parquet(nhlPbp, "nhl_pbp_2019_2020.parquet")
    except Exception as e:
        logger.error(f"Failed to load NHL PBP data: {e}")


def loadNhlPlayerStats():
    """
    NHL player stats data, return_as_pandas=False.
    """
    logger.info("sportsdataverse.nhl.load_nhl_player_stats(return_as_pandas=False)")
    try:
        nhlPlayerStats = sportsdataverse.nhl.load_nhl_player_stats(return_as_pandas=False)
        print(nhlPlayerStats)
        save_data_parquet(nhlPlayerStats, "nhl_player_stats.parquet")
    except Exception as e:
        logger.error(f"Failed to load NHL player stats: {e}")


def loadNhlSchedules():
    """
    NHL schedule data (2020), return_as_pandas=False.
    """
    logger.info("sportsdataverse.nhl.load_nhl_schedule(seasons=range(2020,2021), return_as_pandas=False)")
    try:
        nhlSchedules = sportsdataverse.nhl.load_nhl_schedule(seasons=range(2020, 2021), return_as_pandas=False)
        print(nhlSchedules)
        save_data_parquet(nhlSchedules, "nhl_schedule_2020.parquet")
    except Exception as e:
        logger.error(f"Failed to load NHL schedule: {e}")


def loadEspnNhlTeamInfo():
    """
    ESPN NHL team info/logos, return_as_pandas=False.
    """
    logger.info("sportsdataverse.nhl.espn_nhl_teams(return_as_pandas=False)")
    try:
        espnNhlTeamInfo = sportsdataverse.nhl.espn_nhl_teams(return_as_pandas=False)
        print(espnNhlTeamInfo)
        save_data_parquet(espnNhlTeamInfo, "espn_nhl_teams.parquet")
    except Exception as e:
        logger.error(f"Failed to load ESPN NHL team info: {e}")


def loadEspnNhlPbp():
    """
    ESPN NHL PBP for game_id=401247153, return_as_pandas=False.
    """
    logger.info("sportsdataverse.nhl.espn_nhl_pbp(game_id=401247153, return_as_pandas=False)")
    try:
        espnNhlPbp = sportsdataverse.nhl.espn_nhl_pbp(game_id=401247153, return_as_pandas=False)
        print(espnNhlPbp)
        save_data_parquet(espnNhlPbp, "espn_nhl_pbp_401247153.parquet")
    except Exception as e:
        logger.error(f"Failed to load ESPN NHL PBP: {e}")


def loadEspnNhlCalendar():
    """
    ESPN NHL calendar (2010), return_as_pandas=False.
    """
    logger.info("sportsdataverse.nhl.espn_nhl_calendar(season=2010, return_as_pandas=False)")
    try:
        espnNhlCalendar = sportsdataverse.nhl.espn_nhl_calendar(season=2010, return_as_pandas=False)
        print(espnNhlCalendar)
        save_data_parquet(espnNhlCalendar, "espn_nhl_calendar_2010.parquet")
    except Exception as e:
        logger.error(f"Failed to load ESPN NHL calendar: {e}")


def loadEspnNhlSchedule():
    """
    ESPN NHL schedule for 2010, return_as_pandas=False.
    """
    logger.info("sportsdataverse.nhl.espn_nhl_schedule(dates=2010, season_type=2, return_as_pandas=False)")
    try:
        espnNhlSchedule = sportsdataverse.nhl.espn_nhl_schedule(dates=2010, season_type=2, return_as_pandas=False)
        print(espnNhlSchedule)
        save_data_parquet(espnNhlSchedule, "espn_nhl_schedule_2010.parquet")
    except Exception as e:
        logger.error(f"Failed to load ESPN NHL schedule: {e}")


######################################################################################################
##                                              WBB Section                                         ##
######################################################################################################
def loadWbbPbp():
    """
    Women's college basketball PBP data, return_as_pandas=False.
    """
    logger.info("sportsdataverse.wbb.load_wbb_pbp(seasons=range(2020,2022), return_as_pandas=False)")
    try:
        wbbPbp = sportsdataverse.wbb.load_wbb_pbp(seasons=range(2020, 2022), return_as_pandas=False)
        print(wbbPbp)
        save_data_parquet(wbbPbp, "wbb_pbp_2020_2021.parquet")
    except Exception as e:
        logger.error(f"Failed to load WBB PBP data: {e}")


def loadWbbPlayerBoxscores():
    """
    Women's college basketball player boxscore data, return_as_pandas=False.
    """
    logger.info("sportsdataverse.wbb.load_wbb_player_boxscore(seasons=range(2020,2022), return_as_pandas=False)")
    try:
        wbbPlayerBoxscore = sportsdataverse.wbb.load_wbb_player_boxscore(seasons=range(2020, 2022), return_as_pandas=False)
        print(wbbPlayerBoxscore)
        save_data_parquet(wbbPlayerBoxscore, "wbb_player_boxscore_2020_2021.parquet")
    except Exception as e:
        logger.error(f"Failed to load WBB player boxscore data: {e}")


def loadWbbSchedule():
    """
    Women's college basketball schedule data, return_as_pandas=False.
    """
    logger.info("sportsdataverse.wbb.load_wbb_schedule(seasons=range(2020,2022), return_as_pandas=False)")
    try:
        wbbSchedule = sportsdataverse.wbb.load_wbb_schedule(seasons=range(2020, 2022), return_as_pandas=False)
        print(wbbSchedule)
        save_data_parquet(wbbSchedule, "wbb_schedule_2020_2021.parquet")
    except Exception as e:
        logger.error(f"Failed to load WBB schedule: {e}")


def loadWbbTeamBoxscores():
    """
    Women's college basketball team boxscore data, return_as_pandas=False.
    """
    logger.info("sportsdataverse.wbb.load_wbb_team_boxscore(seasons=range(2020,2022), return_as_pandas=False)")
    try:
        wbbTeamBoxscores = sportsdataverse.wbb.load_wbb_team_boxscore(seasons=range(2020, 2022), return_as_pandas=False)
        print(wbbTeamBoxscores)
        save_data_parquet(wbbTeamBoxscores, "wbb_team_boxscore_2020_2021.parquet")
    except Exception as e:
        logger.error(f"Failed to load WBB team boxscore data: {e}")


def loadWbbPbpModule():
    """
    Placeholder. There's nothing there, but the docs reference it. Implementation TBD.
    """
    logger.info("loadWbbPbpModule() - No data loaded; placeholder for future WBB PBP module.")
    pass


def loadEspnWbbPbp():
    """
    ESPN WBB PBP data (game_id=401266534), return_as_pandas=False.
    """
    logger.info("sportsdataverse.wbb.espn_wbb_pbp(game_id=401266534, return_as_pandas=False)")
    try:
        espnWbbPbp = sportsdataverse.wbb.espn_wbb_pbp(game_id=401266534, return_as_pandas=False)
        print(espnWbbPbp)
        save_data_parquet(espnWbbPbp, "espn_wbb_pbp_401266534.parquet")
    except Exception as e:
        logger.error(f"Failed to load ESPN WBB PBP: {e}")


def loadEspnWbbCalendar():
    """
    ESPN WBB calendar (2020), return_as_pandas=False.
    """
    logger.info("sportsdataverse.wbb.espn_wbb_calendar(season=2020, return_as_pandas=False)")
    try:
        espnWbbCalendar = sportsdataverse.wbb.espn_wbb_calendar(season=2020, return_as_pandas=False)
        print(espnWbbCalendar)
        save_data_parquet(espnWbbCalendar, "espn_wbb_calendar_2020.parquet")
    except Exception as e:
        logger.error(f"Failed to load ESPN WBB calendar: {e}")


def loadEspnWbbSchedule():
    """
    ESPN WBB schedule (date=2020, etc.), return_as_pandas=False.
    """
    logger.info("sportsdataverse.wbb.espn_wbb_schedule(dates=2020, groups=50, season_type=2, return_as_pandas=False)")
    try:
        espnWbbSchedule = sportsdataverse.wbb.espn_wbb_schedule(dates=2020, groups=50, season_type=2, return_as_pandas=False)
        print(espnWbbSchedule)
        save_data_parquet(espnWbbSchedule, "espn_wbb_schedule_2020.parquet")
    except Exception as e:
        logger.error(f"Failed to load ESPN WBB schedule: {e}")


######################################################################################################
##                                              WNBA Section                                         ##
######################################################################################################
def loadWnbaPbp():
    """
    WNBA PBP data (2020–2021), return_as_pandas=False.
    """
    logger.info("sportsdataverse.wnba.load_wnba_pbp(seasons=range(2020,2022), return_as_pandas=False)")
    try:
        wnba_df = sportsdataverse.wnba.load_wnba_pbp(seasons=range(2020, 2022), return_as_pandas=False)
        print(wnba_df)
        save_data_parquet(wnba_df, "wnba_pbp_2020_2021.parquet")
    except Exception as e:
        logger.error(f"Failed to load WNBA PBP data: {e}")


def loadWnbaPlayerBoxscores():
    """
    WNBA player boxscore data (2020–2021), return_as_pandas=False.
    """
    logger.info("sportsdataverse.wnba.load_wnba_player_boxscore(seasons=range(2020,2022), return_as_pandas=False)")
    try:
        wnbaPlayerBoxscore = sportsdataverse.wnba.load_wnba_player_boxscore(seasons=range(2020, 2022), return_as_pandas=False)
        print(wnbaPlayerBoxscore)
        save_data_parquet(wnbaPlayerBoxscore, "wnba_player_boxscore_2020_2021.parquet")
    except Exception as e:
        logger.error(f"Failed to load WNBA player boxscore data: {e}")


def loadWnbaSchedules():
    """
    WNBA schedule data (2020–2021), return_as_pandas=False.
    """
    logger.info("sportsdataverse.wnba.load_wnba_schedule(seasons=range(2020,2022), return_as_pandas=False)")
    try:
        wnbaSchedules = sportsdataverse.wnba.load_wnba_schedule(seasons=range(2020, 2022), return_as_pandas=False)
        print(wnbaSchedules)
        save_data_parquet(wnbaSchedules, "wnba_schedule_2020_2021.parquet")
    except Exception as e:
        logger.error(f"Failed to load WNBA schedule: {e}")


def loadWnbaTeamBoxscores():
    """
    WNBA team boxscore data (2020–2021), return_as_pandas=False.
    """
    logger.info("sportsdataverse.wnba.load_wnba_team_boxscore(seasons=range(2020,2022), return_as_pandas=False)")
    try:
        wnbaTeamBoxscores = sportsdataverse.wnba.load_wnba_team_boxscore(seasons=range(2020, 2022), return_as_pandas=False)
        print(wnbaTeamBoxscores)
        save_data_parquet(wnbaTeamBoxscores, "wnba_team_boxscore_2020_2021.parquet")
    except Exception as e:
        logger.error(f"Failed to load WNBA team boxscore data: {e}")


def loadEspnWnbaPbp():
    """
    Pull ESPN WNBA PBP data (game_id=401370395), return_as_pandas=False.
    """
    logger.info("sportsdataverse.wnba.espn_wnba_pbp(game_id=401370395, return_as_pandas=False)")
    try:
        espnWnbaPbp = sportsdataverse.wnba.espn_wnba_pbp(game_id=401370395, return_as_pandas=False)
        print(espnWnbaPbp)
        save_data_parquet(espnWnbaPbp, "espn_wnba_pbp_401370395.parquet")
    except Exception as e:
        logger.error(f"Failed to load ESPN WNBA PBP: {e}")


def loadEspnWnbaCalendar():
    """
    ESPN WNBA calendar (2020), return_as_pandas=False.
    """
    logger.info("sportsdataverse.wnba.espn_wnba_calendar(season=2020, return_as_pandas=False)")
    try:
        espnWnbaCalendar = sportsdataverse.wnba.espn_wnba_calendar(season=2020, return_as_pandas=False)
        print(espnWnbaCalendar)
        save_data_parquet(espnWnbaCalendar, "espn_wnba_calendar_2020.parquet")
    except Exception as e:
        logger.error(f"Failed to load ESPN WNBA calendar: {e}")


def loadEspnWnbaSchedule():
    """
    ESPN WNBA schedule (2020, season_type=2), return_as_pandas=False.
    """
    logger.info("sportsdataverse.wnba.espn_wnba_schedule(dates=2020, season_type=2, return_as_pandas=False)")
    try:
        espnWnbaSchedule = sportsdataverse.wnba.espn_wnba_schedule(dates=2020, season_type=2, return_as_pandas=False)
        print(espnWnbaSchedule)
        save_data_parquet(espnWnbaSchedule, "espn_wnba_schedule_2020.parquet")
    except Exception as e:
        logger.error(f"Failed to load ESPN WNBA schedule: {e}")


######################################################################################################
##                                               main()                                             ##
######################################################################################################
def main():
    logger.info("starting up")

    # Example calls - uncomment as needed
    # --- CFB ---
    #getCfbEspnTeams()
    #getEspnPbp()
    #getCfbPbp()
    #getEspnCfbCalendar()
    #getEspnSchedule()
    #loadCfbPbp()
    #loadCfbRosters()
    #loadCfbSchedule()
    #loadCfbTeamInfo()

    # --- MBB ---
    #loadMbbPlayerBoxscore()
    #loadMbbSchedule()
    #loadMbbTeamBoxscore()
    #loadEspnMbbPbp()
    #loadEspnMbbSchedule()

    # --- NBA ---
    #loadNbaPbp()
    #loadNbaPlayerBoxscores()
    #loadNbaSchedule()
    #loadNbaTeamBoxscores()
    #loadEspnNbaPbp()
    #loadEspnNbaCalendar()

    # --- NFL ---
    loadNflPbp()
    loadNflPlayerStats()
    #loadNflRosters()
    #loadNflSchedules()
    #loadNflTeams()
    #loadNflDraftPicks()
    #loadNflGameRosters()  # Added game rosters
    #loadNflWeeklyRosters()  # Added weekly rosters (2002-present)
    #loadNflDepthCharts()  # (2001-present)
    #loadNflDriveCharts()
    #loadNflGameBooks()
    #loadNflNgsData()  # Added Next Gen Stats (2016-present)
    #loadNflPfrData()  # Added Pro Football Reference data (2018-present)
    #loadNflSnapCounts()  # Added snap counts (2012-present)
    #loadNflPbpParticipation()  # Added PBP participation (2016-present)
    #loadNflCombine()  # Added combine data
    #loadNflOfficialsData()  # Added officials data
    #loadEspnNflPbp()
    #loadEspnNflCalendar()
    #loadEspnNflSchedule()
    #loadEspnNflTeamInfo()
    #loadEspnNflRosters()
    #loadEspnNflStats()
    #isNflPlayInProgress()  # Still placeholder

    # --- NHL ---
    #loadNhlPbp()
    #loadNhlPlayerStats()
    #loadNhlSchedules()
    #loadEspnNhlTeamInfo()
    #loadEspnNhlPbp()
    #loadEspnNhlCalendar()
    #loadEspnNhlSchedule()

    # --- WBB ---
    #loadWbbPlayerBoxscores()
    #loadWbbSchedule()
    #loadWbbTeamBoxscores()
    #loadWbbPbpModule()
    #loadEspnWbbPbp()
    #loadEspnWbbCalendar()
    #loadEspnWbbSchedule()

    # --- WNBA ---
    #loadWnbaPbp()
    #loadWnbaPlayerBoxscores()
    #loadWnbaSchedules()
    #loadWnbaTeamBoxscores()
    #loadEspnWnbaPbp()
    #loadEspnWnbaSchedule()

    logger.info("Finished main execution")


if __name__ == "__main__":
    main()