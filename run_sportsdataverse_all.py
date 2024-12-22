import sportsdataverse as sdv
import os
import logging

# Configure logging properties
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Set the output directory to save all data
BASE_DIR = "/Users/ryanranft/0/sportsdataverse/sportsdataverse-py/raw_data"


def save_data_parquet(data, filename):
    """
    Save data to a Parquet file within the raw_data directory.

    Args:
        data: DataFrame-like object (Polars or Pandas).
        filename: Name of the output file (string).
    """
    if data is None:
        logger.warning(f"No data to save for {filename}")
        return

    try:
        # Ensure the base directory exists
        os.makedirs(BASE_DIR, exist_ok=True)
        filepath = os.path.join(BASE_DIR, filename)

        # Pandas DataFrame (needs 'pyarrow' or 'fastparquet')
        if hasattr(data, "to_parquet"):
            data.to_parquet(filepath, index=False)
            logger.info(f"Saved data to {filepath}")

        # Polars DataFrame
        elif hasattr(data, "write_parquet"):
            data.write_parquet(filepath)
            logger.info(f"Saved data to {filepath}")

        else:
            logger.warning(f"Unsupported data type; could not save: {filename}")
    except Exception as e:
        logger.error(f"Failed to save data to {filename}: {e}")


######################################################################################################
##                                  Existing NFL/CFB/NBA calls for reference                        ##
######################################################################################################
def execute_nfl():
    """
    Execute NFL functions and save collected data (Parquet).
    """
    try:
        logger.info("Running NFL data collection")

        # NFL Play-by-Play Data
        pbp_data = sdv.load_nfl_pbp(seasons=[2022])
        save_data_parquet(pbp_data, "nfl_pbp_2022.parquet")

        # NFL Schedule
        schedule_data = sdv.load_nfl_schedule(seasons=[2022])
        save_data_parquet(schedule_data, "nfl_schedule_2022.parquet")

        # NFL Team Data
        teams_data = sdv.load_nfl_teams()
        save_data_parquet(teams_data, "nfl_teams.parquet")

        # NFL Player Stats (NGS Passing)
        ngs_passing = sdv.load_nfl_ngs_passing()
        save_data_parquet(ngs_passing, "nfl_ngs_passing.parquet")

        # NFL Player Stats (NGS Rushing)
        ngs_rushing = sdv.load_nfl_ngs_rushing()
        save_data_parquet(ngs_rushing, "nfl_ngs_rushing.parquet")

        logger.info("Finished running NFL data functions!")
    except Exception as e:
        logger.error(f"Error in NFL module: {e}")


def execute_cfb():
    """
    Execute College Football (CFB) functions and save collected data (Parquet).
    """
    try:
        logger.info("Running College Football data collection")

        # College Football Play-by-Play Data
        pbp_data = sdv.load_cfb_pbp(seasons=[2022])
        save_data_parquet(pbp_data, "cfb_pbp_2022.parquet")

        # CFB Schedule
        schedule_data = sdv.load_cfb_schedule(seasons=[2023])
        save_data_parquet(schedule_data, "cfb_schedule_2023.parquet")

        # CFB Rosters
        rosters_data = sdv.load_cfb_game_rosters(seasons=[2023])
        save_data_parquet(rosters_data, "cfb_rosters_2023.parquet")

        # CFB Teams
        teams_data = sdv.load_cfb_teams()
        save_data_parquet(teams_data, "cfb_teams.parquet")

        logger.info("Finished running CFB data functions!")
    except Exception as e:
        logger.error(f"Error in CFB module: {e}")


def execute_nba():
    """
    Execute NBA functions and save collected data (Parquet).
    """
    try:
        logger.info("Running NBA data collection")

        # NBA Play-by-Play Data
        pbp_data = sdv.load_nba_pbp(seasons=[2023])
        save_data_parquet(pbp_data, "nba_pbp_2023.parquet")

        # NBA Schedule
        schedule_data = sdv.load_nba_schedule(seasons=[2023])
        save_data_parquet(schedule_data, "nba_schedule_2023.parquet")

        # NBA Team Box Score Data
        box_score_data = sdv.load_nba_team_box(seasons=[2023])
        save_data_parquet(box_score_data, "nba_team_box_2023.parquet")

        logger.info("Finished running NBA data functions!")
    except Exception as e:
        logger.error(f"Error in NBA module: {e}")


def execute_remaining_sports():
    """
    Execute NHL, WBB, WNBA, and MBB functions and save collected data (Parquet).
    """
    try:
        logger.info("Running NHL, MBB, WBB, and WNBA data collection")

        # Example for NHL
        nhl_pbp = sdv.load_nhl_pbp(seasons=[2022])
        save_data_parquet(nhl_pbp, "nhl_pbp_2022.parquet")

        nhl_schedule = sdv.load_nhl_schedule(seasons=[2022])
        save_data_parquet(nhl_schedule, "nhl_schedule_2022.parquet")

        # More sports can be added systematically...
    except Exception as e:
        logger.error(f"Error while processing remaining sports: {e}")


######################################################################################################
##                                       Extended Data Wrappers                                     ##
######################################################################################################
import sportsdataverse

######################################################################################################
##                                              CFB Section                                         ##
######################################################################################################
def getCfbEspnTeams():
    """
    Example usage:
        getCfbEspnTeams()
    Load college football team ID information and logos, save as Parquet.
    """
    logger.info("sportsdataverse.cfb.espn_cfb_teams() - Load college football team ID information and logos")
    try:
        cfbEspnTeams_df = sportsdataverse.cfb.espn_cfb_teams()
        print(cfbEspnTeams_df)
        save_data_parquet(cfbEspnTeams_df, "cfb_espn_teams.parquet")
    except Exception as e:
        logger.error(f"Failed to load ESPN CFB teams: {e}")


def getEspnPbp():
    """
    Example usage:
        getEspnPbp()
    Pull the game by ID, data from ESPN CFB endpoints. Saves as Parquet.
    """
    logger.info("sportsdataverse.cfb.CFBPlayProcess(gameId=401256137).espn_cfb_pbp()")
    try:
        espnPbp = sportsdataverse.cfb.CFBPlayProcess(gameId=401256137).espn_cfb_pbp()
        print(espnPbp)
        save_data_parquet(espnPbp, "espn_cfb_pbp_401256137.parquet")
    except Exception as e:
        logger.error(f"Failed to load ESPN CFB PBP data: {e}")


def getCfbPbp():
    """
    Example usage:
        getCfbPbp()
    Load CFB PBP data (2009 season), save as Parquet.
    """
    logger.info("sportsdataverse.cfb.load_cfb_pbp(seasons=range(2009,2010)) - PBP Data")
    try:
        cfbPbp_df = sportsdataverse.cfb.load_cfb_pbp(seasons=range(2009, 2010))
        print(cfbPbp_df)
        save_data_parquet(cfbPbp_df, "cfb_pbp_2009.parquet")
    except Exception as e:
        logger.error(f"Failed to load CFB PBP data: {e}")


def getEspnCfbCalendar():
    """
    Example usage:
        getEspnCfbCalendar()
    Load ESPN CFB calendar info (2020), saves as Parquet.
    """
    logger.info("sportsdataverse.cfb.espn_cfb_calendar(season=2020, groups=80)")
    try:
        espnCfbCalendar = sportsdataverse.cfb.espn_cfb_calendar(season=2020, groups=80)
        print(espnCfbCalendar)
        save_data_parquet(espnCfbCalendar, "espn_cfb_calendar_2020.parquet")
    except Exception as e:
        logger.error(f"Failed to load ESPN CFB calendar: {e}")


def getEspnSchedule():
    """
    Example usage:
        getEspnSchedule()
    Load ESPN CFB schedule for a given week in 2020. Saves as Parquet.
    """
    logger.info("sportsdataverse.cfb.espn_cfb_schedule(dates=2020, week=5, season_type=2, groups=80)")
    try:
        espnSchedule = sportsdataverse.cfb.espn_cfb_schedule(dates=2020, week=5, season_type=2, groups=80)
        print(espnSchedule)
        save_data_parquet(espnSchedule, "espn_cfb_schedule_2020_week5.parquet")
    except Exception as e:
        logger.error(f"Failed to load ESPN CFB schedule: {e}")


def loadCfbPbp():
    """
    Example usage:
        loadCfbPbp()
    Load range-based CFB PBP data (2009). Saves as Parquet.
    """
    logger.info("sportsdataverse.cfb.load_cfb_pbp(seasons=range(2009,2010))")
    try:
        loadedCfbPbp = sportsdataverse.cfb.load_cfb_pbp(seasons=range(2009, 2010))
        print(loadedCfbPbp)
        save_data_parquet(loadedCfbPbp, "cfb_pbp_range_2009.parquet")
    except Exception as e:
        logger.error(f"Failed to load range-based CFB PBP data: {e}")


def loadCfbRosters():
    """
    Example usage:
        loadCfbRosters()
    Load CFB roster data (empty in this date range?), saves as Parquet.
    """
    logger.info("sportsdataverse.cfb.load_cfb_rosters(seasons=range(2014,2010)) - Potentially empty!")
    try:
        loadedCfbRosters = sportsdataverse.cfb.load_cfb_rosters(seasons=range(2014, 2010))
        print(loadedCfbRosters)
        save_data_parquet(loadedCfbRosters, "cfb_rosters_2014_to_2010.parquet")
    except Exception as e:
        logger.error(f"Failed to load CFB rosters: {e}")


def loadCfbSchedule():
    """
    Example usage:
        loadCfbSchedule()
    Load CFB schedule data (2009). Saves as Parquet.
    """
    logger.info("sportsdataverse.cfb.load_cfb_schedule(seasons=range(2009,2010))")
    try:
        loadedCfbSchedule = sportsdataverse.cfb.load_cfb_schedule(seasons=range(2009, 2010))
        print(loadedCfbSchedule)
        save_data_parquet(loadedCfbSchedule, "cfb_schedule_2009.parquet")
    except Exception as e:
        logger.error(f"Failed to load CFB schedule: {e}")


def loadCfbTeamInfo():
    """
    Example usage:
        loadCfbTeamInfo()
    Load CFB team info (2009). Saves as Parquet.
    """
    logger.info("sportsdataverse.cfb.load_cfb_team_info(seasons=range(2009,2010))")
    try:
        loadedCfbTeamInfo = sportsdataverse.cfb.load_cfb_team_info(seasons=range(2009, 2010))
        print(loadedCfbTeamInfo)
        save_data_parquet(loadedCfbTeamInfo, "cfb_team_info_2009.parquet")
    except Exception as e:
        logger.error(f"Failed to load CFB team info: {e}")


######################################################################################################
##                                              MBB Section                                         ##
######################################################################################################
def loadMbbPbp():
    """
    Example usage:
        loadMbbPbp()
    Load men's college basketball PBP data (2020–2021). Saves as Parquet.
    """
    logger.info("sportsdataverse.mbb.load_mbb_pbp(seasons=range(2020,2022))")
    try:
        mbbPbp = sportsdataverse.mbb.load_mbb_pbp(seasons=range(2020, 2022))
        print(mbbPbp)
        save_data_parquet(mbbPbp, "mbb_pbp_2020_2021.parquet")
    except Exception as e:
        logger.error(f"Failed to load MBB PBP data: {e}")


def loadMbbPlayerBoxscore():
    """
    Example usage:
        loadMbbPlayerBoxscore()
    Load men's college basketball player boxscore data (2020–2021). Saves as Parquet.
    """
    logger.info("sportsdataverse.mbb.load_mbb_player_boxscore(seasons=range(2020,2022))")
    try:
        mbbPlayerBoxscore = sportsdataverse.mbb.load_mbb_player_boxscore(seasons=range(2020, 2022))
        print(mbbPlayerBoxscore)
        save_data_parquet(mbbPlayerBoxscore, "mbb_player_boxscore_2020_2021.parquet")
    except Exception as e:
        logger.error(f"Failed to load MBB player boxscore data: {e}")


def loadMbbSchedule():
    """
    Example usage:
        loadMbbSchedule()
    Load men's college basketball schedule data (2020–2021). Saves as Parquet.
    """
    logger.info("sportsdataverse.mbb.load_mbb_schedule(seasons=range(2020,2022))")
    try:
        mbbSchedule = sportsdataverse.mbb.load_mbb_schedule(seasons=range(2020, 2022))
        print(mbbSchedule)
        save_data_parquet(mbbSchedule, "mbb_schedule_2020_2021.parquet")
    except Exception as e:
        logger.error(f"Failed to load MBB schedule: {e}")


def loadMbbTeamBoxscore():
    """
    Example usage:
        loadMbbTeamBoxscore()
    Load men's college basketball team boxscore data (2002–2021). Saves as Parquet.
    """
    logger.info("sportsdataverse.mbb.load_mbb_team_boxscore(seasons=range(2002,2022))")
    try:
        mbbTeamBoxscore = sportsdataverse.mbb.load_mbb_team_boxscore(seasons=range(2002, 2022))
        print(mbbTeamBoxscore)
        save_data_parquet(mbbTeamBoxscore, "mbb_team_boxscore_2002_2021.parquet")
    except Exception as e:
        logger.error(f"Failed to load MBB team boxscore data: {e}")


def loadEspnMbbPbp():
    """
    Example usage:
        loadEspnMbbPbp()
    Pull game data (ID=401265031) from ESPN endpoints. Saves as Parquet.
    """
    logger.info("sportsdataverse.mbb.espn_mbb_pbp(game_id=401265031)")
    try:
        espnMbbPbp = sportsdataverse.mbb.espn_mbb_pbp(game_id=401265031)
        print(espnMbbPbp)
        save_data_parquet(espnMbbPbp, "espn_mbb_pbp_401265031.parquet")
    except Exception as e:
        logger.error(f"Failed to load ESPN MBB PBP data: {e}")


def loadEspnMbbSchedule():
    """
    Example usage:
        loadEspnMbbSchedule()
    Lookup ESPN MBB schedule (2020), saves as Parquet.
    """
    logger.info("sportsdataverse.mbb.espn_mbb_schedule(dates=2020, groups=50, season_type=2)")
    try:
        espnMbbSchedule = sportsdataverse.mbb.espn_mbb_schedule(dates=2020, groups=50, season_type=2)
        print(espnMbbSchedule)
        save_data_parquet(espnMbbSchedule, "espn_mbb_schedule_2020.parquet")
    except Exception as e:
        logger.error(f"Failed to load ESPN MBB schedule: {e}")


######################################################################################################
##                                              NBA Section                                         ##
######################################################################################################
def loadNbaPbp():
    """
    Example usage:
        loadNbaPbp()
    Load NBA play by play data (2020–2021). Saves as Parquet.
    """
    logger.info("sportsdataverse.nba.load_nba_pbp(seasons=range(2020,2022))")
    try:
        nbaPbp = sportsdataverse.nba.load_nba_pbp(seasons=range(2020, 2022))
        print(nbaPbp)
        save_data_parquet(nbaPbp, "nba_pbp_2020_2021.parquet")
    except Exception as e:
        logger.error(f"Failed to load NBA PBP data: {e}")


def loadNbaPlayerBoxscores():
    """
    Example usage:
        loadNbaPlayerBoxscores()
    Load NBA player boxscore data (2020–2021). Saves as Parquet.
    """
    logger.info("sportsdataverse.nba.load_nba_player_boxscore(seasons=range(2020,2022))")
    try:
        nbaBoxscore = sportsdataverse.nba.load_nba_player_boxscore(seasons=range(2020, 2022))
        print(nbaBoxscore)
        save_data_parquet(nbaBoxscore, "nba_player_boxscore_2020_2021.parquet")
    except Exception as e:
        logger.error(f"Failed to load NBA player boxscore data: {e}")


def loadNbaSchedule():
    """
    Example usage:
        loadNbaSchedule()
    Load NBA schedule data (2020–2021). Saves as Parquet.
    """
    logger.info("sportsdataverse.nba.load_nba_schedule(seasons=range(2020,2022))")
    try:
        nbaSchedule = sportsdataverse.nba.load_nba_schedule(seasons=range(2020, 2022))
        print(nbaSchedule)
        save_data_parquet(nbaSchedule, "nba_schedule_2020_2021.parquet")
    except Exception as e:
        logger.error(f"Failed to load NBA schedule: {e}")


def loadNbaTeamBoxscores():
    """
    Example usage:
        loadNbaTeamBoxscores()
    Load NBA team boxscore data (2020–2021). Saves as Parquet.
    """
    logger.info("sportsdataverse.nba.load_nba_team_boxscore(seasons=range(2020,2022))")
    try:
        nbaTeamBoxscores = sportsdataverse.nba.load_nba_team_boxscore(seasons=range(2020, 2022))
        print(nbaTeamBoxscores)
        save_data_parquet(nbaTeamBoxscores, "nba_team_boxscore_2020_2021.parquet")
    except Exception as e:
        logger.error(f"Failed to load NBA team boxscore data: {e}")


def loadEspnNbaPbp():
    """
    Example usage:
        loadEspnNbaPbp()
    Pull ESPN NBA PBP for a game_id, saves as Parquet.
    """
    logger.info("sportsdataverse.nba.espn_nba_pbp(game_id=401307514)")
    try:
        espnNbaPbp = sportsdataverse.nba.espn_nba_pbp(game_id=401307514)
        print(espnNbaPbp)
        save_data_parquet(espnNbaPbp, "espn_nba_pbp_401307514.parquet")
    except Exception as e:
        logger.error(f"Failed to load ESPN NBA PBP data: {e}")


def loadEspnNbaCalendar():
    """
    Example usage:
        loadEspnNbaCalendar()
    Lookup the NBA calendar for given seasons from ESPN. Saves as Parquet.
    """
    logger.info("sportsdataverse.nba.espn_nba_schedule(dates=range(2020,2022), season_type=2)")
    try:
        espnNbaCalendar = sportsdataverse.nba.espn_nba_schedule(dates=range(2020, 2022), season_type=2)
        print(espnNbaCalendar)
        save_data_parquet(espnNbaCalendar, "espn_nba_calendar_2020_2021.parquet")
    except Exception as e:
        logger.error(f"Failed to load ESPN NBA calendar: {e}")


######################################################################################################
##                                              NFL Section                                         ##
######################################################################################################
def loadNflPbp():
    """
    Example usage:
        loadNflPbp()
    Load NFL play by play data (2019–2020). Saves as Parquet.
    """
    logger.info("sportsdataverse.nfl.load_nfl_pbp(seasons=range(2019,2021))")
    try:
        nflPbp = sportsdataverse.nfl.load_nfl_pbp(seasons=range(2019, 2021))
        print(nflPbp)
        save_data_parquet(nflPbp, "nfl_pbp_2019_2020.parquet")
    except Exception as e:
        logger.error(f"Failed to load NFL PBP data: {e}")


def loadNflPlayerStats():
    """
    Example usage:
        loadNflPlayerStats()
    Load NFL player stats data, saves as Parquet.
    """
    logger.info("sportsdataverse.nfl.load_nfl_player_stats()")
    try:
        nflPlayerStats = sportsdataverse.nfl.load_nfl_player_stats()
        print(nflPlayerStats)
        save_data_parquet(nflPlayerStats, "nfl_player_stats.parquet")
    except Exception as e:
        logger.error(f"Failed to load NFL player stats: {e}")


def loadNflRosters():
    """
    Example usage:
        loadNflRosters()
    Load NFL roster data for all seasons, saves as Parquet.
    """
    logger.info("sportsdataverse.nfl.load_nfl_rosters()")
    try:
        nflRosters = sportsdataverse.nfl.load_nfl_rosters()
        print(nflRosters)
        save_data_parquet(nflRosters, "nfl_rosters.parquet")
    except Exception as e:
        logger.error(f"Failed to load NFL rosters: {e}")


def loadNflSchedules():
    """
    Example usage:
        loadNflSchedules()
    Load NFL schedule data (2019–2020). Saves as Parquet.
    """
    logger.info("sportsdataverse.nfl.load_nfl_schedule(seasons=range(2019,2021))")
    try:
        nflSchedules = sportsdataverse.nfl.load_nfl_schedule(seasons=range(2019, 2021))
        print(nflSchedules)
        save_data_parquet(nflSchedules, "nfl_schedule_2019_2020.parquet")
    except Exception as e:
        logger.error(f"Failed to load NFL schedule: {e}")


def loadNflTeams():
    """
    Example usage:
        loadNflTeams()
    Load NFL team ID information & logos, saves as Parquet.
    """
    logger.info("sportsdataverse.nfl.load_nfl_teams()")
    try:
        nflTeams = sportsdataverse.nfl.load_nfl_teams()
        print(nflTeams)
        save_data_parquet(nflTeams, "nfl_teams.parquet")
    except Exception as e:
        logger.error(f"Failed to load NFL teams: {e}")


def isNflPlayInProgress():
    """
    Placeholder. Implementation pending.
    """
    logger.info("Checking if NFL play is in progress - Implementation pending.")
    pass


def loadEspnNflPbp():
    """
    Example usage:
        loadEspnNflPbp()
    Pull ESPN NFL PBP data for a given game_id. Saves as Parquet.
    """
    logger.info("sportsdataverse.nfl.NFLPlayProcess(game_id=401220403).espn_nfl_pbp()")
    try:
        espnNflPbp = sportsdataverse.nfl.NFLPlayProcess(game_id=401220403).espn_nfl_pbp()
        print(espnNflPbp)
        save_data_parquet(espnNflPbp, "espn_nfl_pbp_401220403.parquet")
    except Exception as e:
        logger.error(f"Failed to load ESPN NFL PBP: {e}")


def loadEspnNflCalendar():
    """
    Example usage:
        loadEspnNflCalendar()
    Look up the NFL calendar for a given season from ESPN, saves as Parquet.
    """
    logger.info("sportsdataverse.nfl.espn_nfl_calendar(season=2020)")
    try:
        espnNflCalendar = sportsdataverse.nfl.espn_nfl_calendar(season=2020)
        print(espnNflCalendar)
        save_data_parquet(espnNflCalendar, "espn_nfl_calendar_2020.parquet")
    except Exception as e:
        logger.error(f"Failed to load ESPN NFL calendar: {e}")


def loadEspnNflSchedule():
    """
    Example usage:
        loadEspnNflSchedule()
    Look up the NFL schedule for a given date from ESPN, saves as Parquet.
    """
    logger.info("sportsdataverse.nfl.espn_nfl_schedule(dates=2020, week=1, season_type=2)")
    try:
        espnNflSchedule = sportsdataverse.nfl.espn_nfl_schedule(dates=2020, week=1, season_type=2)
        print(espnNflSchedule)
        save_data_parquet(espnNflSchedule, "espn_nfl_schedule_2020_week1.parquet")
    except Exception as e:
        logger.error(f"Failed to load ESPN NFL schedule: {e}")


######################################################################################################
##                                              NHL Section                                         ##
######################################################################################################
def loadNhlPbp():
    """
    Example usage:
        loadNhlPbp()
    Load NHL play by play data (2019–2020). Saves as Parquet.
    """
    logger.info("sportsdataverse.nhl.load_nhl_pbp(seasons=range(2019,2021))")
    try:
        nhlPbp = sportsdataverse.nhl.load_nhl_pbp(seasons=range(2019, 2021))
        print(nhlPbp)
        save_data_parquet(nhlPbp, "nhl_pbp_2019_2020.parquet")
    except Exception as e:
        logger.error(f"Failed to load NHL PBP data: {e}")


def loadNhlPlayerStats():
    """
    Example usage:
        loadNhlPlayerStats()
    Load NHL player stats data, saves as Parquet.
    """
    logger.info("sportsdataverse.nhl.load_nhl_player_stats()")
    try:
        nhlPlayerStats = sportsdataverse.nhl.load_nhl_player_stats()
        print(nhlPlayerStats)
        save_data_parquet(nhlPlayerStats, "nhl_player_stats.parquet")
    except Exception as e:
        logger.error(f"Failed to load NHL player stats: {e}")


def loadNhlSchedules():
    """
    Example usage:
        loadNhlSchedules()
    Load NHL schedule data (2020). Saves as Parquet.
    """
    logger.info("sportsdataverse.nhl.load_nhl_schedule(seasons=range(2020,2021))")
    try:
        nhlSchedules = sportsdataverse.nhl.load_nhl_schedule(seasons=range(2020, 2021))
        print(nhlSchedules)
        save_data_parquet(nhlSchedules, "nhl_schedule_2020.parquet")
    except Exception as e:
        logger.error(f"Failed to load NHL schedule: {e}")


def loadEspnNhlTeamInfo():
    """
    Example usage:
        loadEspnNhlTeamInfo()
    Load NHL team ID information and logos from ESPN. Saves as Parquet.
    """
    logger.info("sportsdataverse.nhl.espn_nhl_teams()")
    try:
        espnNhlTeamInfo = sportsdataverse.nhl.espn_nhl_teams()
        print(espnNhlTeamInfo)
        save_data_parquet(espnNhlTeamInfo, "espn_nhl_teams.parquet")
    except Exception as e:
        logger.error(f"Failed to load ESPN NHL team info: {e}")


def loadEspnNhlPbp():
    """
    Example usage:
        loadEspnNhlPbp()
    Pull ESPN NHL PBP data for a game_id (401247153). Saves as Parquet.
    """
    logger.info("sportsdataverse.nhl.espn_nhl_pbp(game_id=401247153)")
    try:
        espnNhlPbp = sportsdataverse.nhl.espn_nhl_pbp(game_id=401247153)
        print(espnNhlPbp)
        save_data_parquet(espnNhlPbp, "espn_nhl_pbp_401247153.parquet")
    except Exception as e:
        logger.error(f"Failed to load ESPN NHL PBP: {e}")


def loadEspnNhlCalendar():
    """
    Example usage:
        loadEspnNhlCalendar()
    Look up the NHL calendar for 2010 from ESPN, saves as Parquet.
    """
    logger.info("sportsdataverse.nhl.espn_nhl_calendar(season=2010)")
    try:
        espnNhlCalendar = sportsdataverse.nhl.espn_nhl_calendar(season=2010)
        print(espnNhlCalendar)
        save_data_parquet(espnNhlCalendar, "espn_nhl_calendar_2010.parquet")
    except Exception as e:
        logger.error(f"Failed to load ESPN NHL calendar: {e}")


def loadEspnNhlSchedule():
    """
    Example usage:
        loadEspnNhlSchedule()
    Look up the NHL schedule for a given date from ESPN, saves as Parquet.
    """
    logger.info("sportsdataverse.nhl.espn_nhl_schedule(dates=2010, season_type=2)")
    try:
        espnNhlSchedule = sportsdataverse.nhl.espn_nhl_schedule(dates=2010, season_type=2)
        print(espnNhlSchedule)
        save_data_parquet(espnNhlSchedule, "espn_nhl_schedule_2010.parquet")
    except Exception as e:
        logger.error(f"Failed to load ESPN NHL schedule: {e}")


######################################################################################################
##                                              WBB Section                                         ##
######################################################################################################
def loadWbbPbp():
    """
    Example usage:
        loadWbbPbp()
    Load women's college basketball play by play data, saves as Parquet.
    """
    logger.info("sportsdataverse.wbb.load_wbb_pbp(seasons=range(2020,2022))")
    try:
        wbbPbp = sportsdataverse.wbb.load_wbb_pbp(seasons=range(2020, 2022))
        print(wbbPbp)
        save_data_parquet(wbbPbp, "wbb_pbp_2020_2021.parquet")
    except Exception as e:
        logger.error(f"Failed to load WBB PBP data: {e}")


def loadWbbPlayerBoxscores():
    """
    Example usage:
        loadWbbPlayerBoxscores()
    Load women's college basketball player boxscore data, saves as Parquet.
    """
    logger.info("sportsdataverse.wbb.load_wbb_player_boxscore(seasons=range(2020,2022))")
    try:
        wbbPlayerBoxscore = sportsdataverse.wbb.load_wbb_player_boxscore(seasons=range(2020, 2022))
        print(wbbPlayerBoxscore)
        save_data_parquet(wbbPlayerBoxscore, "wbb_player_boxscore_2020_2021.parquet")
    except Exception as e:
        logger.error(f"Failed to load WBB player boxscore data: {e}")


def loadWbbSchedule():
    """
    Example usage:
        loadWbbSchedule()
    Load women's college basketball schedule data, saves as Parquet.
    """
    logger.info("sportsdataverse.wbb.load_wbb_schedule(seasons=range(2020,2022))")
    try:
        wbbSchedule = sportsdataverse.wbb.load_wbb_schedule(seasons=range(2020, 2022))
        print(wbbSchedule)
        save_data_parquet(wbbSchedule, "wbb_schedule_2020_2021.parquet")
    except Exception as e:
        logger.error(f"Failed to load WBB schedule: {e}")


def loadWbbTeamBoxscores():
    """
    Example usage:
        loadWbbTeamBoxscores()
    Load women's college basketball team boxscore data, saves as Parquet.
    """
    logger.info("sportsdataverse.wbb.load_wbb_team_boxscore(seasons=range(2020,2022))")
    try:
        wbbTeamBoxscores = sportsdataverse.wbb.load_wbb_team_boxscore(seasons=range(2020, 2022))
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
    Example usage:
        loadEspnWbbPbp()
    Pull ESPN WBB PBP data (game_id=401266534). Saves as Parquet.
    """
    logger.info("sportsdataverse.wbb.espn_wbb_pbp(game_id=401266534)")
    try:
        espnWbbPbp = sportsdataverse.wbb.espn_wbb_pbp(game_id=401266534)
        print(espnWbbPbp)
        save_data_parquet(espnWbbPbp, "espn_wbb_pbp_401266534.parquet")
    except Exception as e:
        logger.error(f"Failed to load ESPN WBB PBP: {e}")


def loadEspnWbbCalendar():
    """
    Example usage:
        loadEspnWbbCalendar()
    Look up the women's college basketball calendar for 2020 from ESPN, saves as Parquet.
    """
    logger.info("sportsdataverse.wbb.espn_wbb_calendar(season=2020)")
    try:
        espnWbbCalendar = sportsdataverse.wbb.espn_wbb_calendar(season=2020)
        print(espnWbbCalendar)
        save_data_parquet(espnWbbCalendar, "espn_wbb_calendar_2020.parquet")
    except Exception as e:
        logger.error(f"Failed to load ESPN WBB calendar: {e}")


def loadEspnWbbSchedule():
    """
    Example usage:
        loadEspnWbbSchedule()
    Look up the women's college basketball schedule for a given season from ESPN, saves as Parquet.
    """
    logger.info("sportsdataverse.wbb.espn_wbb_schedule(dates=2020, groups=50, season_type=2)")
    try:
        espnWbbSchedule = sportsdataverse.wbb.espn_wbb_schedule(dates=2020, groups=50, season_type=2)
        print(espnWbbSchedule)
        save_data_parquet(espnWbbSchedule, "espn_wbb_schedule_2020.parquet")
    except Exception as e:
        logger.error(f"Failed to load ESPN WBB schedule: {e}")


#######################################################################################################
##                                              WNBA Section                                         ##
#######################################################################################################
def loadWnbaPbp():
    """
    Example usage:
        loadWnbaPbp()
    Load WNBA play by play data (2020–2021). Saves as Parquet.
    """
    logger.info("sportsdataverse.wnba.load_wnba_pbp(seasons=range(2020,2022))")
    try:
        wnba_df = sportsdataverse.wnba.load_wnba_pbp(seasons=range(2020, 2022))
        print(wnba_df)
        save_data_parquet(wnba_df, "wnba_pbp_2020_2021.parquet")
    except Exception as e:
        logger.error(f"Failed to load WNBA PBP data: {e}")


def loadWnbaPlayerBoxscores():
    """
    Example usage:
        loadWnbaPlayerBoxscores()
    Load WNBA player boxscore data (2020–2021). Saves as Parquet.
    """
    logger.info("sportsdataverse.wnba.load_wnba_player_boxscore(seasons=range(2020,2022))")
    try:
        wnbaPlayerBoxscore = sportsdataverse.wnba.load_wnba_player_boxscore(seasons=range(2020, 2022))
        print(wnbaPlayerBoxscore)
        save_data_parquet(wnbaPlayerBoxscore, "wnba_player_boxscore_2020_2021.parquet")
    except Exception as e:
        logger.error(f"Failed to load WNBA player boxscore data: {e}")


def loadWnbaSchedules():
    """
    Example usage:
        loadWnbaSchedules()
    Load WNBA schedule data (2020–2021). Saves as Parquet.
    """
    logger.info("sportsdataverse.wnba.load_wnba_schedule(seasons=range(2020,2022))")
    try:
        wnbaSchedules = sportsdataverse.wnba.load_wnba_schedule(seasons=range(2020, 2022))
        print(wnbaSchedules)
        save_data_parquet(wnbaSchedules, "wnba_schedule_2020_2021.parquet")
    except Exception as e:
        logger.error(f"Failed to load WNBA schedule: {e}")


def loadWnbaTeamBoxscores():
    """
    Example usage:
        loadWnbaTeamBoxscores()
    Load WNBA team boxscore data (2020–2021). Saves as Parquet.
    """
    logger.info("sportsdataverse.wnba.load_wnba_team_boxscore(seasons=range(2020,2022))")
    try:
        wnbaTeamBoxscores = sportsdataverse.wnba.load_wnba_team_boxscore(seasons=range(2020, 2022))
        print(wnbaTeamBoxscores)
        save_data_parquet(wnbaTeamBoxscores, "wnba_team_boxscore_2020_2021.parquet")
    except Exception as e:
        logger.error(f"Failed to load WNBA team boxscore data: {e}")


def loadEspnWnbaPbp():
    """
    Example usage:
        loadEspnWnbaPbp()
    Pull ESPN WNBA PBP data (game_id=401370395). Saves as Parquet.
    """
    logger.info("sportsdataverse.wnba.espn_wnba_pbp(game_id=401370395)")
    try:
        espnWnbaPbp = sportsdataverse.wnba.espn_wnba_pbp(game_id=401370395)
        print(espnWnbaPbp)
        save_data_parquet(espnWnbaPbp, "espn_wnba_pbp_401370395.parquet")
    except Exception as e:
        logger.error(f"Failed to load ESPN WNBA PBP: {e}")


def loadEspnWnbaCalendar():
    """
    Example usage:
        loadEspnWnbaCalendar()
    Look up the WNBA calendar for 2020 from ESPN, saves as Parquet.
    """
    logger.info("sportsdataverse.wnba.espn_wnba_calendar(season=2020)")
    try:
        espnWnbaCalendar = sportsdataverse.wnba.espn_wnba_calendar(season=2020)
        print(espnWnbaCalendar)
        save_data_parquet(espnWnbaCalendar, "espn_wnba_calendar_2020.parquet")
    except Exception as e:
        logger.error(f"Failed to load ESPN WNBA calendar: {e}")


def loadEspnWnbaSchedule():
    """
    Example usage:
        loadEspnWnbaSchedule()
    Look up the WNBA schedule for a given date from ESPN, saves as Parquet.
    """
    logger.info("sportsdataverse.wnba.espn_wnba_schedule(dates=2020, season_type=2)")
    try:
        espnWnbaSchedule = sportsdataverse.wnba.espn_wnba_schedule(dates=2020, season_type=2)
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
    getCfbEspnTeams()
    # getEspnPbp()
    # getCfbPbp()
    # getEspnCfbCalendar()
    # getEspnSchedule()
    # loadCfbPbp()
    # loadCfbRosters()
    # loadCfbSchedule()
    # loadCfbTeamInfo()

    # --- MBB ---
    # loadMbbPbp()
    # loadMbbPlayerBoxscore()
    # loadMbbSchedule()
    # loadMbbTeamBoxscore()
    # loadEspnMbbPbp()
    # loadEspnMbbSchedule()

    # --- NBA ---
    # loadNbaPbp()
    # loadNbaPlayerBoxscores()
    # loadNbaSchedule()
    # loadNbaTeamBoxscores()
    # loadEspnNbaPbp()
    # loadEspnNbaCalendar()

    # --- NFL ---
    # loadNflPbp()
    # loadNflPlayerStats()
    # loadNflRosters()
    # loadNflSchedules()
    # loadNflTeams()
    # isNflPlayInProgress()
    # loadEspnNflPbp()
    # loadEspnNflCalendar()
    # loadEspnNflSchedule()

    # --- NHL ---
    # loadNhlPbp()
    # loadNhlPlayerStats()
    # loadNhlSchedules()
    # loadEspnNhlTeamInfo()
    # loadEspnNhlPbp()
    # loadEspnNhlCalendar()
    # loadEspnNhlSchedule()

    # --- WBB ---
    # loadWbbPbp()
    # loadWbbPlayerBoxscores()
    # loadWbbSchedule()
    # loadWbbTeamBoxscores()
    # loadWbbPbpModule()
    # loadEspnWbbPbp()
    # loadEspnWbbCalendar()
    # loadEspnWbbSchedule()

    # --- WNBA ---
    # loadWnbaPbp()
    # loadWnbaPlayerBoxscores()
    # loadWnbaSchedules()
    # loadWnbaTeamBoxscores()
    # loadEspnWnbaPbp()
    # loadEspnWnbaCalendar()
    # loadEspnWnbaSchedule()

    logger.info("Finished main execution")


if __name__ == "__main__":
    main()