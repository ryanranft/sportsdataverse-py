import pandas as pd
import polars as pl
import json
from sportsdataverse.dl_utils import download, underscore

def espn_cfb_teams(groups=None, return_as_pandas = True, **kwargs) -> pd.DataFrame:
    """espn_cfb_teams - look up the college football teams

    Args:
        groups (int): Used to define different divisions. 80 is FBS, 81 is FCS.
        return_as_pandas (bool): If True, returns a pandas dataframe. If False, returns a polars dataframe.

    Returns:
        pd.DataFrame: Pandas dataframe containing schedule dates for the requested season.

    Example:
        `cfb_df = sportsdataverse.cfb.espn_cfb_teams()`

    """
    url = "http://site.api.espn.com/apis/site/v2/sports/football/college-football/teams"
    params = {
        "groups": groups if groups is not None else "80",
        "limit": 1000
    }
    resp = download(url=url, params = params, **kwargs)
    if resp is not None:
        events_txt = resp.json()

        teams = events_txt.get('sports')[0].get('leagues')[0].get('teams')
        del_keys = ['record', 'links']
        for team in teams:
            for k in del_keys:
                team.get('team').pop(k, None)
        teams = pd.json_normalize(teams, sep='_')
    teams.columns = [underscore(c) for c in teams.columns.tolist()]
    return teams if return_as_pandas else pl.from_pandas(teams)
