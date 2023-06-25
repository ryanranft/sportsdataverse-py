import pandas as pd
import polars as pl
import json
import time
import datetime
from sportsdataverse.dl_utils import download, underscore

def espn_cfb_schedule(dates=None, week=None, season_type=None, groups=None, limit=500,
                      **kwargs) -> pd.DataFrame:
    """espn_cfb_schedule - look up the college football schedule for a given season

    Args:
        dates (int): Used to define different seasons. 2002 is the earliest available season.
        week (int): Week of the schedule.
        groups (int): Used to define different divisions. 80 is FBS, 81 is FCS.
        season_type (int): 2 for regular season, 3 for post-season, 4 for off-season.
        limit (int): number of records to return, default: 500.

    Returns:
        pd.DataFrame: Pandas dataframe containing schedule dates for the requested season.
    """

    cache_buster = int(time.time() * 1000)
    cache_buster_url = f'&{cache_buster}'
    params = {
        'week': week,
        'dates': dates,
        'seasontype': season_type,
        'groups': groups if groups is not None else '80',
        'limit': limit
    }

    url = "http://site.api.espn.com/apis/site/v2/sports/football/college-football/scoreboard"
    resp = download(url=url, params=params, **kwargs)

    ev = pl.DataFrame()
    if resp is not None:
        events_txt = resp.json()
        events = events_txt.get('events')
        if len(events) == 0:
            ev = pd.DataFrame()
            return ev
        for event in events:
            event.get('competitions')[0].get('competitors')[0].get('team').pop('links',None)
            event.get('competitions')[0].get('competitors')[1].get('team').pop('links',None)
            if event.get('competitions')[0].get('competitors')[0].get('homeAway') == 'home':
                event = _extract_home_away_from_espn_cfb_schedule(event, 0, 'home')
                event = _extract_home_away_from_espn_cfb_schedule(event, 1, 'away')
            else:
                event = _extract_home_away_from_espn_cfb_schedule(event, 0, 'away')
                event = _extract_home_away_from_espn_cfb_schedule(event, 1, 'home')
            del_keys = ['geoBroadcasts', 'headlines', 'series', 'situation', 'tickets', 'odds']
            for k in del_keys:
                event.get('competitions')[0].pop(k, None)
            if len(event.get('competitions')[0]['notes']) > 0:
                event.get('competitions')[0]['notes_type'] = event.get('competitions')[0]['notes'][0].get("type")
                event.get('competitions')[0]['notes_headline'] = event.get('competitions')[0]['notes'][0].get("headline").replace('"','')
            else:
                event.get('competitions')[0]['notes_type'] = ''
                event.get('competitions')[0]['notes_headline'] = ''
            if len(event.get('competitions')[0].get('broadcasts')) > 0:
                event.get('competitions')[0]['broadcast_market'] = event.get('competitions')[0].get('broadcasts', [])[0].get('market', "")
                event.get('competitions')[0]['broadcast_name'] = event.get('competitions')[0].get('broadcasts', [])[0].get('names', [])[0]
            else:
                event.get('competitions')[0]['broadcast_market'] = ""
                event.get('competitions')[0]['broadcast_name'] = ""
            event.get('competitions')[0].pop('broadcasts', None)
            event.get('competitions')[0].pop('notes', None)
            event.get('competitions')[0].pop('competitors', None)
            x = pd.json_normalize(event.get('competitions')[0], sep = '_')
            x = pl.from_pandas(x)
            x = x.with_columns(
                game_id = (pl.col('id').cast(pl.Int64)),
                season = (event.get('season').get('year')),
                season_type = (event.get('season').get('type')),
                week = (event.get('week', {}).get('number')),
                home_linescores = pl.when(pl.col('status_type_description') == 'Postponed').then(None).otherwise(pl.col('home_linescores')),
                away_linescores = pl.when(pl.col('status_type_description') == 'Postponed').then(None).otherwise(pl.col('away_linescores')),
            )
            x = x[[s.name for s in x if s.null_count() != x.height]]
            # x['game_id'] = x['id'].cast(pl.Int64)
            # x['season'] = event.get('season').get('year')
            # x['season_type'] = event.get('season').get('type')
            # x['week'] = event.get('week', {}).get('number')

            ev = pl.concat([ev, x], how = 'diagonal')

    ev.columns = [underscore(c) for c in ev.columns]

    return ev.to_pandas()


# TODO Rename this here and in `espn_cfb_schedule`
def _extract_home_away_from_espn_cfb_schedule(event, arg1, arg2):
    event['competitions'][0][arg2] = (
        event.get('competitions')[0].get('competitors')[arg1].get('team')
    )
    event['competitions'][0][arg2]['score'] = (
        event.get('competitions')[0].get('competitors')[arg1].get('score')
    )
    event['competitions'][0][arg2]['winner'] = (
        event.get('competitions')[0].get('competitors')[arg1].get('winner')
    )
    ## add winner back to main competitors if does not exist
    event['competitions'][0]['competitors'][arg1]['winner'] = (
        event.get('competitions')[0].get('competitors')[arg1].get('winner', False)
    )
    event['competitions'][0][arg2]['currentRank'] = (
        event.get('competitions')[0]
        .get('competitors')[arg1]
        .get('curatedRank', {})
        .get('current', 99)
    )
    event['competitions'][0][arg2]['linescores'] = (
        event.get('competitions')[0]
        .get('competitors')[arg1]
        .get('linescores', [{'value': 'N/A'}])
    )
    ## add linescores back to main competitors if does not exist
    event['competitions'][0]['competitors'][arg1]['linescores'] = (
        event.get('competitions')[0]
        .get('competitors')[arg1]
        .get('linescores', [])
    )
    event['competitions'][0][arg2]['records'] = (
        event.get('competitions')[0]
        .get('competitors')[arg1]
        .get('records', [])
    )
    return event



def espn_cfb_calendar(season = None, groups = None, ondays = None, **kwargs) -> pd.DataFrame:
    """espn_cfb_calendar - look up the men's college football calendar for a given season

    Args:
        season (int): Used to define different seasons. 2002 is the earliest available season.
        groups (int): Used to define different divisions. 80 is FBS, 81 is FCS.
        ondays (boolean): Used to return dates for calendar ondays

    Returns:
        pd.DataFrame: Pandas dataframe containing calendar dates for the requested season.

    Raises:
        ValueError: If `season` is less than 2002.
    """
    if ondays is not None:
        full_schedule = _ondays_from_espn_cfb_calendar(season)
    else:
        params = {
            'dates': season,
            'groups': groups if groups is not None else '80'
        }
        url = "http://site.api.espn.com/apis/site/v2/sports/football/college-football/scoreboard"
        resp = download(url = url, params = params, **kwargs)
        txt = resp.json()
        txt = txt.get('leagues')[0].get('calendar')
        full_schedule = pl.DataFrame()
        for i in range(len(txt)):
            if txt[i].get('entries', None) is not None:
                reg = pd.json_normalize(data = txt[i],
                                        record_path = 'entries',
                                        meta=["label","value","startDate","endDate"],
                                        meta_prefix='season_type_',
                                        record_prefix='week_',
                                        errors="ignore",
                                        sep='_')
                full_schedule = pl.concat([full_schedule, pl.from_pandas(reg)], how = 'vertical')
        full_schedule = full_schedule.with_columns(
            season = season
        )
        full_schedule.columns = [underscore(c) for c in full_schedule.columns]
        full_schedule = full_schedule.rename({"week_value": "week", "season_type_value": "season_type"})
    return full_schedule.to_pandas()


def _ondays_from_espn_cfb_calendar(season):
    url = f"https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/{season}/types/2/calendar/ondays"
    resp = download(url=url)
    if resp is not None:
        txt = resp.json().get('eventDate').get('dates')
        result = pl.DataFrame(txt, schema=['dates'])
        result = result.with_columns(dateURL = pl.col('dates').str.slice(0, 10))
        result = result.with_columns(
            url="http://site.api.espn.com/apis/site/v2/sports/football/college-football/scoreboard?dates="
            + pl.col('dateURL')
        )

    return result

def most_recent_cfb_season():
    date = datetime.datetime.now()
    if date.month >= 8 and date.day >= 15:
        return date.year
    elif date.month >= 9:
        return date.year
    else:
        return date.year - 1


# def espn_cfb_schedule_pandas(dates=None, week=None, season_type=None, groups=None, limit=500) -> pd.DataFrame:
#     """espn_cfb_schedule - look up the college football schedule for a given season

#     Args:
#         dates (int): Used to define different seasons. 2002 is the earliest available season.
#         week (int): Week of the schedule.
#         groups (int): Used to define different divisions. 80 is FBS, 81 is FCS.
#         season_type (int): 2 for regular season, 3 for post-season, 4 for off-season.
#         limit (int): number of records to return, default: 500.

#     Returns:
#         pd.DataFrame: Pandas dataframe containing schedule dates for the requested season.
#     """
#     if week is None:
#         week = ''
#     else:
#         week = '&week=' + str(week)
#     if dates is None:
#         dates = ''
#     else:
#         dates = '&dates=' + str(dates)
#     if season_type is None:
#         season_type = ''
#     else:
#         season_type = '&seasontype=' + str(season_type)
#     if groups is None:
#         groups = '&groups=80'
#     else:
#         groups = '&groups=' + str(groups)
#     if limit is None:
#         limit_url = ''
#     else:
#         limit_url = '&limit=' + str(limit)
#     cache_buster = int(time.time() * 1000)
#     cache_buster_url = '&'+str(cache_buster)
#     url = "http://site.api.espn.com/apis/site/v2/sports/football/college-football/scoreboard?{}{}{}{}{}{}".format(
#         limit_url,
#         groups,
#         dates,
#         week,
#         season_type,
#         cache_buster_url
#     )
#     resp = download(url=url)

#     ev = pd.DataFrame()
#     if resp is not None:
#         events_txt = resp.json()
#         events = events_txt.get('events')
#         for event in events:
#             event.get('competitions')[0].get('competitors')[0].get('team').pop('links',None)
#             event.get('competitions')[0].get('competitors')[1].get('team').pop('links',None)
#             if event.get('competitions')[0].get('competitors')[0].get('homeAway')=='home':
#                 event['competitions'][0]['home'] = event.get('competitions')[0].get('competitors')[0].get('team')
#                 event['competitions'][0]['home']['score'] = event.get('competitions')[0].get('competitors')[0].get('score')
#                 event['competitions'][0]['home']['winner'] = event.get('competitions')[0].get('competitors')[0].get('winner')
#                 event['competitions'][0]['home']['currentRank'] = event.get('competitions')[0].get('competitors')[0].get('curatedRank', {}).get('current', '99')
#                 event['competitions'][0]['home']['linescores'] = event.get('competitions')[0].get('competitors')[0].get('linescores', [])
#                 event['competitions'][0]['home']['records'] = event.get('competitions')[0].get('competitors')[0].get('records', [])
#                 event['competitions'][0]['away'] = event.get('competitions')[0].get('competitors')[1].get('team')
#                 event['competitions'][0]['away']['score'] = event.get('competitions')[0].get('competitors')[1].get('score')
#                 event['competitions'][0]['away']['winner'] = event.get('competitions')[0].get('competitors')[1].get('winner')
#                 event['competitions'][0]['away']['currentRank'] = event.get('competitions')[0].get('competitors')[1].get('curatedRank', {}).get('current', '99')
#                 event['competitions'][0]['away']['linescores'] = event.get('competitions')[0].get('competitors')[1].get('linescores', [])
#                 event['competitions'][0]['away']['records'] = event.get('competitions')[0].get('competitors')[1].get('records', [])
#             else:
#                 event['competitions'][0]['away'] = event.get('competitions')[0].get('competitors')[0].get('team')
#                 event['competitions'][0]['away']['score'] = event.get('competitions')[0].get('competitors')[0].get('score')
#                 event['competitions'][0]['away']['winner'] = event.get('competitions')[0].get('competitors')[0].get('winner')
#                 event['competitions'][0]['away']['currentRank'] = event.get('competitions')[0].get('competitors')[0].get('curatedRank', {}).get('current', '99')
#                 event['competitions'][0]['away']['linescores'] = event.get('competitions')[0].get('competitors')[0].get('linescores', [])
#                 event['competitions'][0]['away']['records'] = event.get('competitions')[0].get('competitors')[0].get('records', [])
#                 event['competitions'][0]['home'] = event.get('competitions')[0].get('competitors')[1].get('team')
#                 event['competitions'][0]['home']['score'] = event.get('competitions')[0].get('competitors')[1].get('score')
#                 event['competitions'][0]['home']['winner'] = event.get('competitions')[0].get('competitors')[1].get('winner')
#                 event['competitions'][0]['home']['currentRank'] = event.get('competitions')[0].get('competitors')[1].get('curatedRank', {}).get('current', '99')
#                 event['competitions'][0]['home']['linescores'] = event.get('competitions')[0].get('competitors')[1].get('linescores', [])
#                 event['competitions'][0]['home']['records'] = event.get('competitions')[0].get('competitors')[1].get('records', [])

#             del_keys = ['geoBroadcasts', 'headlines', 'series', 'situation', 'tickets', 'odds']
#             for k in del_keys:
#                 event.get('competitions')[0].pop(k, None)
#             if len(event.get('competitions')[0]['notes'])>0:
#                 event.get('competitions')[0]['notes_type'] = event.get('competitions')[0]['notes'][0].get("type")
#                 event.get('competitions')[0]['notes_headline'] = event.get('competitions')[0]['notes'][0].get("headline").replace('"','')
#             else:
#                 event.get('competitions')[0]['notes_type'] = ''
#                 event.get('competitions')[0]['notes_headline'] = ''
#             if len(event.get('competitions')[0].get('broadcasts'))>0:
#                 event.get('competitions')[0]['broadcast_market'] = event.get('competitions')[0].get('broadcasts', [])[0].get('market', "")
#                 event.get('competitions')[0]['broadcast_name'] = event.get('competitions')[0].get('broadcasts', [])[0].get('names', [])[0]
#             else:
#                 event.get('competitions')[0]['broadcast_market'] = ""
#                 event.get('competitions')[0]['broadcast_name'] = ""
#             event.get('competitions')[0].pop('broadcasts', None)
#             event.get('competitions')[0].pop('notes', None)
#             x = pd.json_normalize(event.get('competitions')[0], sep='_')
#             x['game_id'] = x['id'].astype(int)
#             x['season'] = event.get('season').get('year')
#             x['season_type'] = event.get('season').get('type')
#             x['week'] = event.get('week', {}).get('number')
#             ev = pd.concat([ev, x], axis = 0, ignore_index = True)
#     ev = pd.DataFrame(ev)
#     ev.columns = [underscore(c) for c in ev.columns.tolist()]
#     return ev


# def espn_cfb_calendar_pandas(season = None, groups = None, ondays = None) -> pd.DataFrame:
#     """espn_cfb_calendar - look up the men's college football calendar for a given season

#     Args:
#         season (int): Used to define different seasons. 2002 is the earliest available season.
#         groups (int): Used to define different divisions. 80 is FBS, 81 is FCS.
#         ondays (boolean): Used to return dates for calendar ondays

#     Returns:
#         pd.DataFrame: Pandas dataframe containing calendar dates for the requested season.

#     Raises:
#         ValueError: If `season` is less than 2002.
#     """
#     if ondays is not None:
#         url = "https://sports.core.api.espn.com/v2/sports/football/leagues/college-football/seasons/{}/types/2/calendar/ondays".format(season)
#         resp = download(url=url)
#         txt = resp.json().get('eventDate').get('dates')
#         full_schedule = pd.DataFrame(txt,columns=['dates'])
#         full_schedule['dateURL'] = list(map(lambda x: x[:10].replace("-",""),full_schedule['dates']))
#         full_schedule['url']="http://site.api.espn.com/apis/site/v2/sports/football/college-football/scoreboard?dates="
#         full_schedule['url']= full_schedule['url'] + full_schedule['dateURL']
#     else:
#         if season is None:
#             season_url = ''
#         else:
#             season_url = '&dates=' + str(season)
#         if groups is None:
#             groups_url = '&groups=80'
#         else:
#             groups_url = '&groups=' + str(groups)
#         url = "http://site.api.espn.com/apis/site/v2/sports/football/college-football/scoreboard?{}{}".format(season_url, groups_url)
#         resp = download(url=url)
#         txt = resp.json()
#         txt = txt.get('leagues')[0].get('calendar')
#         full_schedule = pd.DataFrame()
#         for i in range(len(txt)):
#             if txt[i].get('entries', None) is not None:
#                 reg = pd.json_normalize(data = txt[i],
#                                         record_path = 'entries',
#                                         meta=["label","value","startDate","endDate"],
#                                         meta_prefix='season_type_',
#                                         record_prefix='week_',
#                                         errors="ignore",
#                                         sep='_')
#                 full_schedule = pd.concat([full_schedule,reg], ignore_index=True)
#         full_schedule['season']=season
#         full_schedule.columns = [underscore(c) for c in full_schedule.columns.tolist()]
#         full_schedule = full_schedule.rename(columns={"week_value": "week", "season_type_value": "season_type"})
#     return full_schedule