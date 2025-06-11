
import pandas as pd
from airflow.operators.python import get_current_context
from airflow.exceptions import AirflowFailException


#-------------------------------------
#Transform matches 
#-------------------------------------
def transform_matches(**kwargs):
    """
this function takes the fixtures from task:extract_fixtures, make a dataframe out of the important data 
for the matches and neglect the unintersted ones.
handling null values and correcting columns datatypes .
    """    

    context =get_current_context()
    ti=context['ti']
    log=ti.log
    
    fixtures=ti.xcom.pull(task_ids='extract_fixtures')
    if not fixtures:
        log.error("Fixtures data not found via Xcom.")
        raise AirflowFailException("Missing 'Fixtures' from XCom.")
    
    match_data = [{
        'match_id': f['fixture']['id'],
        'date': f['fixture']['date'],
        'match_status': f['fixture']['status']['short'],
        'home_team_id': f['teams']['home']['id'],
        'away_team_id': f['teams']['away']['id'],
        'home_team_score': f['score']['fulltime']['home'],
        'away_team_score': f['score']['fulltime']['away'],
        'venue': f['fixture']['venue']['name'],
        'league_id': f['league']['id'],
        'league': f['league']['name'],
        'country': f['league']['country'],
        'season': f['league']['season']
    } for f in fixtures]
    df = pd.DataFrame(match_data)
    
    # Data Cleansing 
    #  Null values handling:
    try:
        df = df.dropna(subset=['match_id', 'home_team_id', 'away_team_id', 'date', 'season', 'league_id'])
        #dropping the cancelled matches
        df.drop(df[df['match_status']=='CANC'].index,inplace=True)
        df.fillna({'country': 'Unknown', 'match_status': 'Unknown', 'venue': 'Unknown'}, inplace=True)
        df.fillna({'home_team_score': 0, 'away_team_score': 0}, inplace=True)
        #-------------------------------------------------------
    except KeyError as e:
      log.error(f"Missing expected column during dropna: {e}")
      raise AirflowFailException("Required column missing in DataFrame during cleansing.")
      
    #changing the time format
    
    try:
        
        df['date'] =pd.to_datetime(df['date'])    
        #changing columns types into string or int
        df[['venue', 'country', 'league', 'match_status']] = df[['venue', 'country', 'league', 'match_status']].astype('string')
        df[['home_team_score', 'away_team_score']] = df[['home_team_score', 'away_team_score']].astype(int)
    
    except KeyError as e:
        log.error(f"Missing expected column during changing types: {e}")
        raise AirflowFailException("Required column missing in DataFrame during changing types.")
        
    
    return df.to_dict(orient='records')


#-------------------------------------
#Transform Team information
#-------------------------------------


def transform_teams(**kwargs):
    context =get_current_context()
    ti=context['ti']
    log=ti.log
    
    teams=ti.xcom.pull(task_ids='extract_team_info')
    if not teams:
        log.error("Teams data not found via Xcom.")
        raise AirflowFailException("Missing 'Teams' from XCom.")
    
    teams_data=[{
    'team_id':t['team']['id'],
    'team_name':t['team']['name'],
    'team_code':t['team']['code'],
    'country':t['team']['country'],
    'city':t['venue']['city'],
    'founded':t['team']['founded'],
    'venue_id':t['venue']['id'],
    'venue_name':t['venue']['name'],

    }for t in teams]

    df=pd.DataFrame(teams)
        
    try:
        df.dropna(subset=['team_id','team_name','venue_id'],inplace=True)
        df.fillna({'team_code':'UNK','country':'Unknown','city':'Uknown','founded':0,'venue_name':'Unkown'},inplace=True)
        df[['team_name', 'team_code', 'country','city','venue_name']] = df[['team_name', 'team_code',  'country','city','venue_name']] .astype('string')
        pass
    except KeyError as e:
        log.error(f"Missing expected column during Cleansing: {e}")
        raise AirflowFailException("Required column missing in DataFrame during cleansing.")
    
    
    
    return df.to_dict(orient='record')

        
    




