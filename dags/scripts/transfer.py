
import pandas as pd
from airflow.operators.python import get_current_context
from airflow.exceptions import AirflowFailException



def transform_matches(**kwargs):
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