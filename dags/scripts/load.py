import psycopg2
from airflow.exceptions import AirflowFailException
from airflow.models import TaskInstance
from airflow.operators.python import get_current_context

#------------------------------------
#DB Configuration
#------------------------------------
DB_CONFIG = {
    "dbname": "football_data",
    "user": "etl_user",
    "password": "**********",
    "host": "##.###.###.###",
    "port": 5432
}



#-------------------------------
#Loading the fixtures into DB
#-------------------------------
def load_fixtures(**kwargs):
    context = get_current_context()
    ti = context['ti']
    log = ti.log

    records = TaskInstance.xcom_pull(ti, task_ids='transform_matches')
    if not records:
        log.error("No fixtures data found in XCom from 'transform_matches'")
        raise AirflowFailException("No fixtures data to load.")

    insert_query = """
        INSERT INTO fixtures (
            match_id, date, match_status, home_team_id, away_team_id,
            home_team_score, away_team_score, venue, league_id,
            league, country, season
        ) VALUES (
            %(match_id)s, %(date)s, %(match_status)s, %(home_team_id)s, %(away_team_id)s,
            %(home_team_score)s, %(away_team_score)s, %(venue)s, %(league_id)s,
            %(league)s, %(country)s, %(season)s
        )
        ON CONFLICT (match_id) DO NOTHING;
    """

    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.executemany(insert_query, records)
            conn.commit()
        log.info(f"✅ Loaded {len(records)} fixtures into the database.")
    except Exception as e:
        log.error(f"❌ Failed to load fixtures: {e}")
        raise AirflowFailException(f"PostgreSQL insert error: {e}")



#-------------------------------
#Loading Teams into DB
#-------------------------------
def load_teams(**kwargs):
    context = get_current_context()
    ti = context['ti']
    log = ti.log

    records = TaskInstance.xcom_pull(ti, task_ids='transform_teams')
    if not records:
        log.error("No teams data found in XCom from 'transform_teams'")
        raise AirflowFailException("No teams data to load.")

    insert_query = """
        INSERT INTO teams (
            team_id, team_name, team_code, country, city,
            founded, venue_id, venue_name
        ) VALUES (
            %(team_id)s, %(team_name)s, %(team_code)s, %(country)s, %(city)s,
            %(founded)s, %(venue_id)s, %(venue_name)s
        )
        ON CONFLICT (team_id) DO NOTHING;
    """

    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.executemany(insert_query, records)
            conn.commit()
        log.info(f"✅ Loaded {len(records)} teams into the database.")
    except Exception as e:
        log.error(f"❌ Failed to load teams: {e}")
        raise AirflowFailException(f"PostgreSQL insert error: {e}")

#-------------------------------
#Loading Squads into DB
#-------------------------------

def load_squads(**kwargs):
    context = get_current_context()
    ti = context['ti']
    log = ti.log

    records = TaskInstance.xcom_pull(ti, task_ids='transform_squads')
    if not records:
        log.error("No squads data found in XCom from 'transform_squads'")
        raise AirflowFailException("No squads data to load.")

    insert_query = """
        INSERT INTO squads (
            team_id, player_id, team_name, name, age,
            number, position
        ) VALUES (
            %(team_id)s, %(player_id)s, %(team_name)s, %(name)s, %(age)s,
            %(number)s, %(position)s
        )
        ON CONFLICT (player_id, team_id) DO NOTHING;
    """

    try:
        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                cur.executemany(insert_query, records)
            conn.commit()
        log.info(f"✅ Loaded {len(records)} squad members into the database.")
    except Exception as e:
        log.error(f"❌ Failed to load squads: {e}")
        raise AirflowFailException(f"PostgreSQL insert error: {e}")




