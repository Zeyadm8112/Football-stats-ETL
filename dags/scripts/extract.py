import json
import requests
import random
import time
from airflow.operators.python import get_current_context
from airflow.exceptions import AirflowFailException


# -----------------------------
# Fetch Fixtures for a Specific Date
# -----------------------------
def fetch_fixtures(**kwargs):
    """
    this function will return randomally 120 fixtures only from the 
    all fixtures, Because the next API calls depends on the fixtures and 
    the free API is limited per day.
    """
    context = get_current_context()
    log = context['ti'].log

    date = kwargs.get('date') or context['ds']
    api_key = kwargs.get('api_key')
    base_url = kwargs.get('base_url', "https://v3.football.api-sports.io")

    if not api_key:
        log.error("API key is missing.")
        raise AirflowFailException("API key must be provided.")

    endpoint = "/fixtures"
    url = f"{base_url}{endpoint}"
    querystring = {"date": date}
    headers = {
        'x-rapidapi-host': 'v3.football.api-sports.io',
        'x-rapidapi-key': api_key
    }

    try:
        response = requests.get(url, headers=headers, params=querystring)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        log.error(f"API call to fetch fixtures failed: {e}")
        raise AirflowFailException("Failed to fetch fixtures.")

    try:
        data = response.json()
        fixtures = data.get("response", [])
        log.info(f"Fetched {len(fixtures)} fixtures for date {date}")

        return fixtures
    except (ValueError, KeyError) as e:
        log.error(f"Error parsing fixtures JSON: {e}")
        raise AirflowFailException("Invalid JSON format in fixtures response.")


# -----------------------------
# Extract Unique Team IDs from Fixtures
# -----------------------------
def extract_teams_id(**kwargs):
    """
    this def extract IDs for (leagues, season) so it may be used in future API calls
    """
    context = get_current_context()
    ti = context['ti']
    log = ti.log

    # ✅ Pulling fixtures directly from XCom
    fixtures = ti.xcom_pull(task_ids='extract_fixtures')
    
    if not fixtures:
        log.error("Fixtures data not found via XCom.")
        raise AirflowFailException("Missing 'fixtures' from XCom.")

    try:
        teams_ids = set()
        metadata = []

        for fixture in fixtures:
            if isinstance(fixture, str):
                fixture = json.loads(fixture)

            home_team = fixture["teams"]["home"]["id"]
            away_team = fixture["teams"]["away"]["id"]
            league_id = fixture["league"]["id"]
            season = fixture["league"]["season"]

            teams_ids.update([home_team, away_team])

            metadata.append({
                "home_team": home_team,
                "away_team": away_team,
                "league_id": league_id,
                "season": season
            })

        log.info(f"Extracted {len(teams_ids)} unique team IDs")
        return {"team_ids": list(teams_ids), "metadata": metadata}

    except KeyError as e:
        log.error(f"Key missing in fixture structure: {e}")
        raise AirflowFailException("Failed to extract team IDs from fixtures.")

# -----------------------------
# Fetch Info for a Single Team
# -----------------------------
def fetch_team_info(team_id, api_key, base_url, log):
    endpoint = "/teams"
    url = f"{base_url}{endpoint}"
    headers = {
        'x-rapidapi-host': 'v3.football.api-sports.io',
        'x-rapidapi-key': api_key
    }
    querystring = {"id": team_id}

    try:
        response = requests.get(url, headers=headers, params=querystring)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        log.warning(f"Failed to fetch info for team ID {team_id}: {e}")
        return None


# -----------------------------
# Fetch Info for Multiple Teams with Throttling
# -----------------------------
def fetch_teams_info(**kwargs):
    
    """
    The free API allows only 10 requests per second
    """
    context = get_current_context()
    log = context['ti'].log

    team_ids = kwargs.get("team_ids")
    api_key = kwargs.get("api_key")
    base_url = kwargs.get("base_url", "https://v3.football.api-sports.io")

    if not api_key:
        log.error("API key is missing.")
        raise AirflowFailException("API key must be provided.")

    if not team_ids:
        log.error("No team IDs provided.")
        raise AirflowFailException("Missing team IDs for extraction.")

    all_teams_info = []

    for team_id in team_ids:
        info = fetch_team_info(team_id, api_key, base_url, log)
        if info:
            all_teams_info.append(info)
        time.sleep(6)  # Respect rate limit: 10 requests per minute

    log.info(f"Fetched team info for {len(all_teams_info)} teams.")
    return all_teams_info





# -----------------------------
# Fetch Player Squad for a Single Team
# -----------------------------
def fetch_players_squad(team_id, api_key, base_url, log):
    endpoint = "/players/squads"
    url = f"{base_url}{endpoint}"
    headers = {
        'x-rapidapi-host': 'v3.football.api-sports.io',
        'x-rapidapi-key': api_key
    }
    querystring = {"id": team_id}

    try:
        response = requests.get(url, headers=headers, params=querystring)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        log.warning(f"Failed to fetch players squad for team ID {team_id}: {e}")
        return None


# -----------------------------
# Fetch PLayers Squads for Multiple Teams with Throttling
# -----------------------------
def fetch_teams_players_squads(**kwargs):
    context = get_current_context()
    log = context['ti'].log

    team_ids = kwargs.get("team_ids")
    api_key = kwargs.get("api_key")
    base_url = kwargs.get("base_url", "https://v3.football.api-sports.io")

    if not api_key:
        log.error("API key is missing.")
        raise AirflowFailException("API key must be provided.")

    if not team_ids:
        log.error("No team IDs provided.")
        raise AirflowFailException("Missing team IDs for extraction.")

    all_players_squads = []

    for team_id in team_ids:
        squad = fetch_players_squad(team_id, api_key, base_url, log)
        if squad:
            all_players_squads.append(squad)
        time.sleep(6)  # Respect rate limit: 10 requests per minute

    log.info(f"Fetched teams players sqauds info for {len(all_players_squads)} teams.")
    return all_players_squads
