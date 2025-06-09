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
    context = get_current_context()
    log = context['ti'].log

    fixtures = kwargs.get("fixtures")
    if not fixtures:
        log.error("Fixtures data not found in kwargs.")
        raise AirflowFailException("Missing 'fixtures' in kwargs.")

    try:
        teams_ids = set()
        metadata = []

        for fixture in fixtures:
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
def fetch_team_info(**kwargs):
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
            all_team_stats.append(info)
        time.sleep(6)  # Respect rate limit: 10 requests per minute

    log.info(f"Fetched team info for {len(all_teams_info)} teams.")
    return all_teams_info
