import os
import json
from datetime import datetime, timedelta, date
from typing import Dict, Any

import streamlit as st
import requests
import firebase_admin
from firebase_admin import credentials, firestore

# ----------------- ENVIRONMENT CONFIG -----------------

FIREBASE_API_KEY = os.environ.get("FIREBASE_API_KEY")
FIREBASE_PROJECT_ID = os.environ.get("FIREBASE_PROJECT_ID")
SERVICE_ACCOUNT_JSON = os.environ.get("FIREBASE_SERVICE_ACCOUNT_JSON")

if not (FIREBASE_API_KEY and FIREBASE_PROJECT_ID and SERVICE_ACCOUNT_JSON):
    st.error("Firebase environment variables are not set. "
             "Please configure FIREBASE_API_KEY, FIREBASE_PROJECT_ID, and FIREBASE_SERVICE_ACCOUNT_JSON.")
    st.stop()

# ----------------- FIREBASE ADMIN INIT -----------------

if not firebase_admin._apps:
    cred = credentials.Certificate(json.loads(SERVICE_ACCOUNT_JSON))
    firebase_admin.initialize_app(cred, {"projectId": FIREBASE_PROJECT_ID})

db = firestore.client()

# ----------------- CONSTANTS -----------------

NHL_SCHEDULE_URL = "https://statsapi.web.nhl.com/api/v1/schedule"
AUTH_SIGNUP_URL = (
    f"https://identitytoolkit.googleapis.com/v1/accounts:signUp?key={FIREBASE_API_KEY}"
)
AUTH_SIGNIN_URL = (
    f"https://identitytoolkit.googleapis.com/v1/accounts:signInWithPassword?key={FIREBASE_API_KEY}"
)


# ===================== WEEK / DATE HELPERS =====================

def get_next_sunday(today: date) -> date:
    """
    Given a date, return the next Sunday (could be tomorrow).
    Sunday is considered the start of the pick week.
    """
    # weekday(): Monday=0,...,Sunday=6
    days_until_sunday = (6 - today.weekday()) % 7
    if days_until_sunday == 0:
        # today is Sunday -> "next" Sunday
        days_until_sunday = 7
    return today + timedelta(days=days_until_sunday)


def get_week_sunday_for_current_games(today: date) -> date:
    """
    For scoring / current week view:
    Week is Sunday -> Saturday that contains 'today'.
    """
    # Sunday (6) should subtract 0, Monday(0) subtract 1, etc.
    days_since_sunday = (today.weekday() + 1) % 7
    return today - timedelta(days=days_since_sunday)


def get_picks_week_sunday(today: date) -> date:
    """
    Picks behavior:
      - On Saturday: picks are for the upcoming week (next Sunday -> Saturday)
      - On all other days: show the currently active week (Sunday -> Saturday that contains today).
    """
    if today.weekday() == 5:  # Saturday
        return get_next_sunday(today)
    else:
        return get_week_sunday_for_current_games(today)


def get_week_id(sunday: date) -> str:
    """
    We'll use the Sunday date as the unique week ID, e.g. '2025-01-05'.
    """
    return sunday.isoformat()


def get_week_date_range(sunday: date):
    """
    Return (start_date, end_date) strings in 'YYYY-MM-DD' for Sunday -> Saturday.
    """
    start_date = sunday
    end_date = sunday + timedelta(days=6)
    return start_date.isoformat(), end_date.isoformat()


def is_pick_editing_open(today: date) -> bool:
    """
    Picks can only be entered/changed on Saturdays (all day).
    If you want to restrict to "Saturday night" specifically,
    you can add a time-of-day condition here.
    """
    return today.weekday() == 5  # Saturday


# ===================== NHL API HELPERS =====================

def fetch_schedule_range(start_date: str, end_date: str) -> Dict[str, Any]:
    """
    Fetch NHL schedule between startDate and endDate (inclusive).
    """
    params = {"startDate": start_date, "endDate": end_date}
    r = requests.get(NHL_SCHEDULE_URL, params=params)
    r.raise_for_status()
    return r.json()


def fetch_day_schedule(game_date: str) -> Dict[str, Any]:
    params = {"date": game_date}
    r = requests.get(NHL_SCHEDULE_URL, params=params)
    r.raise_for_status()
    return r.json()


def get_game_winner_name(game: Dict[str, Any]) -> str | None:
    """
    Given a game object from NHL schedule API, return the winning team's name if final.
    Returns None if game not final or tie-like edge cases.
    """
    status = game.get("status", {}).get("detailedState")
    if status != "Final":
        return None

    teams = game.get("teams", {})
    home = teams.get("home", {})
    away = teams.get("away", {})

    home_team = home.get("team", {}).get("name")
    away_team = away.get("team", {}).get("name")
    home_score = home.get("score", 0)
    away_score = away.get("score", 0)

    if home_score > away_score:
        return home_team
    elif away_score > home_score:
        return away_team
    else:
        # If you want to handle ties/shootouts differently, you can adjust here.
        return None


# ===================== FIRESTORE HELPERS =====================

def ensure_user_doc(uid: str, display_name: str):
    user_ref = db.collection("users").document(uid)
    if not user_ref.get().exists:
        user_ref.set(
            {
                "display_name": display_name,
                "created_at": firestore.SERVER_TIMESTAMP,
            }
        )


def load_user_picks(uid: str, week_id: str) -> Dict[str, Any]:
    doc_id = f"{week_id}_{uid}"
    doc_ref = db.collection("picks").document(doc_id)
    doc = doc_ref.get()
    if doc.exists:
        return doc.to_dict().get("picks", {})
    return {}


def save_user_picks(uid: str, week_id: str, picks: Dict[str, Any]):
    doc_id = f"{week_id}_{uid}"
    doc_ref = db.collection("picks").document(doc_id)
    doc_ref.set(
        {
            "uid": uid,
            "week_id": week_id,
            "picks": picks,
            "updated_at": firestore.SERVER_TIMESTAMP,
        },
        merge=True,
    )


def get_all_picks_for_week(week_id: str):
    return db.collection("picks").where("week_id", "==", week_id).stream()


# ===================== SCORING / LEADERBOARDS =====================

def compute_weekly_scores(week_id: str) -> Dict[str, Dict[str, Any]]:
    """
    Returns: {uid: {"display_name": str, "correct": int, "total": int}}
    """
    picks_docs = list(get_all_picks_for_week(week_id))
    if not picks_docs:
        return {}

    # Group game picks by date to minimize API calls
    picks_by_date: Dict[str, list] = {}
    user_names: Dict[str, str] = {}

    for doc in picks_docs:
        data = doc.to_dict()
        uid = data["uid"]
        user_doc = db.collection("users").document(uid).get()
        if user_doc.exists:
            user_names[uid] = user_doc.to_dict().get("display_name", uid)
        else:
            user_names[uid] = uid

        picks = data.get("picks", {})
        for game_pk_str, info in picks.items():
            game_date = info["game_date"]
            if game_date not in picks_by_date:
                picks_by_date[game_date] = []
            picks_by_date[game_date].append(
                {
                    "uid": uid,
                    "game_pk": int(game_pk_str),
                    "choice": info["choice"],
                }
            )

    # Determine winners per game
    winners_by_game_pk: Dict[int, str | None] = {}
    for game_date, picks_list in picks_by_date.items():
        sched = fetch_day_schedule(game_date)
        for d in sched.get("dates", []):
            for g in d.get("games", []):
                game_pk = g.get("gamePk")
                winners_by_game_pk[game_pk] = get_game_winner_name(g)

    # Score users
    scores: Dict[str, Dict[str, Any]] = {}
    for doc in picks_docs:
        data = doc.to_dict()
        uid = data["uid"]
        user_picks = data.get("picks", {})
        correct = 0
        total = 0
        for game_pk_str, info in user_picks.items():
            game_pk = int(game_pk_str)
            winner = winners_by_game_pk.get(game_pk)
            if winner is None:
                # game not final or tie-handling
                continue
            total += 1
            if info["choice"] == winner:
                correct += 1

        scores[uid] = {
            "display_name": user_names[uid],
            "correct": correct,      # also "points"
            "total": total,          # final games counted
        }

    return scores


def compute_all_time_scores() -> Dict[str, Dict[str, Any]]:
    """
    Aggregate scores over all weeks.
    Returns: {uid: {"display_name": str, "correct": int, "total": int}}
    """
    all_picks = db.collection("picks").stream()
    # First, collect all picks grouped by uid and date
    user_names: Dict[str, str] = {}
    picks_by_date: Dict[str, list] = {}

    for doc in all_picks:
        data = doc.to_dict()
        uid = data["uid"]

        user_doc = db.collection("users").document(uid).get()
        if user_doc.exists:
            user_names[uid] = user_doc.to_dict().get("display_name", uid)
        else:
            user_names[uid] = uid

        user_picks = data.get("picks", {})
        for game_pk_str, info in user_picks.items():
            game_date = info["game_date"]
            if game_date not in picks_by_date:
                picks_by_date[game_date] = []
            picks_by_date[game_date].append(
                {
                    "uid": uid,
                    "game_pk": int(game_pk_str),
                    "choice": info["choice"],
                }
            )

    if not picks_by_date:
        return {}

    # Fetch winners for each date once
    winners_by_game_pk: Dict[int, str | None] = {}
    for game_date, picks_list in picks_by_date.items():
        sched = fetch_day_schedule(game_date)
        for d in sched.get("dates", []):
            for g in d.get("games", []):
                game_pk = g.get("gamePk")
                winners_by_game_pk[game_pk] = get_game_winner_name(g)

    # Aggregate scores
    scores: Dict[str, Dict[str, Any]] = {}
    for game_date, picks_list in picks_by_date.items():
        for pick in picks_list:
            uid = pick["uid"]
            game_pk = pick["game_pk"]
            choice = pick["choice"]
            winner = winners_by_game_pk.get(game_pk)
            if winner is None:
                continue

            if uid not in scores:
                scores[uid] = {
                    "display_name": user_names[uid],
                    "correct": 0,
                    "total": 0,
                }

            scores[uid]["total"] += 1
            if choice == winner:
                scores[uid]["correct"] += 1

    return scores


# ===================== AUTH HELPERS (Firebase REST) =====================

def firebase_signup(email: str, password: str) -> Dict[str, Any]:
    payload = {"email": email, "password": password, "returnSecureToken": True}
    r = requests.post(AUTH_SIGNUP_URL, json=payload)
    r.raise_for_status()
    return r.json()


def firebase_signin(email: str, password: str) -> Dict[str, Any]:
    payload = {"email": email, "password": password, "returnSecureToken": True}
    r = requests.post(AUTH_SIGNIN_URL, json=payload)
    r.raise_for_status()
    return r.json()


# ===================== UI: AUTH WIDGETS =====================

def auth_widget():
    st.sidebar.title("Login / Signup")

    mode = st.sidebar.radio("Mode", ["Login", "Sign up"])
    email = st.sidebar.text_input("Email")
    password = st.sidebar.text_input("Password", type="password")
    display_name = None

    if mode == "Sign up":
        display_name = st.sidebar.text_input("Display name")

    if st.sidebar.button(mode):
        if not email or not password:
            st.sidebar.error("Email and password are required.")
            return

        try:
            if mode == "Sign up":
                res = firebase_signup(email, password)
                uid = res["localId"]
                st.session_state["user"] = {
                    "uid": uid,
                    "email": email,
                    "id_token": res["idToken"],
                }
                ensure_user_doc(uid, display_name or email)
                st.sidebar.success("Sign up successful!")
            else:
                res = firebase_signin(email, password)
                uid = res["localId"]
                st.session_state["user"] = {
                    "uid": uid,
                    "email": email,
                    "id_token": res["idToken"],
                }
                ensure_user_doc(uid, email)
                st.sidebar.success("Login successful!")
        except requests.HTTPError as e:
            try:
                err_msg = e.response.json().get("error", {}).get("message", str(e))
            except Exception:
                err_msg = str(e)
            st.sidebar.error(f"Auth error: {err_msg}")


def logout_widget():
    if st.sidebar.button("Log out"):
        st.session_state.pop("user", None)
        st.experimental_rerun()


# ===================== UI: PAGES =====================

def picks_page(user: Dict[str, Any]):
    st.header("🏒 Weekly NHL Pick’em – Make Your Picks")

    today = date.today()
    week_sunday = get_picks_week_sunday(today)
    week_id = get_week_id(week_sunday)
    start_date_str, end_date_str = get_week_date_range(week_sunday)

    st.caption(f"Week: {week_sunday.isoformat()} (Sunday) to { (week_sunday + timedelta(days=6)).isoformat() } (Saturday)")
    editing_open = is_pick_editing_open(today)

    if editing_open:
        st.success("Picks are OPEN today (Saturday). You can enter or update your picks.")
    else:
        st.info("Picks are LOCKED. You can view your picks but not change them.")

    schedule_json = fetch_schedule_range(start_date_str, end_date_str)
    user_picks = load_user_picks(user["uid"], week_id)
    all_picks = dict(user_picks)  # copy

    if not schedule_json.get("dates"):
        st.info("No NHL games found for this week.")
        return

    for date_block in schedule_json["dates"]:
        game_date = date_block["date"]
        st.subheader(game_date)

        for game in date_block.get("games", []):
            game_pk = game["gamePk"]
            game_id_str = str(game_pk)

            home_team = game["teams"]["home"]["team"]["name"]
            away_team = game["teams"]["away"]["team"]["name"]

            col1, col2 = st.columns([2, 2])
            with col1:
                st.write(f"{away_team} @ {home_team}")

            with col2:
                if editing_open:
                    # can edit picks
                    default_choice = None
                    if game_id_str in all_picks:
                        default_choice = all_picks[game_id_str]["choice"]

                    options = [home_team, away_team]
                    if default_choice in options:
                        default_index = options.index(default_choice)
                    else:
                        default_index = 0

                    choice = st.radio(
                        "Pick winner",
                        options,
                        index=default_index,
                        key=f"pick_{game_pk}",
                    )

                    all_picks[game_id_str] = {
                        "game_date": game_date,
                        "home_team": home_team,
                        "away_team": away_team,
                        "choice": choice,
                    }
                else:
                    # show existing choice
                    if game_id_str in all_picks:
                        st.write(f"Your pick: **{all_picks[game_id_str]['choice']}**")
                    else:
                        st.write("No pick made.")

    if editing_open and st.button("Save My Picks"):
        save_user_picks(user["uid"], week_id, all_picks)
        st.success("Picks saved!")


def leaderboard_page():
    st.header("📊 Leaderboards")

    today = date.today()
    scoring_week_sunday = get_week_sunday_for_current_games(today)
    week_id = get_week_id(scoring_week_sunday)

    st.subheader(f"Weekly standings – Week starting {scoring_week_sunday.isoformat()}")

    weekly_scores = compute_weekly_scores(week_id)
    if not weekly_scores:
        st.info("No completed games or picks yet for this week.")
    else:
        rows = []
        for stats in weekly_scores.values():
            correct = stats["correct"]
            total = stats["total"]
            accuracy = (correct / total * 100) if total > 0 else 0.0
            rows.append(
                {
                    "Name": stats["display_name"],
                    "Points": correct,  # 1 point per correct pick
                    "Games Graded": total,
                    "Accuracy": f"{accuracy:.1f}%",
                }
            )
        rows = sorted(rows, key=lambda r: r["Points"], reverse=True)
        st.table(rows)

    st.subheader("All-time standings")

    all_time_scores = compute_all_time_scores()
    if not all_time_scores:
        st.info("No all-time data yet.")
    else:
        rows = []
        for stats in all_time_scores.values():
            correct = stats["correct"]
            total = stats["total"]
            accuracy = (correct / total * 100) if total > 0 else 0.0
            rows.append(
                {
                    "Name": stats["display_name"],
                    "Points": correct,
                    "Games Graded": total,
                    "Accuracy": f"{accuracy:.1f}%",
                }
            )
        rows = sorted(rows, key=lambda r: r["Points"], reverse=True)
        st.table(rows)


# ===================== MAIN APP =====================

def main():
    st.set_page_config(page_title="NHL Weekly Pick’em", layout="wide")

    if "user" not in st.session_state:
        st.session_state["user"] = None

    auth_widget()

    user = st.session_state["user"]

    if user:
        st.sidebar.markdown(f"**Logged in as:** {user['email']}")
        logout_widget()

        page = st.sidebar.selectbox("Page", ["Make Picks", "Leaderboards"])
        if page == "Make Picks":
            picks_page(user)
        else:
            leaderboard_page()
    else:
        st.info("Please log in or sign up to make picks and view leaderboards.")


if __name__ == "__main__":
    main()
