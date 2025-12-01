import os
import json
from datetime import datetime, timedelta, date
from typing import Dict, Any

import streamlit as st
import requests
import firebase_admin
from firebase_admin import credentials, firestore
import pandas as pd

# ----------------- CONFIG -----------------

SCHEDULE_CSV_PATH = "nhl_2025_2026_schedule_simple.csv"
ADMIN_EMAIL = "emilyropeter@gmail.com"

# ----------------- ENVIRONMENT CONFIG -----------------

FIREBASE_API_KEY = os.environ.get("FIREBASE_API_KEY")
FIREBASE_PROJECT_ID = os.environ.get("FIREBASE_PROJECT_ID")
SERVICE_ACCOUNT_JSON = os.environ.get("FIREBASE_SERVICE_ACCOUNT_JSON")

if not (FIREBASE_API_KEY and FIREBASE_PROJECT_ID and SERVICE_ACCOUNT_JSON):
    st.error(
        "Firebase environment variables are not set. "
        "Please configure FIREBASE_API_KEY, FIREBASE_PROJECT_ID, and FIREBASE_SERVICE_ACCOUNT_JSON."
    )
    st.stop()

# ----------------- FIREBASE ADMIN INIT -----------------

if not firebase_admin._apps:
    cred = credentials.Certificate(json.loads(SERVICE_ACCOUNT_JSON))
    firebase_admin.initialize_app(cred, {"projectId": FIREBASE_PROJECT_ID})

db = firestore.client()

# ----------------- AUTH CONSTANTS -----------------

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
    Return (start_date, end_date) date objects for Sunday -> Saturday.
    """
    start_date = sunday
    end_date = sunday + timedelta(days=6)
    return start_date, end_date


def is_pick_editing_open(today: date) -> bool:
    """
    Picks can only be entered/changed on Saturdays (all day).
    If you want to restrict to "Saturday night" specifically,
    you can add a time-of-day condition here.
    """
    return today.weekday() == 5  # Saturday


# ===================== SCHEDULE (CSV) HELPERS =====================

@st.cache_data
def load_schedule() -> pd.DataFrame:
    """
    Load the schedule CSV.
    Expected columns: date, game_id, home_team, away_team
    - date: YYYY-MM-DD
    - game_id: unique per game (we'll use as string key)
    """
    df = pd.read_csv(SCHEDULE_CSV_PATH)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    return df


def get_week_games(start_date: date, end_date: date) -> pd.DataFrame:
    df = load_schedule()
    mask = (df["date"] >= start_date) & (df["date"] <= end_date)
    return df.loc[mask].sort_values(["date", "game_id"])


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


def get_game_result(game_id: int) -> str | None:
    """
    Get the winner for a game from Firestore results collection.
    Returns team name or None if not set.
    """
    doc = db.collection("results").document(str(game_id)).get()
    if not doc.exists:
        return None
    return doc.to_dict().get("winner")


def set_game_result(game_id: int, winner: str | None, meta: Dict[str, Any]):
    """
    Set or clear the winner for a game.
    If winner is None, we still keep doc but with winner = None.
    """
    data = {
        "winner": winner,
        "updated_at": firestore.SERVER_TIMESTAMP,
    }
    data.update(meta)
    db.collection("results").document(str(game_id)).set(data, merge=True)


# ===================== SCORING / LEADERBOARDS =====================

def compute_weekly_scores(week_id: str) -> Dict[str, Dict[str, Any]]:
    """
    Returns: {uid: {"display_name": str, "correct": int, "total": int}}
    Uses Firestore results (winner per game_id).
    """
    picks_docs = list(get_all_picks_for_week(week_id))
    if not picks_docs:
        return {}

    scores: Dict[str, Dict[str, Any]] = {}

    for doc in picks_docs:
        data = doc.to_dict()
        uid = data["uid"]

        # Get user display name
        user_doc = db.collection("users").document(uid).get()
        if user_doc.exists:
            display_name = user_doc.to_dict().get("display_name", uid)
        else:
            display_name = uid

        user_picks = data.get("picks", {})
        correct = 0
        total = 0

        for game_id_str, info in user_picks.items():
            try:
                game_id = int(game_id_str)
            except ValueError:
                continue

            winner = get_game_result(game_id)
            if not winner:
                # No winner set yet -> game not graded
                continue

            total += 1
            if info["choice"] == winner:
                correct += 1

        scores[uid] = {
            "display_name": display_name,
            "correct": correct,  # also "points"
            "total": total,      # games graded
        }

    return scores


def compute_all_time_scores() -> Dict[str, Dict[str, Any]]:
    """
    Aggregate scores over all weeks.
    Returns: {uid: {"display_name": str, "correct": int, "total": int}}
    """
    all_picks = db.collection("picks").stream()
    scores: Dict[str, Dict[str, Any]] = {}

    for doc in all_picks:
        data = doc.to_dict()
        uid = data["uid"]

        user_doc = db.collection("users").document(uid).get()
        if user_doc.exists:
            display_name = user_doc.to_dict().get("display_name", uid)
        else:
            display_name = uid

        if uid not in scores:
            scores[uid] = {
                "display_name": display_name,
                "correct": 0,
                "total": 0,
            }

        user_picks = data.get("picks", {})
        for game_id_str, info in user_picks.items():
            try:
                game_id = int(game_id_str)
            except ValueError:
                continue

            winner = get_game_result(game_id)
            if not winner:
                continue

            scores[uid]["total"] += 1
            if info["choice"] == winner:
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
    st.header("🏒 Weekly NHL Pick Tracker – Make Your Picks")

    today = datetime.date(2025, 11, 25)
    week_sunday = get_picks_week_sunday(today)
    week_id = get_week_id(week_sunday)
    start_date, end_date = get_week_date_range(week_sunday)

    st.caption(
        f"Week: {week_sunday.isoformat()} (Sunday) "
        f"to {(week_sunday + timedelta(days=6)).isoformat()} (Saturday)"
    )
    editing_open = is_pick_editing_open(today)

    if editing_open:
        st.success("Picks are OPEN today (Saturday). You can enter or update your picks.")
    else:
        st.info("Picks are LOCKED. You can view your picks but not change them.")

    week_games = get_week_games(start_date, end_date)
    user_picks = load_user_picks(user["uid"], week_id)
    all_picks = dict(user_picks)  # copy

    if week_games.empty:
        st.info("No NHL games found for this week in the schedule CSV.")
        return

    for game_date in sorted(week_games["date"].unique()):
        st.subheader(game_date.isoformat())
        day_games = week_games[week_games["date"] == game_date]

        for _, row in day_games.iterrows():
            game_id = row["game_id"]
            game_id_str = str(game_id)
            home_team = row["home_team"]
            away_team = row["away_team"]

            col1, col2 = st.columns([2, 2])
            with col1:
                st.write(f"{away_team} @ {home_team} (Game ID: {game_id_str})")

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
                        key=f"pick_{game_id_str}",
                    )

                    all_picks[game_id_str] = {
                        "game_date": game_date.isoformat(),
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


def admin_set_winners_page(user: Dict[str, Any]):
    st.header("🛠 Admin – Set Game Winners")

    if user["email"] != ADMIN_EMAIL:
        st.error("You do not have permission to access this page.")
        return

    df = load_schedule()

    default_date = date.today()
    selected_date = st.date_input("Select game date", value=default_date)

    day_games = df[df["date"] == selected_date].sort_values("game_id")

    if day_games.empty:
        st.info("No games found in the schedule CSV for this date.")
        return

    st.write(f"Games on {selected_date.isoformat()}:")

    with st.form("set_winners_form"):
        selections: Dict[int, str | None] = {}

        for _, row in day_games.iterrows():
            game_id = row["game_id"]
            home_team = row["home_team"]
            away_team = row["away_team"]

            current_winner = get_game_result(int(game_id))

            options = ["(no winner yet)", home_team, away_team]
            default_index = 0
            if current_winner in [home_team, away_team]:
                default_index = options.index(current_winner)

            choice = st.selectbox(
                f"{away_team} @ {home_team} (Game ID: {game_id})",
                options,
                index=default_index,
                key=f"winner_{game_id}",
            )

            selections[int(game_id)] = (
                None if choice == "(no winner yet)" else choice
            )

        submitted = st.form_submit_button("Save winners")
        if submitted:
            for game_id, winner in selections.items():
                meta = {
                    "date": selected_date.isoformat(),
                    "home_team": str(
                        day_games[day_games["game_id"] == game_id]["home_team"].iloc[0]
                    ),
                    "away_team": str(
                        day_games[day_games["game_id"] == game_id]["away_team"].iloc[0]
                    ),
                }
                set_game_result(game_id, winner, meta)
            st.success("Winners updated successfully!")


# ===================== MAIN APP =====================

def main():
    st.set_page_config(page_title="NHL Weekly Pick Tracker", layout="wide")

    if "user" not in st.session_state:
        st.session_state["user"] = None

    auth_widget()

    user = st.session_state["user"]

    if user:
        st.sidebar.markdown(f"**Logged in as:** {user['email']}")
        logout_widget()

        pages = ["Make Picks", "Leaderboards"]
        if user["email"] == ADMIN_EMAIL:
            pages.append("Admin – Set Winners")

        page = st.sidebar.selectbox("Page", pages)
        if page == "Make Picks":
            picks_page(user)
        elif page == "Leaderboards":
            leaderboard_page()
        else:
            admin_set_winners_page(user)
    else:
        st.info("Please log in or sign up to make picks and view leaderboards.")


if __name__ == "__main__":
    main()
