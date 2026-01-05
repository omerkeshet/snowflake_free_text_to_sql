import streamlit as st
import openai
import snowflake.connector
import pandas as pd
import json
from datetime import datetime, timedelta
from typing import Optional, Tuple
import re

# =============================================================================
# CONFIGURATION
# =============================================================================

TABLE_NAME = "mako_data_lake.public.combined_events_enriched"
DEFAULT_LIMIT = 100
APP_NAME = "Keshet Query Studio"

# Important columns with detailed descriptions
IMPORTANT_COLUMNS = {
    "date": {
        "description": "Event date",
        "type": "DATE",
        "values": None
    },
    "event_time": {
        "description": "Event timestamp",
        "type": "TIMESTAMP_NTZ(9)",
        "values": None
    },
    "event_name": {
        "description": "The event name",
        "type": "VARCHAR",
        "values": "page_view, play, click, ads, engagement"
    },
    "calculated_visit_id": {
        "description": "The user's visit ID calculated since start of the visit until 30 minutes of inactivity. This is the relevant visit_id to use for session analysis.",
        "type": "NUMBER",
        "values": None
    },
    "user_id": {
        "description": "The user's ID. Users have the same ID that is dependent on the device.",
        "type": "VARCHAR",
        "values": None
    },
    "item_id": {
        "description": "The item ID the event happened for. For example, a page view for a specific article.",
        "type": "VARCHAR",
        "values": None
    },
    "channel_id": {
        "description": "The channel that the item is connected to.",
        "type": "VARCHAR",
        "values": None
    },
    "content_type": {
        "description": "The content type of the item.",
        "type": "VARCHAR",
        "values": None
    },
    "visit_first_event": {
        "description": "1 if this is the first event in the user's visit, null otherwise. Useful for counting visits.",
        "type": "NUMBER",
        "values": "1 or null"
    },
    "push_id": {
        "description": "The ID of the push notification the user started the visit from.",
        "type": "NUMBER",
        "values": None
    },
    "play_id": {
        "description": "The ID of the play event. Stays the same as long as the user is watching the same video.",
        "type": "VARCHAR",
        "values": None
    },
    "action": {
        "description": "The action the user did. Mostly relevant during play events.",
        "type": "VARCHAR",
        "values": "skip_backwards, skip_forward, error, mute, share, display, start, resume, change_display, pause, seek, startover, complete, play, close, live_rt, fallback, change_state, unmute, skip, change_speed, continue_watch"
    },
    "reason": {
        "description": "The reason for the action. Mostly relevant for the action field.",
        "type": "VARCHAR",
        "values": "scroll, end_manual_next_episode, user_idle, next_episode, back, auto_swipe, app_background, dvr_back, more_episodes, user, buffering, midroll, share, user_player_pause, epg, preroll, dvr, swipe, auto_next_episode, click"
    },
    "previous_action": {
        "description": "The action of the previous event.",
        "type": "VARCHAR",
        "values": "skip_backwards, skip_forward, error, mute, share, display, start, resume, change_display, pause, seek, startover, complete, play, close, live_rt, fallback, change_state, unmute, skip, change_speed, continue_watch"
    },
    "previous_reason": {
        "description": "The reason of the previous event.",
        "type": "VARCHAR",
        "values": "scroll, end_manual_next_episode, user_idle, next_episode, back, auto_swipe, app_background, dvr_back, more_episodes, user, buffering, midroll, share, user_player_pause, epg, preroll, dvr, swipe, auto_next_episode, click"
    },
    "type": {
        "description": "The ad type.",
        "type": "VARCHAR",
        "values": "video, display"
    },
    "sub_type": {
        "description": "The ad subtype.",
        "type": "VARCHAR",
        "values": "preroll, cube, article, monster, jambo, native, parallax, standard, full_screen, prime, banner, ozen, poster, inboard, coast2coast, video_paused_ad, midroll"
    },
    "ENGAGEMENT_TYPE": {
        "description": "The type of engagement.",
        "type": "VARCHAR",
        "values": "share, comment, feelings, interaction"
    },
    "ENGAGEMENT_DETAILS": {
        "description": "The details of the engagement type.",
        "type": "VARCHAR",
        "values": "whatsapp, open_sticky_player, twitter, allow_push_notifications, deny_push_notifications, sticky_player_off, copy_link, events_summary, contact_us, loved, comment, close_sticky_player, sticky_player_on, arrow_up, reply, didnt_love, facebook, x_sticky_player"
    },
    "SITE": {
        "description": "The site where the visit happened.",
        "type": "VARCHAR",
        "values": "n12, 12plus, v1, mako"
    },
    "ABSOLUTE_VISIT_REF": {
        "description": "The referrer website.",
        "type": "VARCHAR",
        "values": "facebook, google, direct, etc."
    },
    "DEVICE_TYPE": {
        "description": "The device type used.",
        "type": "VARCHAR",
        "values": "tablet, Unknown, mobile, web, smart_tv"
    },
    "DEVICE_OS": {
        "description": "The OS of the device.",
        "type": "VARCHAR",
        "values": "Windows, hisense, iOS, Linux, Tizen, Apple TVOS, Android, iPadOS, Chrome OS, PlayStation 4, browser, macOS, webos, tizen, tvOS"
    },
    "IL_OR_ABROAD": {
        "description": "Indicator for the user's location - Israel or abroad.",
        "type": "VARCHAR",
        "values": "unknown, abroad, il"
    },
    "PLATFORM": {
        "description": "The platform the user is visiting from.",
        "type": "VARCHAR",
        "values": "mobile-app, smart_tv-app, tablet-browser, unknown, browser, tablet-app, mobile-browser"
    }
}

EXAMPLE_QUERIES = [
    "How many unique users visited mako yesterday?",
    "What's the breakdown of events by device type?",
    "Show me the top 10 most active users by event count",
    "What percentage of visits came from Israel vs abroad?",
    "How many video plays started vs completed?",
    "What are the top referral sources?",
    "Show engagement breakdown by type",
    "Average events per visit by platform"
]

# =============================================================================
# STYLING
# =============================================================================

def apply_custom_css():
    st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');
    
    :root {
        --keshet-blue: #1e3a8a;
        --keshet-purple: #7c3aed;
        --keshet-pink: #ec4899;
        --keshet-orange: #f97316;
        --keshet-yellow: #eab308;
        --keshet-green: #22c55e;
        --keshet-cyan: #06b6d4;
        --bg-primary: #fafbfc;
        --bg-secondary: #ffffff;
        --text-primary: #1e293b;
        --text-secondary: #64748b;
        --border-color: #e2e8f0;
        --success: #10b981;
        --warning: #f59e0b;
        --error: #ef4444;
    }
    
    .stApp {
        background: linear-gradient(135deg, #f8fafc 0%, #f1f5f9 100%);
        font-family: 'Plus Jakarta Sans', sans-serif;
    }
    
    /* Header */
    .app-header {
        background: linear-gradient(135deg, var(--keshet-blue) 0%, var(--keshet-purple) 50%, var(--keshet-pink) 100%);
        padding: 2rem 2.5rem;
        border-radius: 16px;
        margin-bottom: 2rem;
        box-shadow: 0 10px 40px rgba(124, 58, 237, 0.2);
    }
    
    .app-title {
        color: white;
        font-size: 2rem;
        font-weight: 700;
        margin: 0;
        display: flex;
        align-items: center;
        gap: 12px;
    }
    
    .app-subtitle {
        color: rgba(255,255,255,0.85);
        font-size: 1rem;
        font-weight: 400;
        margin-top: 0.5rem;
    }
    
    /* Cards */
    .card {
        background: var(--bg-secondary);
        border-radius: 12px;
        padding: 1.5rem;
        border: 1px solid var(--border-color);
        box-shadow: 0 1px 3px rgba(0,0,0,0.04);
        margin-bottom: 1rem;
    }
    
    .card-header {
        display: flex;
        align-items: center;
        gap: 10px;
        margin-bottom: 1rem;
        padding-bottom: 0.75rem;
        border-bottom: 1px solid var(--border-color);
    }
    
    .card-title {
        font-size: 1rem;
        font-weight: 600;
        color: var(--text-primary);
        margin: 0;
    }
    
    /* SQL Editor */
    .sql-editor {
        font-family: 'JetBrains Mono', monospace;
        font-size: 0.9rem;
        background: #1e293b;
        color: #e2e8f0;
        border-radius: 8px;
        padding: 1rem;
        border: none;
    }
    
    /* Example chips */
    .example-chip {
        display: inline-block;
        background: linear-gradient(135deg, #ede9fe 0%, #fce7f3 100%);
        color: var(--keshet-purple);
        padding: 0.5rem 1rem;
        border-radius: 20px;
        font-size: 0.85rem;
        margin: 0.25rem;
        cursor: pointer;
        border: 1px solid #ddd6fe;
        transition: all 0.2s ease;
    }
    
    .example-chip:hover {
        background: linear-gradient(135deg, var(--keshet-purple) 0%, var(--keshet-pink) 100%);
        color: white;
        transform: translateY(-2px);
        box-shadow: 0 4px 12px rgba(124, 58, 237, 0.3);
    }
    
    /* Stats */
    .stat-box {
        background: var(--bg-secondary);
        border-radius: 10px;
        padding: 1rem 1.25rem;
        border: 1px solid var(--border-color);
        text-align: center;
    }
    
    .stat-value {
        font-size: 1.5rem;
        font-weight: 700;
        color: var(--keshet-purple);
    }
    
    .stat-label {
        font-size: 0.75rem;
        color: var(--text-secondary);
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    
    /* Buttons */
    .stButton > button {
        font-family: 'Plus Jakarta Sans', sans-serif;
        font-weight: 600;
        border-radius: 8px;
        padding: 0.5rem 1.5rem;
        transition: all 0.2s ease;
    }
    
    .stButton > button[kind="primary"] {
        background: linear-gradient(135deg, var(--keshet-purple) 0%, var(--keshet-pink) 100%);
        border: none;
        color: white;
    }
    
    .stButton > button[kind="primary"]:hover {
        box-shadow: 0 4px 15px rgba(124, 58, 237, 0.4);
        transform: translateY(-1px);
    }
    
    /* History items */
    .history-item {
        background: var(--bg-secondary);
        border: 1px solid var(--border-color);
        border-radius: 8px;
        padding: 0.75rem 1rem;
        margin-bottom: 0.5rem;
        cursor: pointer;
        transition: all 0.2s ease;
    }
    
    .history-item:hover {
        border-color: var(--keshet-purple);
        box-shadow: 0 2px 8px rgba(124, 58, 237, 0.1);
    }
    
    .history-question {
        font-size: 0.85rem;
        color: var(--text-primary);
        font-weight: 500;
    }
    
    .history-time {
        font-size: 0.7rem;
        color: var(--text-secondary);
    }
    
    /* Sidebar */
    .css-1d391kg {
        background: var(--bg-secondary);
    }
    
    /* Tabs */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
        background: var(--bg-primary);
        padding: 4px;
        border-radius: 10px;
    }
    
    .stTabs [data-baseweb="tab"] {
        border-radius: 8px;
        font-weight: 500;
    }
    
    /* Explanation box */
    .explanation-box {
        background: linear-gradient(135deg, #fef3c7 0%, #fce7f3 100%);
        border: 1px solid #fcd34d;
        border-radius: 10px;
        padding: 1rem;
        margin-top: 1rem;
    }
    
    .explanation-title {
        font-weight: 600;
        color: #92400e;
        margin-bottom: 0.5rem;
        display: flex;
        align-items: center;
        gap: 8px;
    }
    
    /* Cost estimation */
    .cost-box {
        background: #f0fdf4;
        border: 1px solid #86efac;
        border-radius: 8px;
        padding: 0.75rem 1rem;
        display: flex;
        align-items: center;
        gap: 10px;
    }
    
    .cost-warning {
        background: #fef3c7;
        border-color: #fcd34d;
    }
    
    /* Toggle buttons */
    .view-toggle {
        display: flex;
        gap: 4px;
        background: var(--bg-primary);
        padding: 4px;
        border-radius: 8px;
        width: fit-content;
    }
    
    .toggle-btn {
        padding: 6px 16px;
        border-radius: 6px;
        border: none;
        cursor: pointer;
        font-weight: 500;
        font-size: 0.85rem;
        transition: all 0.2s;
    }
    
    .toggle-btn.active {
        background: var(--keshet-purple);
        color: white;
    }
    
    .toggle-btn:not(.active) {
        background: transparent;
        color: var(--text-secondary);
    }
    
    /* Hide Streamlit elements */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    .stDeployButton {display: none;}
    
    /* Error message styling */
    .error-box {
        background: #fef2f2;
        border: 1px solid #fecaca;
        border-radius: 10px;
        padding: 1rem;
        color: #991b1b;
    }
    
    .success-box {
        background: #f0fdf4;
        border: 1px solid #86efac;
        border-radius: 10px;
        padding: 1rem;
        color: #166534;
    }
    </style>
    """, unsafe_allow_html=True)


# =============================================================================
# DATABASE FUNCTIONS
# =============================================================================

@st.cache_resource
def get_snowflake_connection():
    """Create a Snowflake connection using key-pair authentication."""
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives import serialization
    
    private_key_pem = st.secrets["snowflake"]["private_key"]
    private_key = serialization.load_pem_private_key(
        private_key_pem.encode(),
        password=None,
        backend=default_backend()
    )
    
    private_key_bytes = private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )
    
    return snowflake.connector.connect(
        account=st.secrets["snowflake"]["account"],
        user=st.secrets["snowflake"]["user"],
        private_key=private_key_bytes,
        warehouse=st.secrets["snowflake"]["warehouse"],
        database=st.secrets["snowflake"]["database"],
        schema=st.secrets["snowflake"]["schema"],
        role=st.secrets["snowflake"]["role"]
    )


@st.cache_data(ttl=3600)
def get_all_columns():
    """Fetch all columns from the table."""
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        cursor.execute(f"DESCRIBE TABLE {TABLE_NAME}")
        columns = cursor.fetchall()
        cursor.close()
        return [(col[0], col[1]) for col in columns]
    except Exception as e:
        st.error(f"Error fetching columns: {e}")
        return []


def execute_query(sql: str) -> pd.DataFrame:
    """Execute SQL query and return results as a dataframe."""
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    cursor.execute(sql)
    columns = [desc[0] for desc in cursor.description]
    data = cursor.fetchall()
    cursor.close()
    return pd.DataFrame(data, columns=columns)


def estimate_query_cost(sql: str) -> dict:
    """Estimate query cost/size before execution."""
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        # Use EXPLAIN to get query plan
        cursor.execute(f"EXPLAIN {sql}")
        plan = cursor.fetchall()
        cursor.close()
        
        # Parse plan for estimates
        plan_text = str(plan)
        
        # Extract estimated rows if available
        rows_match = re.search(r'rows[=:]?\s*(\d+)', plan_text.lower())
        estimated_rows = int(rows_match.group(1)) if rows_match else None
        
        return {
            "estimated_rows": estimated_rows,
            "has_date_filter": "date" in sql.lower(),
            "has_limit": "limit" in sql.lower()
        }
    except Exception as e:
        return {
            "estimated_rows": None,
            "has_date_filter": "date" in sql.lower(),
            "has_limit": "limit" in sql.lower(),
            "error": str(e)
        }


def get_column_stats(df: pd.DataFrame) -> dict:
    """Calculate column statistics for the result set."""
    stats = {}
    for col in df.columns:
        col_stats = {
            "distinct": df[col].nunique(),
            "nulls": df[col].isnull().sum()
        }
        if pd.api.types.is_numeric_dtype(df[col]):
            col_stats["min"] = df[col].min()
            col_stats["max"] = df[col].max()
            col_stats["mean"] = df[col].mean()
        stats[col] = col_stats
    return stats


# =============================================================================
# AI FUNCTIONS
# =============================================================================

def build_schema_description(all_columns: list) -> str:
    """Build a schema description for the LLM."""
    lines = []
    for col_name, col_type in all_columns:
        important_info = IMPORTANT_COLUMNS.get(col_name) or IMPORTANT_COLUMNS.get(col_name.upper()) or IMPORTANT_COLUMNS.get(col_name.lower())
        if important_info:
            desc = f"- {col_name} ({important_info['type']}): {important_info['description']}"
            if important_info['values']:
                desc += f" Possible values: {important_info['values']}"
            lines.append(desc)
        else:
            lines.append(f"- {col_name} ({col_type})")
    return "\n".join(lines)


def generate_sql(user_question: str, schema_description: str, limit: int) -> Tuple[str, str]:
    """Generate SQL and explanation from natural language using OpenAI."""
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    
    system_prompt = f"""You are a Snowflake SQL expert. Generate SQL queries based on natural language questions.

Table: {TABLE_NAME}

Schema:
{schema_description}

Rules:
1. Always use the exact table name: {TABLE_NAME}
2. CRITICAL: Always filter by a date range. Default to date = '{yesterday}' (yesterday) unless the user specifies a different date range. Every query MUST have a date filter.
3. Always add LIMIT {limit} at the end unless the user specifies a different limit
4. Use ONLY SELECT statements. Never use INSERT, UPDATE, DELETE, DROP, CREATE, ALTER, or any other modifying statements.
5. Use valid Snowflake SQL syntax
6. Column names are case-insensitive in Snowflake but preserve the case as shown in the schema
7. When counting visits, use SUM(visit_first_event) or COUNT with visit_first_event = 1
8. When analyzing sessions, use calculated_visit_id as the session identifier

Respond in JSON format:
{{
    "sql": "YOUR SQL QUERY HERE",
    "explanation": "A brief, clear explanation of what this query does and why you structured it this way"
}}
"""

    client = openai.OpenAI(api_key=st.secrets["OPENAI_API_KEY"])
    
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_question}
        ],
        temperature=0,
        response_format={"type": "json_object"}
    )
    
    result = json.loads(response.choices[0].message.content)
    return result.get("sql", ""), result.get("explanation", "")


def fix_failed_query(original_question: str, failed_sql: str, error_message: str, schema_description: str, limit: int) -> Tuple[str, str]:
    """Attempt to fix a failed query."""
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    
    system_prompt = f"""You are a Snowflake SQL expert. A query failed and you need to fix it.

Table: {TABLE_NAME}

Schema:
{schema_description}

Original question: {original_question}

Failed SQL:
{failed_sql}

Error message:
{error_message}

Rules:
1. Fix the SQL to resolve the error
2. Keep the original intent of the query
3. Always include a date filter (default: date = '{yesterday}')
4. Always add LIMIT {limit}
5. Use ONLY SELECT statements

Respond in JSON format:
{{
    "sql": "YOUR FIXED SQL QUERY HERE",
    "explanation": "What was wrong and how you fixed it"
}}
"""

    client = openai.OpenAI(api_key=st.secrets["OPENAI_API_KEY"])
    
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": "Please fix this query"}
        ],
        temperature=0,
        response_format={"type": "json_object"}
    )
    
    result = json.loads(response.choices[0].message.content)
    return result.get("sql", ""), result.get("explanation", "")


def validate_sql_safety(sql: str) -> Tuple[bool, str]:
    """Validate that SQL is safe (SELECT only)."""
    sql_upper = sql.upper().strip()
    
    dangerous_keywords = ['INSERT', 'UPDATE', 'DELETE', 'DROP', 'CREATE', 'ALTER', 'TRUNCATE', 'EXEC', 'EXECUTE', 'GRANT', 'REVOKE']
    
    for keyword in dangerous_keywords:
        if re.search(rf'\b{keyword}\b', sql_upper):
            return False, f"Query contains forbidden keyword: {keyword}"
    
    if not sql_upper.startswith('SELECT'):
        return False, "Only SELECT queries are allowed"
    
    return True, "Query is safe"


# =============================================================================
# UI COMPONENTS
# =============================================================================

def render_header():
    """Render the app header."""
    st.markdown("""
    <div class="app-header">
        <h1 class="app-title">
            <span>📊</span> Keshet Query Studio
        </h1>
        <p class="app-subtitle">Transform natural language into Snowflake SQL queries</p>
    </div>
    """, unsafe_allow_html=True)


def render_example_queries():
    """Render clickable example queries."""
    st.markdown("##### 💡 Try an example")
    cols = st.columns(4)
    for i, example in enumerate(EXAMPLE_QUERIES):
        with cols[i % 4]:
            if st.button(example[:40] + "..." if len(example) > 40 else example, key=f"example_{i}", use_container_width=True):
                st.session_state["user_question"] = example
                st.rerun()


def render_sidebar():
    """Render the sidebar with history and favorites."""
    with st.sidebar:
        st.markdown("### ⚙️ Settings")
        
        # Limit selector
        limit = st.slider("Max rows", min_value=10, max_value=1000, value=DEFAULT_LIMIT, step=10)
        st.session_state["query_limit"] = limit
        
        st.markdown("---")
        
        # Query History
        st.markdown("### 📜 Query History")
        if "query_history" in st.session_state and st.session_state["query_history"]:
            for i, item in enumerate(reversed(st.session_state["query_history"][-10:])):
                with st.container():
                    col1, col2 = st.columns([4, 1])
                    with col1:
                        if st.button(f"🕐 {item['question'][:35]}...", key=f"hist_{i}", use_container_width=True):
                            st.session_state["user_question"] = item["question"]
                            st.session_state["generated_sql"] = item["sql"]
                            st.session_state["sql_explanation"] = item.get("explanation", "")
                            st.session_state["gen_counter"] = st.session_state.get("gen_counter", 0) + 1
                            st.rerun()
                    with col2:
                        if st.button("⭐", key=f"fav_{i}"):
                            if "favorites" not in st.session_state:
                                st.session_state["favorites"] = []
                            if item not in st.session_state["favorites"]:
                                st.session_state["favorites"].append(item)
                                st.toast("Added to favorites!")
        else:
            st.caption("No queries yet")
        
        st.markdown("---")
        
        # Favorites
        st.markdown("### ⭐ Favorites")
        if "favorites" in st.session_state and st.session_state["favorites"]:
            for i, item in enumerate(st.session_state["favorites"]):
                col1, col2 = st.columns([4, 1])
                with col1:
                    if st.button(f"⭐ {item['question'][:35]}...", key=f"favitem_{i}", use_container_width=True):
                        st.session_state["user_question"] = item["question"]
                        st.session_state["generated_sql"] = item["sql"]
                        st.session_state["sql_explanation"] = item.get("explanation", "")
                        st.session_state["gen_counter"] = st.session_state.get("gen_counter", 0) + 1
                        st.rerun()
                with col2:
                    if st.button("🗑️", key=f"delfav_{i}"):
                        st.session_state["favorites"].remove(item)
                        st.rerun()
        else:
            st.caption("No favorites yet")


def render_cost_estimation(cost_info: dict):
    """Render query cost estimation."""
    warnings = []
    
    if not cost_info.get("has_date_filter"):
        warnings.append("⚠️ No date filter detected - query may scan large amounts of data")
    
    if not cost_info.get("has_limit"):
        warnings.append("⚠️ No LIMIT clause - may return many rows")
    
    if cost_info.get("estimated_rows") and cost_info["estimated_rows"] > 10000:
        warnings.append(f"⚠️ Estimated {cost_info['estimated_rows']:,} rows")
    
    if warnings:
        st.markdown(f"""
        <div class="cost-box cost-warning">
            <span>⚡</span>
            <div>
                <strong>Query Warnings</strong><br/>
                {"<br/>".join(warnings)}
            </div>
        </div>
        """, unsafe_allow_html=True)
    else:
        st.markdown("""
        <div class="cost-box">
            <span>✅</span>
            <div><strong>Query looks good!</strong> Date filter and limit detected.</div>
        </div>
        """, unsafe_allow_html=True)


def render_column_stats(stats: dict):
    """Render column statistics in an expander."""
    with st.expander("📊 Column Statistics"):
        stat_df = []
        for col, col_stats in stats.items():
            row = {
                "Column": col,
                "Distinct Values": col_stats["distinct"],
                "Null Count": col_stats["nulls"]
            }
            if "min" in col_stats:
                row["Min"] = col_stats["min"]
                row["Max"] = col_stats["max"]
                row["Mean"] = round(col_stats["mean"], 2) if col_stats["mean"] else None
            stat_df.append(row)
        st.dataframe(pd.DataFrame(stat_df), use_container_width=True, hide_index=True)


def render_visualization(df: pd.DataFrame):
    """Render auto-generated visualization based on data."""
    # Determine best chart type based on data
    numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
    categorical_cols = df.select_dtypes(include=['object', 'category']).columns.tolist()
    
    if len(df) == 0:
        st.info("No data to visualize")
        return
    
    # If we have a clear category + numeric pattern, show a bar chart
    if len(categorical_cols) >= 1 and len(numeric_cols) >= 1:
        cat_col = categorical_cols[0]
        num_col = numeric_cols[0]
        
        # Limit categories for readability
        if df[cat_col].nunique() <= 20:
            chart_data = df.groupby(cat_col)[num_col].sum().reset_index()
            chart_data = chart_data.sort_values(num_col, ascending=False).head(15)
            st.bar_chart(chart_data.set_index(cat_col))
        else:
            st.info("Too many categories for visualization. Showing top 15.")
            chart_data = df.groupby(cat_col)[num_col].sum().reset_index()
            chart_data = chart_data.sort_values(num_col, ascending=False).head(15)
            st.bar_chart(chart_data.set_index(cat_col))
    
    # If only numeric, show line or area chart
    elif len(numeric_cols) >= 1:
        st.line_chart(df[numeric_cols])
    
    else:
        st.info("No suitable columns found for automatic visualization")


# =============================================================================
# MAIN APP
# =============================================================================

def main():
    st.set_page_config(
        page_title=APP_NAME,
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    apply_custom_css()
    render_header()
    render_sidebar()
    
    # Initialize session state
    if "query_history" not in st.session_state:
        st.session_state["query_history"] = []
    if "gen_counter" not in st.session_state:
        st.session_state["gen_counter"] = 0
    if "query_limit" not in st.session_state:
        st.session_state["query_limit"] = DEFAULT_LIMIT
    
    # Fetch schema
    with st.spinner("Loading table schema..."):
        all_columns = get_all_columns()
    
    if not all_columns:
        st.error("Could not fetch table schema. Please check your Snowflake connection.")
        return
    
    # Info bar
    col1, col2, col3 = st.columns(3)
    with col1:
        st.markdown(f"""
        <div class="stat-box">
            <div class="stat-value">{len(all_columns)}</div>
            <div class="stat-label">Columns</div>
        </div>
        """, unsafe_allow_html=True)
    with col2:
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        st.markdown(f"""
        <div class="stat-box">
            <div class="stat-value">{yesterday}</div>
            <div class="stat-label">Default Date</div>
        </div>
        """, unsafe_allow_html=True)
    with col3:
        st.markdown(f"""
        <div class="stat-box">
            <div class="stat-value">{st.session_state["query_limit"]}</div>
            <div class="stat-label">Row Limit</div>
        </div>
        """, unsafe_allow_html=True)
    
    st.markdown("<br/>", unsafe_allow_html=True)
    
    # Example queries
    render_example_queries()
    
    st.markdown("<br/>", unsafe_allow_html=True)
    
    # Main query input
    st.markdown("##### 🔍 Ask a question about your data")
    user_question = st.text_area(
        "Enter your question",
        value=st.session_state.get("user_question", ""),
        placeholder="e.g., How many unique users visited mako from mobile devices yesterday?",
        height=100,
        label_visibility="collapsed",
        key="question_input"
    )
    
    col1, col2 = st.columns([1, 5])
    with col1:
        generate_btn = st.button("🚀 Generate Query", type="primary", use_container_width=True)
    
    # Generate query
    if generate_btn and user_question:
        with st.spinner("Generating SQL..."):
            schema_description = build_schema_description(all_columns)
            sql, explanation = generate_sql(user_question, schema_description, st.session_state["query_limit"])
            
            # Validate safety
            is_safe, safety_msg = validate_sql_safety(sql)
            if not is_safe:
                st.error(f"🚫 {safety_msg}")
                return
            
            st.session_state["generated_sql"] = sql
            st.session_state["sql_explanation"] = explanation
            st.session_state["current_question"] = user_question
            st.session_state["gen_counter"] += 1
            st.rerun()
    
    # Display generated SQL
    if "generated_sql" in st.session_state and st.session_state["generated_sql"]:
        st.markdown("---")
        st.markdown("##### 📝 Generated SQL")
        
        # Explanation
        if "sql_explanation" in st.session_state and st.session_state["sql_explanation"]:
            st.markdown(f"""
            <div class="explanation-box">
                <div class="explanation-title">💡 Query Explanation</div>
                <p style="margin:0; color: #78350f;">{st.session_state["sql_explanation"]}</p>
            </div>
            """, unsafe_allow_html=True)
        
        st.markdown("<br/>", unsafe_allow_html=True)
        
        # Cost estimation
        cost_info = estimate_query_cost(st.session_state["generated_sql"])
        render_cost_estimation(cost_info)
        
        st.markdown("<br/>", unsafe_allow_html=True)
        
        # SQL editor
        edited_sql = st.text_area(
            "SQL Query (editable)",
            value=st.session_state["generated_sql"],
            height=200,
            key=f"sql_editor_{st.session_state['gen_counter']}",
            label_visibility="collapsed"
        )
        
        # Preview and Execute buttons
        col1, col2, col3, col4 = st.columns([1, 1, 1, 3])
        with col1:
            preview_btn = st.button("👁️ Preview (10 rows)", use_container_width=True)
        with col2:
            execute_btn = st.button("▶️ Execute", type="primary", use_container_width=True)
        with col3:
            if st.button("🗑️ Clear", use_container_width=True):
                for key in ["generated_sql", "sql_explanation", "query_results", "current_question"]:
                    if key in st.session_state:
                        del st.session_state[key]
                st.rerun()
        
        # Preview execution
        if preview_btn:
            # Add LIMIT 10 for preview
            preview_sql = re.sub(r'LIMIT\s+\d+', 'LIMIT 10', edited_sql, flags=re.IGNORECASE)
            if 'LIMIT' not in preview_sql.upper():
                preview_sql = preview_sql.rstrip(';') + ' LIMIT 10'
            
            with st.spinner("Running preview..."):
                try:
                    df = execute_query(preview_sql)
                    st.markdown("##### 👁️ Preview Results (first 10 rows)")
                    st.dataframe(df, use_container_width=True, hide_index=True)
                except Exception as e:
                    st.error(f"Preview failed: {e}")
        
        # Full execution
        if execute_btn:
            # Validate again before execution
            is_safe, safety_msg = validate_sql_safety(edited_sql)
            if not is_safe:
                st.error(f"🚫 {safety_msg}")
            else:
                with st.spinner("Executing query..."):
                    try:
                        df = execute_query(edited_sql)
                        st.session_state["query_results"] = df
                        
                        # Add to history
                        history_item = {
                            "question": st.session_state.get("current_question", user_question),
                            "sql": edited_sql,
                            "explanation": st.session_state.get("sql_explanation", ""),
                            "timestamp": datetime.now().isoformat()
                        }
                        st.session_state["query_history"].append(history_item)
                        
                    except Exception as e:
                        st.error(f"Query execution failed: {e}")
                        
                        # Offer to fix
                        if st.button("🔧 Try to fix automatically"):
                            with st.spinner("Attempting to fix query..."):
                                schema_description = build_schema_description(all_columns)
                                fixed_sql, fix_explanation = fix_failed_query(
                                    st.session_state.get("current_question", user_question),
                                    edited_sql,
                                    str(e),
                                    schema_description,
                                    st.session_state["query_limit"]
                                )
                                st.session_state["generated_sql"] = fixed_sql
                                st.session_state["sql_explanation"] = fix_explanation
                                st.session_state["gen_counter"] += 1
                                st.rerun()
        
        # Display results
        if "query_results" in st.session_state:
            st.markdown("---")
            df = st.session_state["query_results"]
            
            # Results header with stats
            col1, col2, col3 = st.columns([2, 1, 1])
            with col1:
                st.markdown(f"##### 📊 Results ({len(df):,} rows)")
            with col2:
                view_mode = st.radio("View", ["Table", "Chart"], horizontal=True, label_visibility="collapsed")
            
            # Column stats
            stats = get_column_stats(df)
            render_column_stats(stats)
            
            # Results display
            if view_mode == "Table":
                st.dataframe(df, use_container_width=True, hide_index=True)
            else:
                render_visualization(df)
            
            # Export options
            st.markdown("<br/>", unsafe_allow_html=True)
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                csv = df.to_csv(index=False)
                st.download_button(
                    label="📥 Download CSV",
                    data=csv,
                    file_name="query_results.csv",
                    mime="text/csv",
                    use_container_width=True
                )
            with col2:
                json_data = df.to_json(orient="records", indent=2)
                st.download_button(
                    label="📥 Download JSON",
                    data=json_data,
                    file_name="query_results.json",
                    mime="application/json",
                    use_container_width=True
                )


if __name__ == "__main__":
    main()
