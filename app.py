import streamlit as st
import openai
import snowflake.connector
from datetime import datetime, timedelta

# Page config
st.set_page_config(page_title="Snowflake Query Generator", page_icon="❄️", layout="wide")
st.title("❄️ Natural Language to Snowflake SQL")

# Constants
TABLE_NAME = "mako_data_lake.public.combined_events_enriched"
DEFAULT_LIMIT = 100

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


@st.cache_resource
def get_snowflake_connection():
    """Create a Snowflake connection using key-pair authentication."""
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives import serialization
    
    # Load private key
    private_key_pem = st.secrets["snowflake"]["private_key"]
    private_key = serialization.load_pem_private_key(
        private_key_pem.encode(),
        password=None,
        backend=default_backend()
    )
    
    # Get the private key bytes in the format Snowflake expects
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
        return [(col[0], col[1]) for col in columns]  # (name, type)
    except Exception as e:
        st.error(f"Error fetching columns: {e}")
        return []


def build_schema_description(all_columns):
    """Build a schema description for the LLM."""
    lines = []
    
    for col_name, col_type in all_columns:
        # Check if this is an important column (case-insensitive)
        important_info = IMPORTANT_COLUMNS.get(col_name) or IMPORTANT_COLUMNS.get(col_name.upper()) or IMPORTANT_COLUMNS.get(col_name.lower())
        
        if important_info:
            desc = f"- {col_name} ({important_info['type']}): {important_info['description']}"
            if important_info['values']:
                desc += f" Possible values: {important_info['values']}"
            lines.append(desc)
        else:
            lines.append(f"- {col_name} ({col_type})")
    
    return "\n".join(lines)


def generate_sql(user_question: str, schema_description: str) -> str:
    """Generate SQL from natural language using OpenAI."""
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    
    system_prompt = f"""You are a Snowflake SQL expert. Generate SQL queries based on natural language questions.

Table: {TABLE_NAME}

Schema:
{schema_description}

Rules:
1. Always use the exact table name: {TABLE_NAME}
2. By default, filter by date = '{yesterday}' (yesterday) unless the user specifies a different date range
3. Always add LIMIT {DEFAULT_LIMIT} at the end unless the user specifies a different limit
4. Return ONLY the SQL query, no explanations, no markdown code blocks
5. Use valid Snowflake SQL syntax
6. Column names are case-insensitive in Snowflake but preserve the case as shown in the schema
7. When counting visits, use SUM(visit_first_event) or COUNT with visit_first_event = 1
8. When analyzing sessions, use calculated_visit_id as the session identifier
"""

    client = openai.OpenAI(api_key=st.secrets["OPENAI_API_KEY"])
    
    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_question}
        ],
        temperature=0
    )
    
    return response.choices[0].message.content.strip()


def execute_query(sql: str):
    """Execute SQL query and return results as a dataframe."""
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    cursor.execute(sql)
    
    # Fetch results
    columns = [desc[0] for desc in cursor.description]
    data = cursor.fetchall()
    cursor.close()
    
    import pandas as pd
    return pd.DataFrame(data, columns=columns)


# Main app
def main():
    # Fetch schema on startup
    with st.spinner("Loading table schema..."):
        all_columns = get_all_columns()
    
    if not all_columns:
        st.error("Could not fetch table schema. Please check your Snowflake connection.")
        return
    
    st.caption(f"Table: `{TABLE_NAME}` | {len(all_columns)} columns | Default: yesterday's data, limit {DEFAULT_LIMIT}")
    
    # User input
    user_question = st.text_area(
        "Ask a question about your data:",
        placeholder="e.g., How many unique users visited mako from mobile devices?",
        height=100
    )
    
    # Generate query button
    if st.button("🔍 Generate Query", type="primary"):
        if not user_question:
            st.warning("Please enter a question.")
            return
        
        with st.spinner("Generating SQL..."):
            schema_description = build_schema_description(all_columns)
            sql = generate_sql(user_question, schema_description)
            st.session_state["generated_sql"] = sql
    
    # Display generated SQL
    if "generated_sql" in st.session_state:
        st.subheader("Generated SQL")
        
        # Editable SQL
        edited_sql = st.text_area(
            "Review and edit if needed:",
            value=st.session_state["generated_sql"],
            height=200,
            key="sql_editor"
        )
        
        # Execute button
        col1, col2 = st.columns([1, 5])
        with col1:
            execute_btn = st.button("▶️ Execute", type="secondary")
        with col2:
            if st.button("🗑️ Clear"):
                del st.session_state["generated_sql"]
                if "query_results" in st.session_state:
                    del st.session_state["query_results"]
                st.rerun()
        
        if execute_btn:
            with st.spinner("Executing query..."):
                try:
                    df = execute_query(edited_sql)
                    st.session_state["query_results"] = df
                except Exception as e:
                    st.error(f"Query execution failed: {e}")
    
    # Display results
    if "query_results" in st.session_state:
        st.subheader("Results")
        df = st.session_state["query_results"]
        st.caption(f"{len(df)} rows returned")
        st.dataframe(df, use_container_width=True)
        
        # Download button
        csv = df.to_csv(index=False)
        st.download_button(
            label="📥 Download CSV",
            data=csv,
            file_name="query_results.csv",
            mime="text/csv"
        )


if __name__ == "__main__":
    main()
