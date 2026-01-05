# Snowflake Natural Language Query Generator

A Streamlit app that converts natural language questions into Snowflake SQL queries using OpenAI.

## Features

- Natural language to SQL conversion
- Review generated queries before execution
- Edit queries if needed
- Execute against Snowflake and view results
- Download results as CSV
- Default filters: yesterday's data, limit 100 rows

## Setup

### 1. Clone/Download this repository

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure secrets

**For local development:**

Create `.streamlit/secrets.toml`:

```toml
OPENAI_API_KEY = "sk-your-openai-api-key"

[snowflake]
account = "your-account-identifier"
user = "your-username"
password = "your-password"
warehouse = "your-warehouse"
database = "mako_data_lake"
schema = "public"
```

**For Streamlit Cloud:**

1. Push your code to GitHub (without secrets.toml!)
2. Go to [share.streamlit.io](https://share.streamlit.io)
3. Deploy your app
4. Go to Settings → Secrets
5. Add the same secrets in TOML format

### 4. Run locally

```bash
streamlit run app.py
```

## Usage

1. Type your question in natural language (e.g., "How many unique users from Israel visited mako yesterday?")
2. Click "Generate Query"
3. Review the generated SQL
4. Edit if needed
5. Click "Execute" to run against Snowflake
6. View/download results

## Notes

- Queries default to yesterday's data unless you specify a date
- Results are limited to 100 rows unless you specify otherwise
- The app fetches all table columns on startup, with detailed descriptions for 25 key columns
