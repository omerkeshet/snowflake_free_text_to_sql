# Keshet Query Studio

A professional natural language to Snowflake SQL query tool built for Keshet Media Group.

![Keshet Query Studio](https://img.shields.io/badge/Keshet-Query%20Studio-7c3aed)

## Features

### Query Management
- **Query History** — Automatically saves your queries for easy re-use
- **Favorites** — Star your most-used queries for quick access
- **Example Queries** — Click-to-run example queries to get started

### Usability
- **Natural Language Input** — Just describe what you want in plain English/Hebrew
- **Query Explanation** — AI explains what each generated query does
- **Auto-Fix** — If a query fails, the AI can attempt to fix it automatically
- **SQL Editor** — Review and edit generated SQL before running

### Results & Visualization
- **Table/Chart Toggle** — Switch between table and chart views
- **Preview Mode** — Preview first 10 rows before full execution
- **Column Statistics** — View distinct counts, min/max, nulls for each column
- **Export** — Download results as CSV or JSON

### Safety & Control
- **SELECT Only** — Only read queries allowed, no modifications to data
- **Date Filter Required** — All queries must have a date range (default: yesterday)
- **Adjustable Limit** — Control max rows returned (default: 100)
- **Cost Warnings** — Alerts for queries that might scan too much data

## Setup

### 1. Clone this repository

### 2. Install dependencies
```bash
pip install -r requirements.txt
```

### 3. Configure secrets

**For Streamlit Cloud:**

Go to your app Settings → Secrets and add:

```toml
OPENAI_API_KEY = "sk-your-openai-api-key"

[snowflake]
account = "el66449.eu-central-1"
user = "OmerY"
database = "MAKO_DATA_LAKE"
schema = "PUBLIC"
warehouse = "COMPUTE_WH"
role = "ACCOUNTADMIN"
private_key = """-----BEGIN PRIVATE KEY-----
YOUR PRIVATE KEY HERE
-----END PRIVATE KEY-----"""
```

### 4. Deploy
Push to GitHub and connect to Streamlit Cloud at [share.streamlit.io](https://share.streamlit.io)

## Usage

1. **Ask a question** — Type naturally, e.g., "How many users visited mako yesterday?"
2. **Review the SQL** — Check the generated query and explanation
3. **Preview** — Click "Preview" to see first 10 rows
4. **Execute** — Run the full query
5. **Analyze** — View results as table or chart, check column stats
6. **Export** — Download as CSV or JSON

## Tech Stack

- **Frontend**: Streamlit
- **AI**: OpenAI GPT-4o-mini
- **Database**: Snowflake
- **Auth**: Snowflake Key-Pair Authentication

---

Built with ❤️ for Keshet Media Group
