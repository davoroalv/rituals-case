import os
import json
import time
import requests
import duckdb
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
REPO = 'duckdb/duckdb'

# Configuration - set page limits here
MAX_PAGES = 2  # ← Change this to control how many pages to pull

HEADERS = {
    "Authorization": f"Bearer {GITHUB_TOKEN}",
    "Accept": "application/vnd.github+json"
}

DB_PATH = "data/rituals.duckdb"

def github_get(url, params=None):
    r = requests.get(url, headers=HEADERS, params=params)
    r.raise_for_status()
    return r.json()

def init_db(con):
    # Create the schema first
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")
    
    # Create tables in the raw schema
    con.execute("""
        CREATE TABLE IF NOT EXISTS raw.raw_data_pull_requests (
            payload VARCHAR,
            _extracted_at TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS raw.raw_data_commits (
            payload VARCHAR,
            _extracted_at TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS raw.raw_data_issues (
            payload VARCHAR,
            _extracted_at TIMESTAMP
        );
    """)

def extract_pull_requests(con):
    print("Extracting pull requests...")
    page = 1
    total = 0
    while page <= MAX_PAGES:  # ← Added page limit check
        try:
            prs = github_get(
                f"https://api.github.com/repos/{REPO}/pulls",
                params={"state": "all", "per_page": 100, "page": page}
            )
            if not prs:
                break

            for pr in prs:
                con.execute(
                    "INSERT INTO raw.raw_data_pull_requests VALUES (?, ?)",
                    [json.dumps(pr), datetime.utcnow()]
                )
                total += 1

            print(f"  Page {page}: {len(prs)} PRs (total: {total})")
            page += 1
            time.sleep(0.5)
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 422:
                print(f"  Reached pagination limit at page {page}")
                break
            raise
    
    print(f"Completed: {total} pull requests extracted")

def extract_commits(con):
    print("Extracting commits...")
    page = 1
    total = 0
    while page <= MAX_PAGES:  # ← Added page limit check
        try:
            commits = github_get(
                f"https://api.github.com/repos/{REPO}/commits",
                params={"per_page": 100, "page": page}
            )
            if not commits:
                break

            for commit in commits:
                con.execute(
                    "INSERT INTO raw.raw_data_commits VALUES (?, ?)",
                    [json.dumps(commit), datetime.utcnow()]
                )
                total += 1

            print(f"  Page {page}: {len(commits)} commits (total: {total})")
            page += 1
            time.sleep(0.5)
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 422:
                print(f"  Reached pagination limit at page {page}")
                break
            raise
    
    print(f"Completed: {total} commits extracted")

def extract_issues(con):
    print("Extracting issues...")
    page = 1
    total = 0
    while page <= MAX_PAGES:  # ← Added page limit check
        try:
            issues = github_get(
                f"https://api.github.com/repos/{REPO}/issues",
                params={"state": "all", "per_page": 100, "page": page}
            )
            if not issues:
                break

            for issue in issues:
                # Skip pull requests (they appear in the issues endpoint too)
                if 'pull_request' not in issue:
                    con.execute(
                        "INSERT INTO raw.raw_data_issues VALUES (?, ?)",
                        [json.dumps(issue), datetime.utcnow()]
                    )
                    total += 1

            print(f"  Page {page}: {len([i for i in issues if 'pull_request' not in i])} issues (total: {total})")
            page += 1
            time.sleep(0.5)
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 422:
                print(f"  Reached pagination limit at page {page}")
                break
            raise
    
    print(f"Completed: {total} issues extracted")

def main():
    print(f"Starting extraction (limited to {MAX_PAGES} pages per endpoint)...")
    con = duckdb.connect(DB_PATH)
    init_db(con)
    extract_pull_requests(con)
    extract_commits(con)
    extract_issues(con)
    con.close()
    print("\nExtraction complete!")

if __name__ == "__main__":
    main()