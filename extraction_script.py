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

def github_get(url, params=None, max_retries=3):
    """
    Make a GET request to GitHub API with retry logic for 502 errors.
    Returns JSON response or None if all retries fail.
    """
    for attempt in range(max_retries):
        try:
            r = requests.get(url, headers=HEADERS, params=params, timeout=30)
            r.raise_for_status()
            return r.json()
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 502 and attempt < max_retries - 1:
                wait_time = 2 ** attempt  # Exponential backoff: 1s, 2s, 4s
                print(f"    502 Bad Gateway - retrying in {wait_time}s (attempt {attempt + 1}/{max_retries})...")
                time.sleep(wait_time)
                continue
            elif e.response.status_code == 502:
                print(f"    502 Bad Gateway - max retries reached, skipping")
                return None
            raise
        except requests.exceptions.Timeout:
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt
                print(f"    Request timeout - retrying in {wait_time}s (attempt {attempt + 1}/{max_retries})...")
                time.sleep(wait_time)
                continue
            else:
                print(f"    Request timeout - max retries reached, skipping")
                return None
    return None

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
    skipped = 0
    
    while page <= MAX_PAGES:
        try:
            prs = github_get(
                f"https://api.github.com/repos/{REPO}/pulls",
                params={"state": "all", "per_page": 100, "page": page}
            )
            
            # Handle None response from failed retries
            if prs is None:
                print(f"  Page {page}: Failed to fetch, skipping page")
                page += 1
                time.sleep(2)
                continue
                
            if not prs:
                break

            for pr in prs:
                try:
                    con.execute(
                        "INSERT INTO raw.raw_data_pull_requests VALUES (?, ?)",
                        [json.dumps(pr), datetime.utcnow()]
                    )
                    total += 1
                except Exception as e:
                    pr_number = pr.get('number', 'unknown')
                    print(f"  ⚠ Skipping PR #{pr_number}: {e}")
                    skipped += 1
                    continue

            status = f"total: {total}"
            if skipped > 0:
                status += f", skipped: {skipped}"
            print(f"  ✓ Page {page}: {len(prs)} PRs ({status})")
            page += 1
            time.sleep(0.5)
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 422:
                print(f"  Reached pagination limit at page {page}")
                break
            print(f"  ⚠ HTTP error on page {page}: {e}")
            page += 1
            time.sleep(2)
            continue
        except Exception as e:
            print(f"  ⚠ Unexpected error on page {page}: {e}")
            page += 1
            time.sleep(2)
            continue
    
    summary = f"Completed: {total} pull requests extracted"
    if skipped > 0:
        summary += f" ({skipped} skipped due to errors)"
    print(summary)

def extract_commits(con):
    print("Extracting commits...")
    page = 1
    total = 0
    skipped = 0
    
    while page <= MAX_PAGES:
        try:
            commits = github_get(
                f"https://api.github.com/repos/{REPO}/commits",
                params={"per_page": 100, "page": page}
            )
            
            # Handle None response from failed retries
            if commits is None:
                print(f"  Page {page}: Failed to fetch, skipping page")
                page += 1
                time.sleep(2)
                continue
                
            if not commits:
                break

            for commit in commits:
                try:
                    con.execute(
                        "INSERT INTO raw.raw_data_commits VALUES (?, ?)",
                        [json.dumps(commit), datetime.utcnow()]
                    )
                    total += 1
                except Exception as e:
                    commit_sha = commit.get('sha', 'unknown')[:7]
                    print(f"  ⚠ Skipping commit {commit_sha}: {e}")
                    skipped += 1
                    continue

            status = f"total: {total}"
            if skipped > 0:
                status += f", skipped: {skipped}"
            print(f"  ✓ Page {page}: {len(commits)} commits ({status})")
            page += 1
            time.sleep(0.5)
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 422:
                print(f"  Reached pagination limit at page {page}")
                break
            print(f"  ⚠ HTTP error on page {page}: {e}")
            page += 1
            time.sleep(2)
            continue
        except Exception as e:
            print(f"  ⚠ Unexpected error on page {page}: {e}")
            page += 1
            time.sleep(2)
            continue
    
    summary = f"Completed: {total} commits extracted"
    if skipped > 0:
        summary += f" ({skipped} skipped due to errors)"
    print(summary)

def extract_issues(con):
    print("Extracting issues...")
    page = 1
    total = 0
    skipped = 0
    
    while page <= MAX_PAGES:
        try:
            issues = github_get(
                f"https://api.github.com/repos/{REPO}/issues",
                params={"state": "all", "per_page": 100, "page": page}
            )
            
            # Handle None response from failed retries
            if issues is None:
                print(f"  Page {page}: Failed to fetch, skipping page")
                page += 1
                time.sleep(2)
                continue
                
            if not issues:
                break

            page_issues = 0
            for issue in issues:
                # Skip pull requests (they appear in the issues endpoint too)
                if 'pull_request' in issue:
                    continue
                    
                try:
                    con.execute(
                        "INSERT INTO raw.raw_data_issues VALUES (?, ?)",
                        [json.dumps(issue), datetime.utcnow()]
                    )
                    total += 1
                    page_issues += 1
                except Exception as e:
                    issue_number = issue.get('number', 'unknown')
                    print(f"  ⚠ Skipping issue #{issue_number}: {e}")
                    skipped += 1
                    continue

            status = f"total: {total}"
            if skipped > 0:
                status += f", skipped: {skipped}"
            print(f"  ✓ Page {page}: {page_issues} issues ({status})")
            page += 1
            time.sleep(0.5)
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 422:
                print(f"  Reached pagination limit at page {page}")
                break
            print(f"  ⚠ HTTP error on page {page}: {e}")
            page += 1
            time.sleep(2)
            continue
        except Exception as e:
            print(f"  ⚠ Unexpected error on page {page}: {e}")
            page += 1
            time.sleep(2)
            continue
    
    summary = f"Completed: {total} issues extracted"
    if skipped > 0:
        summary += f" ({skipped} skipped due to errors)"
    print(summary)

def main():
    print(f"Starting extraction (limited to {MAX_PAGES} pages per endpoint)...")
    print(f"Repository: {REPO}")
    print("-" * 60)
    
    con = duckdb.connect(DB_PATH)
    init_db(con)
    
    extract_pull_requests(con)
    print()
    extract_commits(con)
    print()
    extract_issues(con)
    
    con.close()
    print("-" * 60)
    print("✓ Extraction complete!")

if __name__ == "__main__":
    main()