import os
import sys
import json
import time
import requests
import duckdb
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
REPO = 'duckdb/duckdb'

# Configuration - get MAX_PAGES from command line argument or use default
if len(sys.argv) > 1:
    try:
        MAX_PAGES = int(sys.argv[1])
        if MAX_PAGES < 1:
            print("Error: Number of pages must be at least 1")
            sys.exit(1)
    except ValueError:
        print(f"Error: Invalid argument '{sys.argv[1]}'. Please provide a number.")
        print("Usage: python extraction_script.py [pages]")
        print("Example: python extraction_script.py 5")
        sys.exit(1)
else:
    MAX_PAGES = 10  # Default to 10 pages if no argument provided

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
            contents VARCHAR,
            _extracted_at TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS raw.raw_data_commits (
            contents VARCHAR,
            _extracted_at TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS raw.raw_data_issues (
            contents VARCHAR,
            _extracted_at TIMESTAMP
        );
    """)

def extract_pull_requests(con):
    print("Extracting pull requests...")
    print("  Step 1: Fetching PR list...")
    page = 1
    total = 0
    skipped = 0
    pr_numbers = []
    
    # Step 1: Get list of all PRs
    while page <= MAX_PAGES:
        try:
            prs = github_get(
                f"https://api.github.com/repos/{REPO}/pulls",
                params={"state": "all", "per_page": 100, "page": page}
            )
            
            # Handle None response from failed retries
            if prs is None:
                print(f"    Page {page}: Failed to fetch, skipping page")
                page += 1
                time.sleep(2)
                continue
                
            if not prs:
                break

            # Collect PR numbers for detailed fetching
            for pr in prs:
                pr_numbers.append(pr.get('number'))

            print(f"    ✓ Page {page}: Found {len(prs)} PRs")
            page += 1
            time.sleep(0.5)
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 422:
                print(f"    Reached pagination limit at page {page}")
                break
            print(f"    ⚠ HTTP error on page {page}: {e}")
            page += 1
            time.sleep(2)
            continue
        except Exception as e:
            print(f"    ⚠ Unexpected error on page {page}: {e}")
            page += 1
            time.sleep(2)
            continue
    
    # Step 2: Fetch detailed data for each PR
    print(f"  Step 2: Fetching detailed data for {len(pr_numbers)} PRs...")
    for i, pr_number in enumerate(pr_numbers, 1):
        try:
            # Fetch individual PR with full details (includes additions/deletions)
            pr_detail = github_get(
                f"https://api.github.com/repos/{REPO}/pulls/{pr_number}"
            )
            
            if pr_detail is None:
                print(f"    ⚠ Skipping PR #{pr_number}: Failed to fetch details")
                skipped += 1
                continue
            
            con.execute(
                "INSERT INTO raw.raw_data_pull_requests VALUES (?, ?)",
                [json.dumps(pr_detail), datetime.utcnow()]
            )
            total += 1
            
            # Progress update every 10 PRs
            if i % 10 == 0:
                print(f"    Progress: {i}/{len(pr_numbers)} PRs processed...")
            
            time.sleep(0.3)  # Rate limiting
            
        except Exception as e:
            print(f"    ⚠ Skipping PR #{pr_number}: {e}")
            skipped += 1
            continue
    
    summary = f"Completed: {total} pull requests extracted"
    if skipped > 0:
        summary += f" ({skipped} skipped due to errors)"
    print(f"  {summary}")

def extract_commits(con):
    print("Extracting commits...")
    print("  Step 1: Fetching commit list...")
    page = 1
    total = 0
    skipped = 0
    commit_shas = []
    
    # Step 1: Get list of all commits
    while page <= MAX_PAGES:
        try:
            commits = github_get(
                f"https://api.github.com/repos/{REPO}/commits",
                params={"per_page": 100, "page": page}
            )
            
            # Handle None response from failed retries
            if commits is None:
                print(f"    Page {page}: Failed to fetch, skipping page")
                page += 1
                time.sleep(2)
                continue
                
            if not commits:
                break

            # Collect commit SHAs for detailed fetching
            for commit in commits:
                commit_shas.append(commit.get('sha'))

            print(f"    ✓ Page {page}: Found {len(commits)} commits")
            page += 1
            time.sleep(0.5)
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 422:
                print(f"    Reached pagination limit at page {page}")
                break
            print(f"    ⚠ HTTP error on page {page}: {e}")
            page += 1
            time.sleep(2)
            continue
        except Exception as e:
            print(f"    ⚠ Unexpected error on page {page}: {e}")
            page += 1
            time.sleep(2)
            continue
    
    # Step 2: Fetch detailed data for each commit
    print(f"  Step 2: Fetching detailed data for {len(commit_shas)} commits...")
    for i, sha in enumerate(commit_shas, 1):
        try:
            # Fetch individual commit with full details (includes stats)
            commit_detail = github_get(
                f"https://api.github.com/repos/{REPO}/commits/{sha}"
            )
            
            if commit_detail is None:
                print(f"    ⚠ Skipping commit {sha[:7]}: Failed to fetch details")
                skipped += 1
                continue
            
            con.execute(
                "INSERT INTO raw.raw_data_commits VALUES (?, ?)",
                [json.dumps(commit_detail), datetime.utcnow()]
            )
            total += 1
            
            # Progress update every 10 commits
            if i % 10 == 0:
                print(f"    Progress: {i}/{len(commit_shas)} commits processed...")
            
            time.sleep(0.3)  # Rate limiting
            
        except Exception as e:
            print(f"    ⚠ Skipping commit {sha[:7]}: {e}")
            skipped += 1
            continue
    
    summary = f"Completed: {total} commits extracted"
    if skipped > 0:
        summary += f" ({skipped} skipped due to errors)"
    print(f"  {summary}")

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
    # Show usage info
    print("=" * 60)
    print("GitHub Data Extraction Script")
    print("=" * 60)
    if len(sys.argv) > 1:
        print(f"Running with {MAX_PAGES} pages (specified via command line)")
    else:
        print(f"Running with {MAX_PAGES} pages (default)")
        print("Tip: Run with 'python extraction_script.py <pages>' to specify")
    print(f"Repository: {REPO}")
    print("=" * 60)
    
    con = duckdb.connect(DB_PATH)
    init_db(con)
    
    extract_pull_requests(con)
    print()
    extract_commits(con)
    print()
    extract_issues(con)
    
    con.close()
    print("=" * 60)
    print("✓ Extraction complete!")
    print("=" * 60)

if __name__ == "__main__":
    main()