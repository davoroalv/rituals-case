OVERVIEW
This is work behind a Rituals case study. 

There is an extraction script to get GitHub data into a database (in my case using the DuckDB database).
I chose the DuckDB database because of the ease of use and setup.

The script pulls the first X ( a parameter) pages from commits, issues and pull_requests and drops each into its own table in DuckDB.

There is a rituals_dbt folder which has the SQL/dbt transformation logic.

SETUP:
git clone https://github.com/davoroalv/rituals-case.git
cd rituals-case

(Assuming you are using a Mac):
python3.12 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

Github authorization:
in a .env file, you need to add your OWN github token. This can be created via the Settings part of your profile.
GITHUB_TOKEN=Your_Own_Generated_Token_Goes_Here_Please

Getting Data:
inside of the rituals-case folder, run-
python extraction_script.py

This creates data inside of the data folder. You can modify the number of pages pulled - it is a parameter towards the top of the script. i.e. if you are testing that this works, just run it with 5 pages or so.

dbt work:
You can the change directory to the dbt part of this case- 
cd rituals_dbt
and run-
dbt run
in order to run all the dbt scripts, which will create your staging tables, your marts tables and KPIs tables.

To play around with the data:
You can use duckdb data/rituals.duckdb from terminal/VS code or such
If you prefer other interfaces (i.e. a free and popular one, dbeaver), you can download dbeaver, create a new connection (select DuckDB) and choose your rituals.duckdb file as the source


DATA MODEL OVERVIEW

dim_contributors:
Key - user_id

fact_pull_requests:
Key - pr_id
Foreign Key - author_id (dim_contributor.author_id)

fact_commits
Key - commit_sha
Foreign Key - author_id (dim_contributor.user_id)

KPI DEFINITIONS

contributor_impact.sql will return:
Weekly PR activity per developer showing total PRs, merged count, code changes (additions/deletions/net), files touched, and lines-per-PR metrics, excluding drafts and non-users.

contributor_work.sql will return:
Weekly commit activity per developer tracking total commits, commit type (local vs web-based), code changes (additions/deletions/net), and average commit size, excluding non-users.

cycle_time_by_author.sql will return:
Monthly PR metrics per developer tracking merge time, PR count, discussion activity (comments/review comments), commits, and per-PR averages, excluding drafts and non-users.

(DO Note - I decided to use monthly here to see things on a longer-term window, compared to work and impact which could be measured more closely/granularly)

Weekly open PR tracking by developer and staleness category showing count, time since last update, code size metrics (additions/deletions/total changes), and size distribution (small/medium/large) to analyze correlation between PR size and staleness.

Future Development Work
building out more dbt tests, e.g. accepted ranges for code - i.e. something greater than 500K or 1M might be obviously wrong (tbd by looking at ranges)

build incremental tables using updated_at, so that new data from github is added at a daily/weekly level

build partitions for more efficient querying

expand this to other repositories

Design Decisions (Tools)
DuckDB - easiest database to setup, no Dockerfile required e.g. for Postgres. for this 4hr exercise, made the most sense, I thought

DBT - common tool for analytics engineering

Python - common tool for API calls

Design Decisions (Data)
Move data retrieved via API calls from git into raw tables, containing all raw data

Move data from raw to staging tables, extracting values

From Staging, move data into Marts tables:

Dim Contributors - includes any user who raised an issue, made a commit, or made a PR, activity for each of these, when it exists, and the latest activity

Fact Commits - includes one row per commit_sha, and data around this - the author, date and how many changes

Fact Pull Requests - includes one row per PR, and data around this - author, created timestamp, updated timestamp, comment and line changes

From Marts to KPI tables (KPI definitions above)

