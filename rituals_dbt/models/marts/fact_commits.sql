{{
    config(
        materialized='table'
    )
}}

with commits as (
	select * 
	from rituals.rituals_staging.stg_commits
)

select 
commit_sha, -- this is the key
github_author_id as author_id, -- this is the foreign key
commit_author_date,
case when github_author_id = github_committer_id then 'local'
else 'web-based' end as commit_type,
additions,
deletions,
total_changes,
additions-deletions as net_changes,
from commits