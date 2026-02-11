{{
    config(
        materialized='table'
    )
}}

with prs as (
	select * 
	from {{ ref('stg_pull_requests') }}
)
select 
pr_id,
pr_number,
pr_title,
pr_state,
is_draft,
author_id,
created_at,
updated_at,
closed_at,
merged_at,
merge_commit_sha,
is_merged,
comment_count,
review_comment_count,
commit_count,
additions,
deletions,
additions+deletions as total_changes,
additions-deletions as net_changes,
changed_files,
base_branch
from prs