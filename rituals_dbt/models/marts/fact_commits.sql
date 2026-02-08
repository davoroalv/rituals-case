{{
    config(
        materialized='table'
    )
}}

-- Grain: One row per commit
-- This fact table captures commit activity and code changes

with commits as (
    select * from {{ ref('stg_commits') }}
),

final as (
    select
        -- Primary keys
        commit_sha,
        
        -- Foreign keys
        github_author_id as contributor_id,
        
        -- Commit details
        commit_message,
        
        -- Extract first line as summary
        split_part(commit_message, chr(10), 1) as commit_summary,
        
        -- Timestamps
        commit_author_date,
        committer_date,
        
        -- Code change metrics
        additions,
        deletions,
        total_changes,
        
        -- Categorize commit size
        case
            when total_changes < 10 then 'Tiny'
            when total_changes < 50 then 'Small'
            when total_changes < 200 then 'Medium'
            when total_changes < 1000 then 'Large'
            else 'Huge'
        end as commit_size,
        
        -- Author information
        commit_author_name,
        commit_author_email,
        github_author_login,
        
        -- Committer information
        committer_name,
        committer_email,
        github_committer_login,
        
        -- Check if author and committer are different (e.g., merged PRs)
        case
            when commit_author_email != committer_email then true
            else false
        end as is_different_committer,
        
        -- Links
        html_url,
        
        -- Metadata
        _extracted_at,
        current_timestamp as _dbt_created_at
        
    from commits
    -- Defensive: exclude potential duplicates
    qualify row_number() over (partition by commit_sha order by _extracted_at desc) = 1
)

select * from final