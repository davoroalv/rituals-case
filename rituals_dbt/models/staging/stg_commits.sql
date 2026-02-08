{{
    config(
        materialized='view'
    )
}}

with source as (
    select * from {{ source('github_raw', 'raw_data_commits') }}
    where payload is not null
),

parsed as (
    select
        -- Primary keys
        payload->>'$.sha' as commit_sha,
        payload->>'$.node_id' as commit_node_id,
        
        -- Commit details
        payload->>'$.commit.message' as commit_message,
        cast(payload->>'$.commit.comment_count' as integer) as comment_count,
        
        -- Author information (from commit object)
        payload->>'$.commit.author.name' as commit_author_name,
        payload->>'$.commit.author.email' as commit_author_email,
        cast(payload->>'$.commit.author.date' as timestamp) as commit_author_date,
        
        -- GitHub user information (from API)
        cast(payload->>'$.author.id' as bigint) as github_author_id,
        payload->>'$.author.login' as github_author_login,
        payload->>'$.author.type' as github_author_type,
        
        -- Committer information
        payload->>'$.commit.committer.name' as committer_name,
        payload->>'$.commit.committer.email' as committer_email,
        cast(payload->>'$.commit.committer.date' as timestamp) as committer_date,
        
        -- GitHub committer information
        cast(payload->>'$.committer.id' as bigint) as github_committer_id,
        payload->>'$.committer.login' as github_committer_login,
        
        -- Parent commits
        payload->>'$.parents' as parents,
        
        -- Links
        payload->>'$.html_url' as html_url,
        
        -- Stats
        cast(payload->>'$.stats.additions' as integer) as additions,
        cast(payload->>'$.stats.deletions' as integer) as deletions,
        cast(payload->>'$.stats.total' as integer) as total_changes,
        
        -- Metadata
        _extracted_at
        
    from source
)

select * from parsed