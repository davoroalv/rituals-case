{{
    config(
        materialized='view'
    )
}}

with source as (
    select * from {{ source('github_raw', 'raw_data_commits') }}
    where contents is not null
),

parsed as (
    select
        -- Primary keys
        contents->>'$.sha' as commit_sha,
        contents->>'$.node_id' as commit_node_id,
        
        -- Commit details
        contents->>'$.commit.message' as commit_message,
        cast(contents->>'$.commit.comment_count' as integer) as comment_count,
        
        -- Author information (from commit object - git metadata)
        contents->>'$.commit.author.name' as commit_author_name,
        contents->>'$.commit.author.email' as commit_author_email,
        cast(contents->>'$.commit.author.date' as timestamp) as commit_author_date,
        
        -- GitHub user information (from API - may be null if email doesn't match)
        cast(contents->>'$.author.id' as bigint) as github_author_id,
        contents->>'$.author.login' as github_author_login,
        contents->>'$.author.type' as github_author_type,
        
        -- Committer information (person who committed - often different from author)
        contents->>'$.commit.committer.name' as committer_name,
        contents->>'$.commit.committer.email' as committer_email,
        cast(contents->>'$.commit.committer.date' as timestamp) as committer_date,
        
        -- GitHub committer information
        cast(contents->>'$.committer.id' as bigint) as github_committer_id,
        contents->>'$.committer.login' as github_committer_login,
        
        -- Parent commits (for merge detection)
        contents->>'$.parents' as parents,
        
        -- Links
        contents->>'$.html_url' as html_url,
        
        -- Stats (code changes)
        cast(contents->>'$.stats.additions' as integer) as additions,
        cast(contents->>'$.stats.deletions' as integer) as deletions,
        cast(contents->>'$.stats.total' as integer) as total_changes,
        
        -- Metadata
        _extracted_at
        
    from source
)

select * from parsed