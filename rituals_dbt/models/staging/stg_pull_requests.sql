{{
    config(
        materialized='view'
    )
}}

-- Staging: Parse raw JSON into typed columns
-- Grain: One row per pull request
-- Source: raw.raw_data_pull_requests

with source as (
    select * from {{ source('github_raw', 'raw_data_pull_requests') }}
    where contents is not null  -- Defensive: exclude any null payloads
),

parsed as (
    select
        -- Primary keys
        cast(contents->>'$.id' as bigint) as pr_id,
        cast(contents->>'$.number' as integer) as pr_number,
        contents->>'$.node_id' as pr_node_id,
        
        -- PR attributes
        contents->>'$.title' as pr_title,
        contents->>'$.state' as pr_state,
        (contents->>'$.draft')::boolean as is_draft,
        (contents->>'$.locked')::boolean as is_locked,
        
        -- User information
        cast(contents->>'$.user.id' as bigint) as author_id,
        contents->>'$.user.login' as author_login,
        contents->>'$.user.type' as author_type,
        
        -- Timestamps
        cast(contents->>'$.created_at' as timestamp) as created_at,
        cast(contents->>'$.updated_at' as timestamp) as updated_at,
        cast(contents->>'$.closed_at' as timestamp) as closed_at,
        cast(contents->>'$.merged_at' as timestamp) as merged_at,
        
        -- Merge information
        contents->>'$.merge_commit_sha' as merge_commit_sha,
        (contents->>'$.merged')::boolean as is_merged,
        
        -- Activity metrics
        cast(contents->>'$.comments' as integer) as comment_count,
        cast(contents->>'$.review_comments' as integer) as review_comment_count,
        cast(contents->>'$.commits' as integer) as commit_count,
        cast(contents->>'$.additions' as integer) as additions,
        cast(contents->>'$.deletions' as integer) as deletions,
        cast(contents->>'$.changed_files' as integer) as changed_files,
        
        -- Links
        contents->>'$.html_url' as html_url,
        
        -- Base and Head branches
        contents->>'$.base.ref' as base_branch,
        contents->>'$.head.ref' as head_branch,
        
        -- Metadata
        _extracted_at
        
    from source
)

select * from parsed