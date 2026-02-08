{{
    config(
        materialized='view'
    )
}}

with source as (
    select * from {{ source('github_raw', 'raw_data_pull_requests') }}
    where payload is not null
),

parsed as (
    select
        -- Primary keys
        cast(payload->>'$.id' as bigint) as pr_id,
        cast(payload->>'$.number' as integer) as pr_number,
        payload->>'$.node_id' as pr_node_id,
        
        -- PR attributes
        payload->>'$.title' as pr_title,
        payload->>'$.state' as pr_state,
        (payload->>'$.draft')::boolean as is_draft,
        (payload->>'$.locked')::boolean as is_locked,
        
        -- User information
        cast(payload->>'$.user.id' as bigint) as author_id,
        payload->>'$.user.login' as author_login,
        payload->>'$.user.type' as author_type,
        
        -- Timestamps
        cast(payload->>'$.created_at' as timestamp) as created_at,
        cast(payload->>'$.updated_at' as timestamp) as updated_at,
        cast(payload->>'$.closed_at' as timestamp) as closed_at,
        cast(payload->>'$.merged_at' as timestamp) as merged_at,
        
        -- Merge information
        payload->>'$.merge_commit_sha' as merge_commit_sha,
        (payload->>'$.merged')::boolean as is_merged,
        
        -- Activity metrics
        cast(payload->>'$.comments' as integer) as comment_count,
        cast(payload->>'$.review_comments' as integer) as review_comment_count,
        cast(payload->>'$.commits' as integer) as commit_count,
        cast(payload->>'$.additions' as integer) as additions,
        cast(payload->>'$.deletions' as integer) as deletions,
        cast(payload->>'$.changed_files' as integer) as changed_files,
        
        -- Links
        payload->>'$.html_url' as html_url,
        
        -- Base and Head branches
        payload->>'$.base.ref' as base_branch,
        payload->>'$.head.ref' as head_branch,
        
        -- Metadata
        _extracted_at
        
    from source
)

select * from parsed