{{
    config(
        materialized='view'
    )
}}

with source as (
    select * from {{ source('github_raw', 'raw_data_issues') }}
    where payload is not null
),

parsed as (
    select
        -- Primary keys
        cast(payload->>'$.id' as bigint) as issue_id,
        cast(payload->>'$.number' as integer) as issue_number,
        payload->>'$.node_id' as issue_node_id,
        
        -- Issue attributes
        payload->>'$.title' as issue_title,
        payload->>'$.state' as issue_state,
        payload->>'$.state_reason' as state_reason,
        (payload->>'$.locked')::boolean as is_locked,
        
        -- User information
        cast(payload->>'$.user.id' as bigint) as author_id,
        payload->>'$.user.login' as author_login,
        payload->>'$.user.type' as author_type,
        
        -- Assignees
        payload->>'$.assignee.login' as assignee_login,
        
        -- Labels (as JSON array string for now)
        payload->>'$.labels' as labels,
        
        -- Timestamps
        cast(payload->>'$.created_at' as timestamp) as created_at,
        cast(payload->>'$.updated_at' as timestamp) as updated_at,
        cast(payload->>'$.closed_at' as timestamp) as closed_at,
        
        -- Activity metrics
        cast(payload->>'$.comments' as integer) as comment_count,
        
        -- Links
        payload->>'$.html_url' as html_url,
        
        -- Metadata
        _extracted_at
        
    from source
)

select * from parsed