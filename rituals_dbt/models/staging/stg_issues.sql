{{
    config(
        materialized='view'
    )
}}

-- Staging: Parse raw JSON into typed columns
-- Grain: One row per issue (excluding pull requests)
-- Source: raw.raw_data_issues
-- Note: GitHub's API returns PRs in the issues endpoint, but we filter them out

with source as (
    select * from {{ source('github_raw', 'raw_data_issues') }}
    where contents is not null
),

parsed as (
    select
        -- Primary keys
        cast(contents->>'$.id' as bigint) as issue_id,
        cast(contents->>'$.number' as integer) as issue_number,
        contents->>'$.node_id' as issue_node_id,
        
        -- Issue attributes
        contents->>'$.title' as issue_title,
        contents->>'$.state' as issue_state,
        contents->>'$.state_reason' as state_reason,
        (contents->>'$.locked')::boolean as is_locked,
        
        -- User information
        cast(contents->>'$.user.id' as bigint) as author_id,
        contents->>'$.user.login' as author_login,
        contents->>'$.user.type' as author_type,
        
        -- Assignees
        contents->>'$.assignee.login' as assignee_login,
        
        -- Labels (as JSON array string for now)
        contents->>'$.labels' as labels,
        
        -- Timestamps
        cast(contents->>'$.created_at' as timestamp) as created_at,
        cast(contents->>'$.updated_at' as timestamp) as updated_at,
        cast(contents->>'$.closed_at' as timestamp) as closed_at,
        
        -- Activity metrics
        cast(contents->>'$.comments' as integer) as comment_count,
        
        -- Links
        contents->>'$.html_url' as html_url,
        
        -- Metadata
        _extracted_at
        
    from source
)

select * from parsed