{{
    config(
        materialized='table'
    )
}}

-- Grain: One row per pull request
-- This fact table captures the lifecycle and metrics of each PR

with prs as (
    select * from {{ ref('stg_pull_requests') }}
),

final as (
    select
        -- Primary keys
        pr_id,
        pr_number,
        
        -- Foreign keys
        author_id as contributor_id,
        
        -- PR attributes
        pr_title,
        pr_state,
        is_draft,
        is_merged,
        is_locked,
        
        -- Timestamps
        created_at,
        updated_at,
        closed_at,
        merged_at,
        
        -- Calculated time metrics (in hours)
        case
            when merged_at is not null and created_at is not null
            then date_diff('hour', created_at, merged_at)
        end as time_to_merge_hours,
        
        case
            when closed_at is not null and created_at is not null
            then date_diff('hour', created_at, closed_at)
        end as time_to_close_hours,
        
        case
            when updated_at is not null and created_at is not null
            then date_diff('hour', created_at, updated_at)
        end as time_to_last_update_hours,
        
        -- Activity metrics
        comment_count,
        review_comment_count,
        commit_count,
        
        -- Code change metrics
        additions,
        deletions,
        additions + deletions as total_changes,
        changed_files,
        
        -- Branches
        base_branch,
        head_branch,
        
        -- Size categorization
        case
            when additions + deletions < 10 then 'XS'
            when additions + deletions < 50 then 'S'
            when additions + deletions < 200 then 'M'
            when additions + deletions < 500 then 'L'
            else 'XL'
        end as pr_size,
        
        -- Status flags
        case
            when pr_state = 'closed' and is_merged then 'Merged'
            when pr_state = 'closed' and not is_merged then 'Closed without merge'
            when pr_state = 'open' and is_draft then 'Draft'
            when pr_state = 'open' then 'Open'
        end as pr_status,
        
        -- Links
        html_url,
        
        -- Metadata
        _extracted_at,
        current_timestamp as _dbt_created_at
        
    from prs
    -- Defensive: exclude potential duplicates
    qualify row_number() over (partition by pr_id order by _extracted_at desc) = 1
)

select * from final