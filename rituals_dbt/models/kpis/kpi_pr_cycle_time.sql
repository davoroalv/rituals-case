{{
    config(
        materialized='table'
    )
}}

-- KPI 1: Pull Request Cycle Time
-- Definition: Time from PR creation to merge (for merged PRs only)
-- Grain: Aggregated metrics by time period and PR size
-- Business Value: Identifies bottlenecks in code review and merge processes
-- Excludes: Draft PRs, bot activity, and unmerged PRs

with pr_metrics as (
    select
        pr_id,
        pr_number,
        created_at,
        merged_at,
        time_to_merge_hours,
        pr_size,
        total_changes,
        review_comment_count,
        is_draft,
        contributor_id
    from {{ ref('fact_pull_requests') }}
    where is_merged = true
        and is_draft = false  -- Exclude drafts
        and time_to_merge_hours is not null
),

-- Filter out bot activity
pr_metrics_clean as (
    select pm.*
    from pr_metrics pm
    left join {{ ref('dim_contributors') }} dc
        on pm.contributor_id = dc.user_id
    where dc.user_type != 'Bot'
),

-- Calculate overall statistics
overall_stats as (
    select
        'Overall' as segment,
        count(*) as total_prs,
        round(avg(time_to_merge_hours), 2) as avg_cycle_time_hours,
        round(avg(time_to_merge_hours) / 24, 2) as avg_cycle_time_days,
        round(percentile_cont(0.5) within group (order by time_to_merge_hours), 2) as median_cycle_time_hours,
        round(percentile_cont(0.75) within group (order by time_to_merge_hours), 2) as p75_cycle_time_hours,
        round(percentile_cont(0.90) within group (order by time_to_merge_hours), 2) as p90_cycle_time_hours,
        min(time_to_merge_hours) as min_cycle_time_hours,
        max(time_to_merge_hours) as max_cycle_time_hours
    from pr_metrics_clean
),

-- By PR size
by_size as (
    select
        pr_size as segment,
        count(*) as total_prs,
        round(avg(time_to_merge_hours), 2) as avg_cycle_time_hours,
        round(avg(time_to_merge_hours) / 24, 2) as avg_cycle_time_days,
        round(percentile_cont(0.5) within group (order by time_to_merge_hours), 2) as median_cycle_time_hours,
        round(percentile_cont(0.75) within group (order by time_to_merge_hours), 2) as p75_cycle_time_hours,
        round(percentile_cont(0.90) within group (order by time_to_merge_hours), 2) as p90_cycle_time_hours,
        min(time_to_merge_hours) as min_cycle_time_hours,
        max(time_to_merge_hours) as max_cycle_time_hours
    from pr_metrics_clean
    group by pr_size
),

-- Combine all segments
final as (
    select * from overall_stats
    union all
    select * from by_size
)

select
    segment,
    total_prs,
    avg_cycle_time_hours,
    avg_cycle_time_days,
    median_cycle_time_hours,
    p75_cycle_time_hours,
    p90_cycle_time_hours,
    min_cycle_time_hours,
    max_cycle_time_hours,
    current_timestamp as _calculated_at
from final
order by
    case segment
        when 'Overall' then 1
        when 'XS' then 2
        when 'S' then 3
        when 'M' then 4
        when 'L' then 5
        when 'XL' then 6
    end