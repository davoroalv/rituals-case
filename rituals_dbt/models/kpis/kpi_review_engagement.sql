{{
    config(
        materialized='table'
    )
}}

-- KPI 2: Code Review Engagement
-- Definition: Ratio of PRs with review comments and average review comments per PR
-- Grain: Aggregated metrics by contributor segment
-- Business Value: Measures collaboration quality and thoroughness of code review
-- Excludes: Draft PRs and bot activity

with pr_reviews as (
    select
        pr_id,
        pr_number,
        contributor_id,
        review_comment_count,
        is_draft,
        is_merged,
        pr_size
    from {{ ref('fact_pull_requests') }}
    where is_draft = false
),

-- Filter out bot activity
pr_reviews_clean as (
    select pr.*
    from pr_reviews pr
    left join {{ ref('dim_contributors') }} dc
        on pr.contributor_id = dc.user_id
    where dc.user_type != 'Bot'
),

-- Overall metrics
overall_stats as (
    select
        'Overall' as segment,
        count(*) as total_prs,
        count(case when review_comment_count > 0 then 1 end) as prs_with_reviews,
        round(100.0 * count(case when review_comment_count > 0 then 1 end) / count(*), 2) as review_rate_pct,
        round(avg(review_comment_count), 2) as avg_review_comments,
        round(percentile_cont(0.5) within group (order by review_comment_count), 2) as median_review_comments,
        sum(review_comment_count) as total_review_comments
    from pr_reviews_clean
),

-- By PR size
by_size as (
    select
        pr_size as segment,
        count(*) as total_prs,
        count(case when review_comment_count > 0 then 1 end) as prs_with_reviews,
        round(100.0 * count(case when review_comment_count > 0 then 1 end) / count(*), 2) as review_rate_pct,
        round(avg(review_comment_count), 2) as avg_review_comments,
        round(percentile_cont(0.5) within group (order by review_comment_count), 2) as median_review_comments,
        sum(review_comment_count) as total_review_comments
    from pr_reviews_clean
    group by pr_size
),

-- By merge status
by_merge_status as (
    select
        case when is_merged then 'Merged' else 'Not Merged' end as segment,
        count(*) as total_prs,
        count(case when review_comment_count > 0 then 1 end) as prs_with_reviews,
        round(100.0 * count(case when review_comment_count > 0 then 1 end) / count(*), 2) as review_rate_pct,
        round(avg(review_comment_count), 2) as avg_review_comments,
        round(percentile_cont(0.5) within group (order by review_comment_count), 2) as median_review_comments,
        sum(review_comment_count) as total_review_comments
    from pr_reviews_clean
    group by is_merged
),

final as (
    select * from overall_stats
    union all
    select * from by_size
    union all
    select * from by_merge_status
)

select
    segment,
    total_prs,
    prs_with_reviews,
    review_rate_pct,
    avg_review_comments,
    median_review_comments,
    total_review_comments,
    current_timestamp as _calculated_at
from final
order by
    case
        when segment = 'Overall' then 1
        when segment = 'XS' then 2
        when segment = 'S' then 3
        when segment = 'M' then 4
        when segment = 'L' then 5
        when segment = 'XL' then 6
        when segment = 'Merged' then 7
        when segment = 'Not Merged' then 8
    end