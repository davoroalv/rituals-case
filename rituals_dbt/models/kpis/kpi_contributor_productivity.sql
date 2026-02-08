{{
    config(
        materialized='table'
    )
}}

-- KPI 3: Contributor Productivity & Code Impact
-- Definition: Commit velocity, code churn, and contribution patterns by contributor segment
-- Grain: Aggregated metrics by contributor segment
-- Business Value: Identifies high-impact contributors and optimal contribution patterns
-- Excludes: Bot activity

with contributor_commits as (
    select
        fc.contributor_id,
        fc.commit_sha,
        fc.commit_author_date,
        fc.additions,
        fc.deletions,
        fc.total_changes,
        dc.contributor_segment,
        dc.user_type
    from {{ ref('fact_commits') }} fc
    left join {{ ref('dim_contributors') }} dc
        on fc.contributor_id = dc.user_id
    where dc.user_type != 'Bot'
),

contributor_prs as (
    select
        fpr.contributor_id,
        fpr.pr_id,
        fpr.is_merged,
        fpr.additions,
        fpr.deletions,
        fpr.total_changes,
        dc.contributor_segment
    from {{ ref('fact_pull_requests') }} fpr
    left join {{ ref('dim_contributors') }} dc
        on fpr.contributor_id = dc.user_id
    where dc.user_type != 'Bot'
        and fpr.is_draft = false
),

-- Aggregate by contributor segment

by_segment as (
    select
        cc.contributor_segment as segment,
        -- Commit metrics
        count(distinct cc.contributor_id) as active_contributors,
        count(cc.commit_sha) as total_commits,
        round(avg(cc.total_changes), 2) as avg_changes_per_commit,
        sum(cc.additions) as total_additions,
        sum(cc.deletions) as total_deletions,
        sum(cc.total_changes) as total_code_changes,
        
        -- PR metrics
        count(cp.pr_id) as total_prs,
        count(case when cp.is_merged then 1 end) as merged_prs,
        round(100.0 * count(case when cp.is_merged then 1 end) / nullif(count(cp.pr_id), 0), 2) as merge_rate_pct,
        
        -- Productivity metrics
        round(cast(count(cc.commit_sha) as double) / nullif(count(distinct cc.contributor_id), 0), 2) as avg_commits_per_contributor,
        round(cast(sum(cc.total_changes) as double) / nullif(count(distinct cc.contributor_id), 0), 2) as avg_changes_per_contributor,
        round(cast(count(cp.pr_id) as double) / nullif(count(distinct cp.contributor_id), 0), 2) as avg_prs_per_contributor
        
    from contributor_commits cc
    left join contributor_prs cp
        on cc.contributor_id = cp.contributor_id
    group by cc.contributor_segment
),

-- Overall metrics
overall as (
    select
        'Overall' as segment,
        count(distinct cc.contributor_id) as active_contributors,
        count(cc.commit_sha) as total_commits,
        round(avg(cc.total_changes), 2) as avg_changes_per_commit,
        sum(cc.additions) as total_additions,
        sum(cc.deletions) as total_deletions,
        sum(cc.total_changes) as total_code_changes,
        count(cp.pr_id) as total_prs,
        count(case when cp.is_merged then 1 end) as merged_prs,
        round(100.0 * count(case when cp.is_merged then 1 end) / nullif(count(cp.pr_id), 0), 2) as merge_rate_pct,
        round(cast(count(cc.commit_sha) as double) / nullif(count(distinct cc.contributor_id), 0), 2) as avg_commits_per_contributor,
        round(cast(sum(cc.total_changes) as double) / nullif(count(distinct cc.contributor_id), 0), 2) as avg_changes_per_contributor,
        round(cast(count(cp.pr_id) as double) / nullif(count(distinct cp.contributor_id), 0), 2) as avg_prs_per_contributor
    from contributor_commits cc
    left join contributor_prs cp
        on cc.contributor_id = cp.contributor_id
),

final as (
    select * from overall
    union all
    select * from by_segment
)

select
    segment,
    active_contributors,
    total_commits,
    total_prs,
    merged_prs,
    merge_rate_pct,
    avg_commits_per_contributor,
    avg_prs_per_contributor,
    avg_changes_per_commit,
    avg_changes_per_contributor,
    total_additions,
    total_deletions,
    total_code_changes,
    current_timestamp as _calculated_at
from final
order by
    case segment
        when 'Overall' then 1
        when 'Core Contributor' then 2
        when 'Regular Contributor' then 3
        when 'Casual Contributor' then 4
    end