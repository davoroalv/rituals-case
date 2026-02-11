{{
    config(
        materialized='table'
    )
}}

with pr_base as (
    select
        pr.pr_id,
        pr.created_at,
        pr.is_merged,
        pr.additions,
        pr.deletions,
        pr.net_changes,
        pr.total_changes,
        pr.changed_files,
        c.author_login,
        c.user_type
    from {{ ref('fact_pull_requests') }} pr
    left join {{ ref('dim_contributors') }} c
        on pr.author_id = c.author_id
    where pr.is_draft = false
        and coalesce(c.user_type, 'User') != 'non-user'
),

add_time as (
    select
        *,

        date_trunc('week', created_at) as week
    from pr_base
)
		select
        week as date_week,
        author_login as active_developer,
        count(*) as total_prs,
        sum(case when is_merged then 1 else 0 end) as merged_prs,
        sum(additions) as total_additions,
        sum(deletions) as total_deletions,
        sum(total_changes) as total_lines_touched,
        sum(net_changes) as total_net_lines,
        sum(changed_files) as total_changed_files,
        round(avg(total_changes), 2) as avg_lines_per_pr,
        round(sum(total_changes) over (partition by week) / count(distinct author_login) over (partition by week),2) overall_weekly_avg
        
    from add_time
    group by week, author_login, total_changes