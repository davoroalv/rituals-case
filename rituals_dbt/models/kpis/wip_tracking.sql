{{
    config(
        materialized='table'
    )
}}

with open_prs as (
    select
        pr_id,
        pr_number,
        pr_title,
        pr_state,
        is_draft,
        created_at,
        updated_at,
        author_id,
        additions,        
        deletions,    
        total_changes     
    from {{ ref('fact_pull_requests') }}
    where pr_state = 'open'
        and is_draft = false
),

with_contributors as (
    select
        pr.*,
        c.author_login,
        c.user_type
    from open_prs pr
    left join {{ ref('dim_contributors') }} c
        on pr.author_id = c.author_id
    where coalesce(c.user_type, 'User') != 'non-user'
),

add_week as (
    select
        *,
        date_trunc('week', created_at) as week_opened
    from with_contributors
),

last_update as (      
    select *,
        date_diff('hour', created_at, updated_at) as hours_last_update,
        case
            when date_diff('hour', created_at, updated_at) < 24 then 'In Last Day'
            when date_diff('hour', created_at, updated_at) < 168 then 'In Last Week'
            when date_diff('hour', created_at, updated_at) < 720 then 'In Last Month'
            else 'Over Month'
        end as staleness
    from add_week
)

select 
    week_opened,
    author_login,
    staleness,
    count(distinct pr_id) as num_prs,
    avg(hours_last_update) as avg_hours_since_last_update,
    avg(hours_last_update)/24 as avg_days_since_last_update,
    
    sum(additions) as total_additions,
    sum(deletions) as total_deletions,
    sum(total_changes) as total_lines_changed,
    round(avg(additions), 2) as avg_additions,
    round(avg(deletions), 2) as avg_deletions,
    round(avg(total_changes), 2) as avg_total_changes,
    
    count(case when total_changes < 50 then 1 end) as small_prs,
    count(case when total_changes between 50 and 200 then 1 end) as medium_prs,
    count(case when total_changes > 200 then 1 end) as large_prs
    
from last_update
group by week_opened, author_login, staleness