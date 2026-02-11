{{
    config(
        materialized='table'
    )
}}

with commit_base as (
    select
        c.commit_sha,
        c.commit_author_date,
        c.github_author_id as author_id,
        c.additions,
        c.deletions,
        c.total_changes,
        c.additions - c.deletions as net_changes,
        case 
            when c.github_author_id = c.github_committer_id then 'local'
            else 'web-based' 
        end as commit_type,
        contrib.author_login,
        contrib.user_type
    from {{ ref('stg_commits') }} c
    left join {{ ref('dim_contributors') }} contrib
        on c.github_author_id = contrib.author_id
    where coalesce(contrib.user_type, 'User') != 'non-user'
),

add_time as (
    select
        *,
        date_trunc('week', commit_author_date) as week
    from commit_base
)

select
    week as date_week,
    author_login as active_developer,
    count(*) as total_commits,
    count(distinct case when commit_type = 'local' then commit_sha end) as local_commits,
    count(distinct case when commit_type = 'web-based' then commit_sha end) as web_commits,
    sum(additions) as total_additions,
    sum(deletions) as total_deletions,
    sum(total_changes) as total_lines_touched,
    sum(net_changes) as total_net_lines,
    round(avg(total_changes), 2) as avg_lines_per_commit
from add_time
group by week, author_login
