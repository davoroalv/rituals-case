{{
    config(
        materialized='table'
    )
}}

with all_activities as (
    select 
        author_id as user_id, 
        author_login as author_login,
        author_type as user_type,
        created_at as activity_at, 
        'pr' as type
    from {{ ref('stg_pull_requests') }}

    union all

    select 
        author_id as user_id, 
        author_login as author_login,
        author_type as user_type,
        created_at as activity_at, 
        'issue' as type 
    from {{ ref('stg_issues') }}

    union all

    select 
        github_author_id as user_id, 
        github_author_login as author_login,
        github_author_type as user_type,
        committer_date as activity_at, 
        'commit' as type
    from {{ ref('stg_commits') }}
),
user_milestones as (
    select
        user_id as author_id,
        author_login,
        case when user_type = 'User' then 'user'
        else 'non-user' end as user_type,
        min(activity_at) as first_activity_at,
        max(activity_at) as last_activity_at,
        min(case when type = 'commit' then activity_at end) as first_commit_at,
        min(case when type = 'issue' then activity_at end) as first_issue_at,
        min(case when type = 'pr' then activity_at end) as first_pr_at
    from all_activities
    where user_id is not null
    group by user_id, author_login, user_type
)
select *
from user_milestones