{{
    config(
        materialized='table'
    )
}}

-- Grain: One row per unique contributor (GitHub user)
-- This dimension captures all unique contributors from PRs, issues, and commits

with pr_users as (
    select distinct
        author_id as user_id,
        author_login as login,
        author_type as user_type
    from {{ ref('stg_pull_requests') }}
    where author_id is not null
),

issue_users as (
    select distinct
        author_id as user_id,
        author_login as login,
        author_type as user_type
    from {{ ref('stg_issues') }}
    where author_id is not null
),

commit_users as (
    select distinct
        github_author_id as user_id,
        github_author_login as login,
        github_author_type as user_type
    from {{ ref('stg_commits') }}
    where github_author_id is not null
),

all_users as (
    select * from pr_users
    union
    select * from issue_users
    union
    select * from commit_users
),

-- Get activity counts per user
user_activity as (
    select
        user_id,
        login,
        user_type,
        
        -- PR activity
        (select count(*) from {{ ref('stg_pull_requests') }} where author_id = all_users.user_id) as total_prs,
        (select count(*) from {{ ref('stg_pull_requests') }} where author_id = all_users.user_id and is_merged) as merged_prs,
        
        -- Issue activity
        (select count(*) from {{ ref('stg_issues') }} where author_id = all_users.user_id) as total_issues,
        (select count(*) from {{ ref('stg_issues') }} where author_id = all_users.user_id and issue_state = 'closed') as closed_issues,
        
        -- Commit activity
        (select count(*) from {{ ref('stg_commits') }} where github_author_id = all_users.user_id) as total_commits,
        
        -- First and last activity
        (select min(created_at) from {{ ref('stg_pull_requests') }} where author_id = all_users.user_id) as first_pr_date,
        (select max(created_at) from {{ ref('stg_pull_requests') }} where author_id = all_users.user_id) as last_pr_date,
        
        current_timestamp as _dbt_created_at
        
    from all_users
),

final as (
    select
        user_id,
        login,
        user_type,
        
        -- Classify user based on type and activity
        case
            when user_type = 'Bot' then 'Bot'
            when total_prs >= 10 or total_commits >= 50 then 'Core Contributor'
            when total_prs >= 1 or total_commits >= 5 then 'Regular Contributor'
            else 'Casual Contributor'
        end as contributor_segment,
        
        total_prs,
        merged_prs,
        total_issues,
        closed_issues,
        total_commits,
        
        first_pr_date,
        last_pr_date,
        
        _dbt_created_at
        
    from user_activity
)

select * from final