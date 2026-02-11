{{
    config(
        materialized='table'
    )
}}

with pr_base as (
    select
        pr.pr_id,
        pr.pr_number,
        pr.created_at,
        pr.merged_at,
        pr.is_merged,
        pr.author_id,
        pr.comment_count,
        pr.review_comment_count,
        pr.commit_count,
        c.author_login,
        c.user_type
    from {{ ref('fact_pull_requests') }} pr
    left join {{ ref('dim_contributors') }} c
        on pr.author_id = c.author_id
    where pr.is_merged = true
        and pr.is_draft = false
        and coalesce(c.user_type, 'User') != 'non-user'

),

add_month as (
    select
        *,
      
        date_trunc('month', created_at) as month
    from pr_base
)
, merge_time as (
    select
   
        month as date_month,
        author_login as author_login,
        pr_id,
        comment_count,
        review_comment_count,
        commit_count,
        date_diff('hour', created_at, merged_at) as time_to_merge_hours
        
    from add_month
    group by date_month, 
    author_login, 
    pr_id,
    comment_count,
    review_comment_count,
    commit_count,
    created_at, 
    merged_at
   )
   select 
   date_month,
   author_login,
   avg(time_to_merge_hours) as avg_time_to_merge_hours,
   avg(time_to_merge_hours)/24 as avg_time_to_merge_days,
   count(distinct pr_id) as num_prs,
   sum(comment_count) as num_comments,
   sum(review_comment_count) as num_review_comments,
   sum(comment_count) + sum(review_comment_count) as num_all_comments,
   sum(commit_count) as num_commits,
   (sum(comment_count) + sum(review_comment_count)) / count(distinct pr_id) as comments_per_pr,
   sum(commit_count) / count(distinct pr_id) as commits_per_pr
   from merge_time
   group by date_month, author_login