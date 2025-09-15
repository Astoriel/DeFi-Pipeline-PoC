/*
  int_user__activity_periods
  ============================
  Determines whether each wallet was ACTIVE or INACTIVE in each calendar week.
  Used as input for the cohort retention fact table.

  Active = made at least 1 transaction in that week.
  Inactive = no transactions in that week.

  Uses a "spine" approach:
  1. Generate all weeks for each wallet from first_interaction → today
  2. Join actual activity
  3. Calculate weeks_since_first (cohort offset)
*/

with user_weekly_activity as (

    select
        from_address                        as wallet_address,
        tx_week,
        count(distinct tx_hash)             as weekly_tx_count,
        count(distinct protocol_name)       as protocols_used,
        sum(value_eth)                      as week_volume_eth

    from {{ ref('stg_etherscan__transactions') }}
    group by from_address, tx_week

),

cohort_info as (

    select
        wallet_address,
        cohort_week,
        cohort_id,
        acquisition_source

    from {{ ref('int_user__first_interaction') }}

),

-- Generate a spine of (wallet, week) combinations
-- from their first week up to the most recent week with data
date_spine as (

    select
        ci.wallet_address,
        ci.cohort_week,
        ci.cohort_id,
        ci.acquisition_source,
        weeks.week_start

    from cohort_info ci
    cross join (
        -- All weeks since earliest cohort
        select distinct tx_week as week_start
        from {{ ref('stg_etherscan__transactions') }}
    ) weeks
    where weeks.week_start >= ci.cohort_week

),

activity_flags as (

    select
        ds.wallet_address,
        ds.cohort_week,
        ds.cohort_id,
        ds.acquisition_source,
        ds.week_start,

        -- How many weeks after first interaction is this week?
        (ds.week_start - ds.cohort_week) / 7 as weeks_since_first,

        -- Was wallet active this week?
        case when uwa.weekly_tx_count is not null then 1 else 0
        end                                 as is_active,

        coalesce(uwa.weekly_tx_count, 0)   as weekly_tx_count,
        coalesce(uwa.week_volume_eth, 0)   as week_volume_eth

    from date_spine ds
    left join user_weekly_activity uwa
        on ds.wallet_address = uwa.wallet_address
        and ds.week_start = uwa.tx_week

)

select * from activity_flags
