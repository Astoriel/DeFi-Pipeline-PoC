/*
  fct_cohort_retention
  =====================
  THE CORE BUSINESS TABLE.

  Answers: "What % of wallets from cohort X are still active N weeks later?"
  Broken down by acquisition_source (airdrop_hunter, governance_voter, etc.)

  Grain: cohort_id × weeks_since_first × acquisition_source
  Used for: Retention heatmap dashboard
*/

with activity as (

    select * from {{ ref('int_user__activity_periods') }}

),

cohort_sizes as (

    -- Total wallets in each cohort, by acquisition source
    select
        cohort_id,
        cohort_week,
        acquisition_source,
        count(distinct wallet_address)  as cohort_size

    from {{ ref('int_user__first_interaction') }}
    group by cohort_id, cohort_week, acquisition_source

),

retention_agg as (

    select
        a.cohort_id,
        a.cohort_week,
        a.acquisition_source,
        a.weeks_since_first,

        count(distinct a.wallet_address)        as total_wallets_in_period,
        sum(a.is_active)                        as active_wallets,
        avg(a.weekly_tx_count)                  as avg_weekly_txs,
        sum(a.week_volume_eth)                  as total_volume_eth

    from activity a
    group by
        a.cohort_id, a.cohort_week,
        a.acquisition_source, a.weeks_since_first

),

final as (

    select
        ra.cohort_id,
        ra.cohort_week,
        ra.acquisition_source,
        ra.weeks_since_first,

        cs.cohort_size,
        ra.active_wallets,
        ra.total_wallets_in_period,

        -- KEY METRIC: Retention Rate
        round(
            ra.active_wallets::numeric / nullif(cs.cohort_size::numeric, 0),
            4
        )                                       as retention_rate,

        -- Human-readable retention %
        round(
            ra.active_wallets::numeric / nullif(cs.cohort_size::numeric, 0) * 100,
            2
        )                                       as retention_pct,

        ra.avg_weekly_txs,
        ra.total_volume_eth,

        -- Classify week period
        case
            when ra.weeks_since_first = 0  then 'Week 0 (Activation)'
            when ra.weeks_since_first <= 4  then 'Month 1'
            when ra.weeks_since_first <= 12 then 'Quarter 1'
            else 'Long Term'
        end                                     as retention_period

    from retention_agg ra
    join cohort_sizes cs
        on ra.cohort_id = cs.cohort_id
        and ra.acquisition_source = cs.acquisition_source

    where ra.weeks_since_first >= 0

)

select * from final
order by cohort_week, weeks_since_first, acquisition_source
