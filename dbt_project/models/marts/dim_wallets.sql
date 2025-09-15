/*
  dim_wallets
  ============
  Wallet master dimension table.
  One row per wallet_address with aggregated lifetime stats.

  Columns business stakeholders care about:
  - Total revenue generated
  - Acquisition source (behavioral label)
  - Segment: "whale" (top 1%), "power user" (top 10%), "retail"
*/

with wallet_txs as (

    select
        from_address                as wallet_address,
        min(block_timestamp)        as first_seen_at,
        max(block_timestamp)        as last_seen_at,
        count(distinct tx_hash)     as total_txs,
        count(distinct protocol_name) as protocols_used,
        sum(value_eth)              as total_volume_eth,
        sum(gas_cost_eth)           as total_gas_spent_eth,
        count(distinct tx_date)     as active_days

    from {{ ref('stg_etherscan__transactions') }}
    group by from_address

),

cohort_info as (

    select
        wallet_address,
        first_protocol,
        first_tx_type,
        first_interaction_date,
        cohort_week,
        cohort_id,
        acquisition_source

    from {{ ref('int_user__first_interaction') }}

),

eth_prices_latest as (

    select price_usd
    from {{ ref('stg_coingecko__token_prices') }}
    where token_symbol = 'ETH'
    order by date desc
    limit 1

),

wallet_percentiles as (

    select
        wallet_address,
        total_volume_eth,
        ntile(100) over (order by total_volume_eth desc) as volume_percentile

    from wallet_txs

),

sybil as (

    select wallet_address, is_bot
    from {{ ref('int_user__sybil_scoring') }}

),

cross_chain as (

    select wallet_address, raw_nomad_score as nomad_score
    from {{ ref('stg_lifi__cross_chain_activity') }}

),

portfolio as (

    select wallet_address, historical_win_rate, smart_money_tier
    from {{ ref('stg_portfolio__wallet_enrichment') }}

),

final as (

    select
        wt.wallet_address,

        -- Acquisition
        ci.acquisition_source,
        ci.first_protocol,
        ci.cohort_id,
        ci.cohort_week,
        ci.first_interaction_date,

        -- Activity stats
        wt.first_seen_at,
        wt.last_seen_at,
        (wt.last_seen_at::date - wt.first_seen_at::date)    as lifetime_days,
        wt.active_days,
        wt.total_txs,
        wt.protocols_used,

        -- Volume (ETH and USD approximation)
        round(wt.total_volume_eth, 6)                          as total_volume_eth,
        round(wt.total_volume_eth * ep.price_usd, 2)           as total_volume_usd,
        round(wt.total_gas_spent_eth, 6)                       as total_gas_spent_eth,

        -- Segmentation
        case
            when wp.volume_percentile <= 1  then 'whale'
            when wp.volume_percentile <= 10 then 'power_user'
            when wp.volume_percentile <= 50 then 'active_user'
            else 'retail'
        end                                                     as volume_segment,

        -- Advanced Analytics (BADE)
        coalesce(s.is_bot, 0)                                   as is_bot,
        coalesce(cc.nomad_score, 0.0)                           as nomad_score,
        p.historical_win_rate,
        coalesce(p.smart_money_tier, 'Retail')                  as smart_money_tier

    from wallet_txs wt
    left join cohort_info ci using (wallet_address)
    left join wallet_percentiles wp using (wallet_address)
    cross join eth_prices_latest ep
    left join sybil s using (wallet_address)
    left join cross_chain cc using (wallet_address)
    left join portfolio p using (wallet_address)

)

select * from final
