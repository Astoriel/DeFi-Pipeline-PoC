/*
  fct_daily_revenue_attribution
  ==============================
  Daily revenue attributed to user cohorts by acquisition source.

  Business question: "Which type of user generates the most protocol revenue?"
  Answers: CAC equivalent, Revenue per cohort, LTV by acquisition channel.

  Grain: date × protocol_name × acquisition_source
*/

with daily_revenue as (

    select * from {{ ref('int_protocol__daily_revenue_usd') }}

),

daily_active_wallets as (

    select
        tx.tx_date                              as date,
        tx.protocol_name,
        fi.acquisition_source,
        count(distinct tx.from_address)         as active_wallets,
        count(tx.tx_hash)                       as total_txs,
        sum(tx.value_eth)                       as total_volume_eth

    from {{ ref('stg_etherscan__transactions') }} tx
    join {{ ref('int_user__first_interaction') }} fi
        on tx.from_address = fi.wallet_address

    group by tx.tx_date, tx.protocol_name, fi.acquisition_source

),

eth_prices as (

    select date, price_usd
    from {{ ref('stg_coingecko__token_prices') }}
    where token_symbol = 'ETH'

),

final as (

    select
        dr.date,
        dr.protocol_name,
        daw.acquisition_source,

        -- Volume and activity
        daw.active_wallets,
        daw.total_txs,
        daw.total_volume_eth,
        round(daw.total_volume_eth * ep.price_usd, 2)  as volume_usd,

        -- Revenue attributed to this cohort's share of activity
        round(
            dr.estimated_revenue_usd
            * daw.active_wallets::numeric
            / nullif(dr.unique_wallets::numeric, 0),
            2
        )                                               as attributed_revenue_usd,

        -- Protocol-level metrics for context
        dr.estimated_revenue_usd                        as protocol_total_revenue_usd,
        dr.tvl_usd,
        dr.eth_price_usd,

        -- Revenue per active wallet (daily LTV proxy)
        round(
            dr.estimated_revenue_usd
            / nullif(dr.unique_wallets::numeric, 0),
            4
        )                                               as revenue_per_wallet_usd

    from daily_revenue dr
    left join daily_active_wallets daw 
        on dr.date = daw.date 
        and dr.protocol_name = daw.protocol_name
    left join eth_prices ep on dr.date = ep.date

)

select * from final
order by date desc, protocol_name, acquisition_source
