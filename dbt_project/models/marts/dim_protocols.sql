/*
  dim_protocols
  ==============
  Protocol dimension table.
  Static protocol metadata enriched with latest TVL and activity stats.
*/

with protocol_activity as (

    select
        protocol_name,
        chain,
        min(tx_date)                    as first_activity_date,
        max(tx_date)                    as last_activity_date,
        count(distinct tx_hash)         as total_txs,
        count(distinct from_address)    as total_unique_wallets,
        count(distinct tx_date)         as active_days

    from {{ ref('stg_etherscan__transactions') }}
    group by protocol_name, chain

),

latest_tvl as (

    select distinct on (protocol_slug)
        protocol_slug,
        protocol_name,
        chain,
        tvl_usd as latest_tvl_usd,
        date as tvl_date

    from {{ ref('stg_defillama__protocol_tvl') }}
    order by protocol_slug, date desc

),

-- Static seed data enrichment (from seeds/protocol_metadata.csv)
protocol_meta as (

    select * from {{ ref('protocol_metadata') }}

),

final as (

    select
        pa.protocol_name,
        pa.chain,
        coalesce(pm.category, 'defi')           as category,
        coalesce(pm.launch_year, 2020)          as launch_year,
        coalesce(pm.token_symbol, 'UNKNOWN')    as governance_token,
        coalesce(pm.website, '')                as website,

        -- TVL
        lt.latest_tvl_usd,
        lt.tvl_date,

        -- Activity stats
        pa.first_activity_date,
        pa.last_activity_date,
        pa.total_txs,
        pa.total_unique_wallets,
        pa.active_days

    from protocol_activity pa
    left join latest_tvl lt
        on pa.protocol_name = lt.protocol_name
        and pa.chain = lt.chain
    left join protocol_meta pm
        on pa.protocol_name = pm.protocol_name

)

select * from final
