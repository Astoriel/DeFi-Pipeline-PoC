/*
  int_protocol__daily_revenue_usd
  =================================
  Calculates daily protocol revenue in USD by combining:
  - Etherscan transaction gas fees (proxy for Uniswap fee revenue)
  - DeFiLlama official fees data (where available)
  - ETH/token prices from CoinGecko

  Business metric: "How much revenue does each protocol generate per day?"

  Note: For Uniswap V3, fee revenue = 0.05%/0.30%/1.0% per swap on TVL
        We approximate this from tx volume × avg fee tier.
        DeFiLlama provides exact numbers where available.
*/

with daily_txs as (

    select
        tx_date                             as date,
        protocol_name,

        -- Transaction volume
        count(distinct tx_hash)             as tx_count,
        count(distinct from_address)        as unique_wallets,
        sum(value_eth)                      as total_volume_eth,

        -- Gas costs (proxy for protocol interaction cost)
        sum(gas_cost_eth)                   as total_gas_eth

    from {{ ref('stg_etherscan__transactions') }}
    group by tx_date, protocol_name

),

eth_prices as (

    select
        date,
        price_usd
    from {{ ref('stg_coingecko__token_prices') }}
    where token_symbol = 'ETH'

),

tvl_data as (

    select
        date,
        protocol_slug,
        protocol_name,
        tvl_usd

    from {{ ref('stg_defillama__protocol_tvl') }}

),

combined as (

    select
        dt.date,
        dt.protocol_name,
        dt.tx_count,
        dt.unique_wallets,
        dt.total_volume_eth,
        round(dt.total_volume_eth * ep.price_usd, 2)   as volume_usd,

        -- Estimated fee revenue:
        -- Uniswap V3 avg fee = ~0.30% of volume
        -- Aave = interest spread, approximated from TVL
        case
            when dt.protocol_name ilike '%uniswap%'
                then round(dt.total_volume_eth * ep.price_usd * 0.003, 2)
            when dt.protocol_name ilike '%aave%'
                then round(tvl.tvl_usd * 0.0003, 2)  -- ~0.03% daily approximation
            else 0
        end                                             as estimated_revenue_usd,

        tvl.tvl_usd,
        ep.price_usd                                   as eth_price_usd

    from daily_txs dt
    left join eth_prices ep on dt.date = ep.date
    left join tvl_data tvl
        on dt.date = tvl.date
        and dt.protocol_name = tvl.protocol_name

)

select * from combined
