/*
  int_user__sybil_scoring
  =======================
  Calculates math-based sybil/bot scores based on standard deviations of time and value.
*/

with txs as (

    select
        from_address as wallet_address,
        extract(epoch from block_timestamp) as tx_epoch,
        value_eth
    from {{ ref('stg_etherscan__transactions') }}

),

wallet_stats as (

    select
        wallet_address,
        count(*) as total_txs,
        stddev_samp(tx_epoch) as time_variance_sec,
        stddev_samp(value_eth) as value_variance_eth
    from txs
    group by wallet_address

),

final as (

    select
        wallet_address,
        total_txs,
        coalesce(time_variance_sec, 0) as time_variance_sec,
        coalesce(value_variance_eth, 0) as value_variance_eth,
        
        -- Algorithmic Bot Classification
        case
            -- High frequency, very low variance in timing (scripted)
            when total_txs > 10 and coalesce(time_variance_sec, 0) < 3600 then 1 
            -- Exact same value sent repeatedly (airdrop farming)
            when total_txs > 5 and coalesce(value_variance_eth, 0) = 0 then 1
            else 0
        end as is_bot

    from wallet_stats

)

select * from final
