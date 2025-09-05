/*
  stg_etherscan__transactions
  ============================
  Cleans and standardises raw Etherscan transaction data.

  Key transformations:
  - Cast wei → ETH (divide by 1e18)
  - Lowercase addresses for consistent joins
  - Classify transaction type from method_id / function_name
  - Filter out failed transactions
  - Add gas cost in USD (requires token_prices join at mart level)
*/

with source as (

    select * from {{ source('etherscan', 'etherscan_transactions') }}

),

cleaned as (

    select
        -- Identifiers
        tx_hash,
        block_number,
        block_timestamp::timestamp                              as block_timestamp,
        date_trunc('day', block_timestamp)::date               as tx_date,
        date_trunc('week', block_timestamp)::date              as tx_week,

        -- Addresses (standardised to lowercase)
        lower(from_address)                                     as from_address,
        lower(to_address)                                       as to_address,
        lower(nullif(contract_address, ''))                    as contract_address,

        -- Values
        value_wei::numeric(38, 0)                              as value_wei,
        round(value_wei::numeric / 1e18, 8)                    as value_eth,
        gas_used::bigint                                        as gas_used,
        gas_price_wei::numeric                                 as gas_price_wei,
        round((gas_used::numeric * gas_price_wei::numeric) / 1e18, 8) as gas_cost_eth,

        -- Method classification
        lower(method_id)                                        as method_id,
        lower(coalesce(nullif(function_name, ''), 'unknown'))   as function_name,
        case
            when lower(function_name) in ('exactinputsingle', 'exactinput',
                                          'exactoutputsingle', 'exactoutput')
                then 'swap'
            when lower(function_name) in ('supply', 'deposit')
                then 'supply'
            when lower(function_name) = 'borrow'
                then 'borrow'
            when lower(function_name) = 'repay'
                then 'repay'
            when lower(function_name) = 'withdraw'
                then 'withdraw'
            else 'other'
        end                                                     as tx_type,

        -- Protocol metadata
        protocol_name,
        chain,

        -- Pipeline metadata
        _extracted_at

    from source
    where is_error = false  -- Only successful transactions

)

select * from cleaned
