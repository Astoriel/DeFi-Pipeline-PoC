/*
  stg_dune__wallet_labels
  ========================
  Cleaned wallet classification from Dune Analytics.
  Labels each wallet by behavioural archetype.
*/

with source as (

    select * from {{ source('dune', 'dune_wallet_labels') }}

),

cleaned as (

    select
        lower(wallet_address)           as wallet_address,
        coalesce(label, 'unknown')      as label,
        coalesce(label_type, 'unknown') as label_type,
        project,
        first_activity_date::date       as first_activity_date,
        coalesce(total_txs, 0)          as total_txs,
        _extracted_at

    from source
    where wallet_address is not null

)

select * from cleaned
