/*
  int_user__first_interaction
  ============================
  Identifies each wallet's FIRST transaction with any tracked DeFi protocol.
  This determines cohort assignment (cohort_week = week of first interaction).

  Business logic:
  - Cohort = year-week of first-ever transaction on any watched protocol
  - acquisition_source = wallet label from Dune (proxy for "how they arrived")
  - One row per wallet_address
*/

with first_txs as (

    select
        from_address                                as wallet_address,
        protocol_name                               as first_protocol,
        tx_type                                     as first_tx_type,
        tx_date                                     as first_interaction_date,
        tx_week                                     as cohort_week,
        row_number() over (
            partition by from_address
            order by block_timestamp asc
        )                                           as rn

    from {{ ref('stg_etherscan__transactions') }}

),

first_interaction as (

    select * from first_txs where rn = 1

),

enriched as (

    select
        fi.wallet_address,
        fi.first_protocol,
        fi.first_tx_type,
        fi.first_interaction_date,
        fi.cohort_week,

        -- Wallet classification from Dune labels
        coalesce(wl.label, 'unknown')               as acquisition_source,
        coalesce(wl.label_type, 'unknown')          as label_type,

        -- Cohort identifier for reporting
        to_char(fi.cohort_week, 'IYYY-IW')          as cohort_id

    from first_interaction fi
    left join {{ ref('stg_dune__wallet_labels') }} wl
        on fi.wallet_address = wl.wallet_address

)

select * from enriched
