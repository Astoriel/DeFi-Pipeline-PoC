/*
  stg_defillama__protocol_tvl
  ============================
  Normalised daily TVL snapshots for Uniswap V3 and Aave V3.
*/

with source as (

    select * from {{ source('defillama', 'defillama_tvl') }}

),

cleaned as (

    select
        protocol_slug,
        protocol_name,
        chain,
        "date"::date                            as date,
        coalesce(tvl_usd, 0)::numeric(20, 2)  as tvl_usd,
        _extracted_at

    from source
    where tvl_usd is not null
      and tvl_usd > 0

)

select * from cleaned
