/*
  stg_coingecko__token_prices
  ============================
  Daily token prices from CoinGecko.
  Forward-fills any gaps (weekends have same data as Friday on CoinGecko).
*/

with source as (

    select * from {{ source('coingecko', 'token_prices') }}

),

cleaned as (

    select
        token_id,
        upper(token_symbol)                             as token_symbol,
        "date"::date                                    as date,
        price_usd::numeric(20, 8)                       as price_usd,
        coalesce(market_cap_usd, 0)::numeric(20, 2)    as market_cap_usd,
        coalesce(volume_24h_usd, 0)::numeric(20, 2)    as volume_24h_usd,
        _extracted_at

    from source
    where price_usd is not null
      and price_usd > 0

)

select * from cleaned
