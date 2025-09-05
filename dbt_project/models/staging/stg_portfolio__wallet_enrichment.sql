with source as (
    select * from {{ source('portfolio', 'wallet_enrichment') }}
),

renamed as (
    select
        wallet_address,
        historical_win_rate,
        realized_profit_usd,

        -- Smart Money Classification
        case 
            when historical_win_rate >= 0.60 then 'Smart Money'
            when historical_win_rate >= 0.40 then 'Average'
            else 'Retail'
        end as smart_money_tier,

        _extracted_at
    from source
)

select * from renamed
