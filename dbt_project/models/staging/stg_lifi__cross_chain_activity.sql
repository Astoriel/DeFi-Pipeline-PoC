with source as (
    select * from {{ source('lifi', 'cross_chain_activity') }}
),

renamed as (
    select
        wallet_address,
        distinct_chains_used,
        total_bridging_volume_usd,
        last_bridge_date,

        -- Derived Nomad Score
        case 
            when distinct_chains_used = 1 then 0.0
            when distinct_chains_used > 1 then 
                round(distinct_chains_used::numeric / nullif(total_bridging_volume_usd::numeric, 0), 6)
            else 0.0
        end as raw_nomad_score,
        
        _extracted_at
    from source
)

select * from renamed
