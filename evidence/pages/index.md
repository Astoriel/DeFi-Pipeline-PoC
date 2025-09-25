---
title: DeFi Protocol Revenue & User Attribution
---

# 📊 Overall Business Health

This dashboard presents the core metrics of the DeFi Revenue Attribution pipeline, focusing on user cohorts, protocol revenue, and acquisition source performance.

```sql daily_revenue
select
    date,
    sum(attributed_revenue_usd) as total_revenue
from defi_pipeline.fct_daily_revenue_attribution
group by date
order by date desc
limit 30
```

<AreaChart 
    data={daily_revenue}
    x=date 
    y=total_revenue
    title="Daily Revenue (Last 30 Days)"
    yAxisTitle="Revenue (USD)"
/>

---

## 👥 Cohort Retention

Retention is measured by the percentage of users still active N weeks after their first interaction. The `airdrop_hunter` cohort typically shows the lowest retention.

```sql cohort_retention
select
    cohort_week,
    acquisition_source,
    retention_rate,
    weeks_since_first as retention_week
from defi_pipeline.fct_cohort_retention
order by cohort_week desc, retention_week asc
```

<LineChart 
    data={cohort_retention}
    x=retention_week
    y=retention_rate
    series=acquisition_source
    title="Weekly Retention by Acquisition Source"
    yAxisTitle="Retention Rate"
    xAxisTitle="Weeks Since First Interaction"
/>

---

## 💰 Revenue by Acquisition Source

Which user segment is driving the most fee generation?

```sql revenue_by_source
select
    acquisition_source,
    sum(attributed_revenue_usd) as total_revenue
from defi_pipeline.fct_daily_revenue_attribution
group by acquisition_source
order by total_revenue desc
```

<BarChart 
    data={revenue_by_source}
    x=acquisition_source
    y=total_revenue
    title="Total Revenue by Acquisition Source (USD)"
    swapXY=true
/>

---

## 🏛️ Protocol Performance

```sql top_protocols
select
    p.protocol_name,
    p.category,
    sum(f.attributed_revenue_usd) as total_revenue,
    sum(f.active_wallets) as unique_users
from defi_pipeline.fct_daily_revenue_attribution f
join defi_pipeline.dim_protocols p on f.protocol_name = p.protocol_name
group by p.protocol_name, p.category
order by total_revenue desc
```

<DataTable data={top_protocols}>
  <Column id=protocol_name title="Protocol" />
  <Column id=category title="Category" />
  <Column id=total_revenue title="Total Revenue" fmt="usd" />
  <Column id=unique_users title="Unique Users" fmt="num0" />
</DataTable>

---

## 🔬 Advanced Growth Analytics (BADE)

### 🤖 Sybil & Bot Detection
Are our active wallets organic, or machine-driven? 

```sql bot_breakdown
select
    case when is_bot = 1 then 'Bot / Sybil' else 'Organic Human' end as interaction_type,
    count(distinct wallet_address) as total_wallets,
    sum(total_volume_usd) as total_volume_usd
from defi_pipeline.dim_wallets
group by interaction_type
```

<BarChart
    data={bot_breakdown}
    x=interaction_type
    y=total_volume_usd
    title="Cumulative Volume: Bots vs Humans (USD)"
    swapXY=true
/>

---

### 🧠 Smart Money Profitability
Who is actually extracting value from the ecosystem?

```sql smart_money
select
    smart_money_tier,
    count(distinct wallet_address) as wallet_count,
    sum(total_volume_usd) as total_volume_usd,
    avg(historical_win_rate) as avg_win_rate
from defi_pipeline.dim_wallets
group by smart_money_tier
order by avg_win_rate desc
```

<DataTable data={smart_money}>
  <Column id=smart_money_tier title="Wallet Tier" />
  <Column id=wallet_count title="Total Wallets" fmt="num0" />
  <Column id=total_volume_usd title="Volume Generated" fmt="usd" />
  <Column id=avg_win_rate title="Avg Win Rate (%)" fmt="pct" />
</DataTable>

---

### 🌍 Cross-Chain Nomad Score
Are our users loyal, or are they mercenary capital jumping bridges?

```sql nomad_score
select
    case 
        when nomad_score = 0 then 'Ethereum Native (Loyalist)'
        when nomad_score < 0.05 then 'Multichain Casual'
        else 'Heavy Cross-Chain (Mercenary)'
    end as loyalty_segment,
    count(distinct wallet_address) as wallet_count,
    sum(total_volume_usd) as total_volume_usd
from defi_pipeline.dim_wallets
group by loyalty_segment
order by wallet_count desc
```

<BarChart
    data={nomad_score}
    x=loyalty_segment
    y=wallet_count
    title="Wallet Distribution by Cross-Chain Loyalty"
/>
