with 

frax_transf_allocation as (
select 
    date_trunc('day',evt_block_time) as day,
    'FXS' as token_symbol,
    sum(value/1e18) as value
from erc20."ERC20_evt_Transfer"
where "to" = '\x72E158d38dbd50A483501c24f792bDAAA3e7D55C' --- Curve.fi FRAX3CRV-f Gauge Deposit
and "from" = '\xbbbaf1adf4d39b2843928cca1e65564e5ce99ccc' --- FRAX3CRV_Curve_FXS_Distributor Contract
and contract_address = '\x3432B6A60D23Ca0dFCa7761B7ab56459D9C964D0' --- FXS Token Address
and evt_block_time > '2021-03-04 01:00' --date since the first tx in contract
group by 1
order by 1
)

, frax_allocation_daily as (
select  day, 
        lead(day, 1, day + interval '1 month') OVER (ORDER BY day) AS next_day,
        value/(lead(day, 1, day + interval '1 month') OVER (ORDER BY day) ::date - day::date) as daily_frax_allocation
from frax_transf_allocation
)

, frax_avg_daily_prices as (
select date_trunc('day',"hour") as day, 
       lead(date_trunc('day',"hour"), 1, date_trunc('hour', now())) OVER ( ORDER BY date_trunc('day',"hour")) AS next_day,
       avg("median_price") as price 
from prices."prices_from_dex_data" 
where "contract_address" = '\x3432B6A60D23Ca0dFCa7761B7ab56459D9C964D0'--- FXS Token Address
group by 1
)
-------------------------------------------- TVL CTEs ------------------------------------------
-----------------------------------------------------------------------------------------------
, frax_3crv_pool_tvl as (
with
--- Token Pairs 
param as (
select * from (values
    ('\x853d955aCEf822Db058eb8505911ED77F175b99e'::bytea, 'FRAX', 18, 'Pair'),
    ('\x6c3F90f043a72FA612cbac8115EE7e52BDe6E490'::bytea, '3CRV', 18, 'Pair')    
            ) as t (token_address, symbol, decimals, asset_type)
)
-- Pool address
, pool as (
select * from (values
    ('\xd632f22692FaC7611d2AA1C0D552930D43CAEd3B'::bytea, 'FRAX3CRV-f', 18, 'Pool')
            ) as t (pool_address, symbol, decimals, asset_type)
)

, days as (
    select generate_series('2021-03-04'::timestamp, date_trunc('day', NOW()), '1 day') AS day -- Generate all days since the first tx in contract
)

-- In and Out of tokens in the Pool
, transfers as (
select day, contract_address, sum(amount) as amount 
from 
    (
    select
        date_trunc('day', evt_block_time) as day,
        contract_address,
        sum(value/1e18) as amount 
    from  erc20."ERC20_evt_Transfer"
    where contract_address in (select token_address from param)
    and "to" = (select pool_address from pool)
    group by 1,2

    union all

    select
        date_trunc('day', evt_block_time) as day,
        contract_address,
        sum(-value/1e18) as amount
    from  erc20."ERC20_evt_Transfer"
    where  contract_address in (select token_address from param)
    and "from" = (select pool_address from pool)
    group by 1,2
    )a1
    group by 1,2
)

, gap_days_tvl as (
select t.day, token_address, amount, symbol, decimals, asset_type,
    sum(amount) over (partition by token_address order by day) as running_balance,
    sum(amount) over (order by day) as tvl,    
    lead(day, 1, now()) over (partition by token_address order by day) AS next_day
from transfers t
inner join param p on t.contract_address = p.token_address
)

, all_days_tvl as (
select d.day, amount, running_balance, symbol, token_address, decimals, asset_type, tvl from gap_days_tvl b
inner join days d on b.day <= d.day and d.day < b.next_day
order by day desc
)

, summary as (
select *
from all_days_tvl a 
)

, final_tvl as (
select distinct on (day) 
    day,
    tvl
from  summary 
)

select * from final_tvl
)

----------------------------------------END TVL CTEs ------------------------------------------
-----------------------------------------------------------------------------------------------

---------------------------------------- CRV APY CALC CTEs ------------------------------------------
--------------------------------------------------------------------------------------------------
--, crv_apy_calc as (
--with 
, crv_total_inflation as ( --the crv inflation can see here https://dao.curve.fi/releaseschedule (purple color), 
--reduce about 15% each year, happened 14th of August of each year 
select  '2020-08-14 00:00'::date as time,
        '2021-08-14 00:00'::date as next_time,
        754943.21 as daily_crvinflation
union
select  '2021-08-14 00:00'::date as time,
        '2022-08-14 00:00'::date as next_time,
         633047.62 as daily_crvinflation
union
select  '2022-08-14 00:00'::date as time,
        '2023-08-14 00:00'::date as next_time,
         531061.68 as daily_crvinflation         
union
select  '2023-08-14 00:00'::date as time,
        '2024-08-14 00:00'::date as next_time,
         449826.16 as daily_crvinflation         
union
select  '2024-08-14 00:00'::date as time,
        '2025-08-14 00:00'::date as next_time,
         375298.42 as daily_crvinflation         
union
select  '2025-08-14 00:00'::date as time,
        '2026-08-14 00:00'::date as next_time,
         314972.50 as daily_crvinflation         

) 

, gauge_weights as (
select date_trunc('minute',"call_block_time") as time, 
        lead("call_block_time", 1, now()) over (order by "call_block_time") as next_time,
        output_0/1e18 as weight
from curvefi."gauge_controller_call_gauge_relative_weight0" 
where addr = '\x72E158d38dbd50A483501c24f792bDAAA3e7D55C'--- Curve.fi FRAX3CRV-f Gauge Deposit
order by "call_block_time" desc
)

, crv_prices as ( 
select distinct date_trunc('day', minute) as day,
last_value(price) over (partition by date_trunc('day', minute) order by minute range between unbounded preceding and unbounded following) as price
from prices."usd" p
where "contract_address" = '\xd533a949740bb3306d119cc777fa900ba034cd52' --crv
  and date_trunc('day',"minute") >= '2020-09-15'
order by 1
)

, stecrv_gauge_inflation as (
select  date_trunc('day',w.time) as day, daily_crvinflation, avg(weight) as weight,
        avg(daily_crvinflation*weight/86400) as stecrv_inflation,
        avg(daily_crvinflation*weight*p.price/86400) as stecrv_inflation_usd
from gauge_weights w 
left join crv_total_inflation inf on w.time >= inf.time and w.time < inf.next_time 
left join crv_prices p on date_trunc('day',w.time) = p.day
group by 1,2
)

, gauge_working_supplies as ( 
select date_trunc('day',time) as day, avg(wsupply/1e18) as wsupply
from (
    select distinct "evt_block_time" as time, "working_supply" as wsupply--, *
    from curvefi."frax_gauge_evt_UpdateLiquidityLimit"
    order by "evt_block_time" desc
    )q1
    group by 1
)

, pool_virtual_price as (
select date_trunc('day',time) as day, avg(vprice/1e18) as vprice,
lead(date_trunc('day',time), 1, now()) over (order by (date_trunc('day',time))) as next_day
from (
    select "call_block_time" as time, --lead("call_block_time",1, now()) over (order by "call_block_time") as next_time, 
    "output_0" as vprice
    from curvefi."frax_call_get_virtual_price"
    order by "call_block_time" desc
     )q2
     group by 1
)

, crv_apy_calc_metrics_with_lead as (
select *, lead(day, 1, now()) over (order by day) as next_day
    from (
        select s.day, s.wsupply, max(vprice) as vprice
        from gauge_working_supplies s
        left join pool_virtual_price p on s.day >= p.day and s.day < p.next_day
        group by 1,2
        )q3
        where vprice is not null
)

--)

---------------------------------------- END CRV APY CALC CTEs ------------------------------------------
--------------------------------------------------------------------------------------------------

,crv_rewards_apy as (
select c.*,
100*0.4*31536000*inf.stecrv_inflation_usd/(r.wsupply*r.vprice) as "Min CRV APY Curve, %",
2.5*100*0.4*31536000*inf.stecrv_inflation_usd/(r.wsupply*r.vprice) as "Max CRV APY Curve, %"
from frax_3crv_pool_tvl c
left join stecrv_gauge_inflation inf on date_trunc('day', c.day) = inf.day
left join crv_apy_calc_metrics_with_lead r on c.day >= r.day and c.day < r.next_day
order by 1 desc
)
,frax_rewards_apy as (
select a.day, 
       tvl, 
       coalesce(daily_frax_allocation,0) as daily_frax_allocation,
       (365 * coalesce(daily_frax_allocation,0) * price * 100)/nullif(tvl,0) as "FXS APY %"
from frax_3crv_pool_tvl a
left join frax_allocation_daily b  on a.day >= b.day and  a.day < b.next_day
left join frax_avg_daily_prices c  on a.day = c.day
)

, final as (
select 
    a.day,
    a.tvl,
   -- coalesce("Min CRV APY Curve, %",0) as "Min CRV APY Curve, %",
    (coalesce("Max CRV APY Curve, %",0))/100 as "CRV Rewards APY",
    ("FXS APY %")/100 as "FXS APY",
    ("Max CRV APY Curve, %" + "FXS APY %")/100 as "Total APY",
     ("Max CRV APY Curve, %" + "FXS APY %") as "Total APY, %"
from crv_rewards_apy a
left join frax_rewards_apy b on a.day = b.day
)

select * from final
where day >= now() - interval '{{Time Period}}'
