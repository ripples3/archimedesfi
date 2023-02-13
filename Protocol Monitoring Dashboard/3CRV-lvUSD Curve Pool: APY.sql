with 
lvusd_transf_allocation as (
select 
    date_trunc('day',evt_block_time) as day,
    'CEA' as token_symbol,
    sum(value/1e18) as value
from erc20_ethereum.evt_Transfer
where `to` = lower('0x5770f9a1d363b1a309cc348acbe6fFFa899f8Ceb') --- Curve.fi lvUSD3CRV-f Gauge Deposit
and `from` = lower('0x45a691eB91B23cbcF833D98DB70c00231D2eeC3e') --- CEA Treasury wallet addres
and contract_address = lower('0xD6e61183915cd72a010dF81574edBdc7f4F254b2') --- CEA Token Address
and evt_block_time > '2021-03-04 01:00' --date since the first tx in contract
group by 1
order by 1
)

, arch_allocation_daily as (
select  day,
        lead(day, 1, day + interval '1 month') OVER (ORDER BY day) AS next_day, 
        value/(lead(day, 1, day + interval '1 month') OVER (ORDER BY day)::date - day::date)::numeric as  daily_lvusd_allocation
from lvusd_transf_allocation
)

, arch_avg_daily_prices as (
with 
underlying_tokens as (
select * from (values
	('0xd6e61183915cd72a010df81574edbdc7f4f254b2', 'ARCH',  18)

) as t (token_address, token_symbol, decimals)
)

, days as (
   select explode(sequence('2021-03-04'::timestamp, now(), interval '1 day')) as day 
)

, prices_dex_trades as (
select blockchain, block_date, block_time, --'ARCH' as symbol,
case when token_bought_address = (select token_address from underlying_tokens where token_symbol = 'ARCH') then amount_usd/(token_bought_amount_raw/1e18)
else amount_usd/(token_sold_amount_raw/1e18)
end as price
from dex.trades
where (  token_bought_address = (select token_address from underlying_tokens where token_symbol = 'ARCH')
      or token_sold_address = (select token_address from underlying_tokens where token_symbol = 'ARCH')
      )
and project = 'uniswap' and version = '3'
)


, daily_price_gap as (
select *, lead(day, 1, now()) over (order by day) AS next_day
from (
    select blockchain, block_date as day, avg(price) as price_usd
    from prices_dex_trades
    group by 1,2
     )t1
)

, daily_price_all as (
select d.day, blockchain, price_usd
from daily_price_gap b
inner join days d on b.day <= d.day and d.day < b.next_day
order by day desc
)
select day, token_symbol as symbol, price_usd from daily_price_all cross join underlying_tokens
)



/*
, arch_avg_daily_prices as (
select date_trunc('day', minute) as day, symbol, avg(price) as price_usd
from prices.usd
where contract_address = lower('0x3432B6A60D23Ca0dFCa7761B7ab56459D9C964D0') -- ARCH
and blockchain = 'ethereum'
and date_trunc('day',minute) >= '2020-09-15'
group by 1,2
order by 1
)
*/
/*
, arch_avg_daily_prices as (
with
underlying_tokens as (
select * from (values
	(lower('0x3432B6A60D23Ca0dFCa7761B7ab56459D9C964D0'), 'ARCH',  18)

) as t (token_address, token_symbol, decimals)
)


, prices_dex_trades as (
select blockchain, block_date, block_time, --'ARCH' as symbol,
case when token_bought_address = (select token_address from underlying_tokens where token_symbol = 'ARCH') then amount_usd/(token_bought_amount_raw/1e18)
else amount_usd/(token_bought_amount_raw/1e18)
end as price
from dex.trades
where (  token_bought_address = (select token_address from underlying_tokens where token_symbol = 'ARCH')
      or token_sold_address = (select token_address from underlying_tokens where token_symbol = 'ARCH')
      )
and project = 'uniswap' and version = '3'
)

, daily_price as (
select blockchain, block_date as day, block_time, avg(price) as price_usd
from prices_dex_trades
group by 1,2,3
)

select * from daily_price cross join underlying_tokens
)

*/


-------------------------------------------- TVL CTEs ------------------------------------------
-----------------------------------------------------------------------------------------------
, lvusd_3crv_pool_tvl as (
with
--- Token Pairs 
param as (
select * from (values
    (lower('0x72dfe359150984C8013105BbbAEe9a152335bD23'), 'lvUSD', 18, 'Pair'),
    (lower('0x6c3F90f043a72FA612cbac8115EE7e52BDe6E490'), '3CRV', 18, 'Pair')    
            ) as t (token_address, symbol, decimals, asset_type)
)
-- Pool address
, pool as (
select * from (values
    (lower('0x8D35ECe39566d65d06c9207C571934DD3C3a3916'), 'cealv3CRV-f', 18, 'Pool')
            ) as t (pool_address, symbol, decimals, asset_type)
)

, days as (
   select explode(sequence('2021-03-04'::timestamp, now(), interval '1 day')) as day -- Generate all days since the creation of the contract
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
    from  erc20_ethereum.evt_Transfer
    where contract_address in (select token_address from param)
    and `to` = (select pool_address from pool)
    group by 1,2

    union all

    select
        date_trunc('day', evt_block_time) as day,
        contract_address,
        sum(-value/1e18) as amount
    from  erc20_ethereum.evt_Transfer
    where  contract_address in (select token_address from param)
    and `from` = (select pool_address from pool)
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
select distinct (day) 
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
select date_trunc('minute',call_block_time) as time, 
        lead(call_block_time, 1, now()) over (order by call_block_time) as next_time,
        output_0/1e18 as weight
from curvefi_ethereum.gauge_controller_call_gauge_relative_weight
where addr = lower('0x5770f9a1d363b1a309cc348acbe6fFFa899f8Ceb')--- Curve.fi cealv3CRV-f Gauge Deposit
order by call_block_time desc
)

, crv_prices as ( 
select distinct date_trunc('day', minute) as day, avg(price) as price
     --  last_value(price) over (partition by date_trunc('day', minute) order by minute range between unbounded preceding and unbounded following) as price
from prices.usd p
--where symbol = 'CRV'
where contract_address = lower('0xd533a949740bb3306d119cc777fa900ba034cd52') --crv
and blockchain = 'ethereum'
and date_trunc('day',minute) >= '2020-09-15'
group by 1
order by 1
--limit 10
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
    select evt_block_time as time, working_supply as wsupply--, *
    from curvefi_ethereum.frax_gauge_evt_UpdateLiquidityLimit 
    order by evt_block_time desc
    )q1
    group by 1
)

, pool_virtual_price as (
select *, lead(day, 1, now()) over (order by day) as next_day
from (
    select date_trunc('day',call_block_time) as day, avg(output_0/1e18) as vprice
    from curvefi_ethereum.frax_call_get_virtual_price
    group by 1
    order by 1 desc
     )q2
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


---------------------------------------- END CRV APY CALC CTEs ------------------------------------------
--------------------------------------------------------------------------------------------------

, crv_rewards_apy as (
select c.*,
100*0.4*31536000*inf.stecrv_inflation_usd/(r.wsupply*r.vprice) as  min_crvapy_curve_prcnt,--"Min CRV APY Curve, %",
2.5*100*0.4*31536000*inf.stecrv_inflation_usd/(r.wsupply*r.vprice) as max_crvapy_curve_prcnt --"Max CRV APY Curve, %"
from lvusd_3crv_pool_tvl c
left join stecrv_gauge_inflation inf on date_trunc('day', c.day) = inf.day
left join crv_apy_calc_metrics_with_lead r on c.day >= r.day and c.day < r.next_day
order by 1 desc
)


, lvusd_rewards_apy as (
select a.day, 
       tvl, 
       coalesce(daily_lvusd_allocation,0) as daily_lvusd_allocation,
       (365 * coalesce(daily_lvusd_allocation,0) * price_usd * 100)/nullif(tvl,0) as cealv_apy_prct-- "FXS APY %"
from lvusd_3crv_pool_tvl a
left join arch_allocation_daily b  on a.day >= b.day and  a.day < b.next_day
left join arch_avg_daily_prices c  on a.day = c.day
)


, final as (
select 
    a.day,
    a.tvl,
   -- coalesce("Min CRV APY Curve, %",0) as "Min CRV APY Curve, %",
    coalesce((max_crvapy_curve_prcnt)/100,0) as `CRV Rewards APY`,
    coalesce((max_crvapy_curve_prcnt),0) as `CRV Rewards APY, %`,    
    coalesce((cealv_apy_prct)/100,0) as  `ARCH APY`,
    coalesce((cealv_apy_prct),0) as `ARCH APY, %`
    --(max_crvapy_curve_prcnt + cealv_apy_prct)/100 as `Total APY`,
    --(max_crvapy_curve_prcnt + cealv_apy_prct) as `Total APY, %`
from crv_rewards_apy a
left join lvusd_rewards_apy b on a.day = b.day
)

, summarize_result as (
select * from final
--where day >= now() - interval '{{Time Period}}'
)

select day,
tvl,
'3CRV/lvUSD Curve Pool' as pool,
`CRV Rewards APY`,
`CRV Rewards APY, %`,
`ARCH APY` as `ARCH APY`,
`ARCH APY, %` as `ARCH APY, %`,
`CRV Rewards APY` + `ARCH APY` as `Total APY`,
`CRV Rewards APY, %` + `ARCH APY, %` as `Total APY, %`
from summarize_result
where day >= now() - interval '{{Time Period}}'
order by 1 desc
--select * from lvusd_rewards_apy
