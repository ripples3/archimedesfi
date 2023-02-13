with 
---------------------------------------[Start] newdefi__3crvprice -------------------------------------------------
-------------------------------------------------------------------------------------------------------------------
newdefi__3crvprice as (
with crv_total_inflation as ( 
--the crv inflation can see here https://dao.curve.fi/releaseschedule (purple color), 
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
where addr = lower('0xbFcF63294aD7105dEa65aA58F8AE5BE2D9d0952A')  -- Curve pool 3crv/cealv gauge 
order by call_block_time desc
)

, crv_prices as ( 
select date_trunc('day', minute) as day, symbol, avg(price) as price
from prices.usd
where contract_address = lower('0xd533a949740bb3306d119cc777fa900ba034cd52') -- CRV
and blockchain = 'ethereum'
and date_trunc('day',minute) >= '2020-09-15'
group by 1,2
order by 1
)

, stecrv_gauge_inflation as (
select  date_trunc('day',w.time) as day, daily_crvinflation, avg(weight) as weight,
        coalesce(avg(daily_crvinflation*weight/86400),0) as stecrv_inflation,
        coalesce(avg(daily_crvinflation*weight*p.price/86400),0) as stecrv_inflation_usd
from gauge_weights w 
left join crv_total_inflation inf on w.time >= inf.time and w.time < inf.next_time 
left join crv_prices p on date_trunc('day',w.time) = p.day
group by 1,2
)

, gauge_working_supplies as ( 
select time as day, (wsupply/1e18) as wsupply
from (
    select 
          date_trunc('day',evt_block_time) as time,
          avg(working_supply) as wsupply
    from curvefi_ethereum.threepool_gauge_evt_UpdateLiquidityLimit
    group by 1   
    order by 1 desc
    )q1
)

, pool_virtual_price as (
select time as day, vprice/1e18 as vprice,
lead(date_trunc('day',time), 1, now()) over (order by (date_trunc('day',time))) as next_day
from (
    select 
          date_trunc('day',call_block_time) as time,
          avg(output_0) as vprice
          --row_number() over (partition by date_trunc('day',call_block_time) order by call_block_time desc, call_trace_address desc) as row_number
    from curvefi_ethereum.threepool_swap_call_get_virtual_price
    where call_success = 'true' 
    group by 1   
    order by 1 desc
     )q2
)

, crv_apy_calc_metrics_with_lead as (
select *, lead(day, 1, now()) over (order by day) as next_day
    from (
        select s.day, s.wsupply, avg(vprice) as vprice
        from gauge_working_supplies s
        left join pool_virtual_price p on s.day >= p.day and s.day < p.next_day
        group by 1,2
        )q3
       where vprice is not null
)
-------------------- TVL CTEs day, tvl ---------------------------------
------------------------------------------------------------------------
, tvl as (
with 

underlying_tokens as (
select * from (values

	(lower('0xdAC17F958D2ee523a2206206994597C13D831ec7'), 'USDT',      6, 'Token'),
	(lower('0x6B175474E89094C44Da98b954EedeAC495271d0F'), 'DAI',      18, 'Token'),
    (lower('0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'), 'USDC',       6, 'Token')
             ) as t (token_address, token_symbol, decimals, token_type)
)

, pool as (
select * from (values
    (lower('0xbEbc44782C7dB0a1A60Cb6fe97d0b483032FF1C7'), 'Curve.fi: DAI/USDC/USDT Pool')
) as t (pool_address, poolname)
)

, days as (select explode(sequence('2020-09-06'::timestamp, now(), interval '1 day')) as day )-- Generate all days since the creation of the contract

-- In and Out of tokens in the Pool
, transfers as (
select day, contract_address, sum(amount) as amount 
from 
    (
    select
        date_trunc('day', evt_block_time) as day,
        contract_address,
        sum(value/power(10,decimals)) as amount 
    from  erc20_ethereum.evt_Transfer a
    inner join underlying_tokens b      on a.contract_address = b.token_address    
 --   where contract_address in (select token_address from underlying_tokens)
    and `to` = (select pool_address from pool)
    group by 1,2

    union all

    select
        date_trunc('day', evt_block_time) as day,
        contract_address,
        sum(-value/power(10,decimals)) as amount
    from  erc20_ethereum.evt_Transfer a
    inner join underlying_tokens b      on a.contract_address = b.token_address
  --  where  contract_address in (select token_address from underlying_tokens)
    and `from` = (select pool_address from pool)
    group by 1,2
    )a1
    group by 1,2
)

, gap_days_tvl as (
select t.day, token_address, amount, token_symbol, decimals, token_type,
    sum(amount) over (partition by token_address order by day) as running_balance,
    lead(day, 1, now()) over (partition by token_address order by day) AS next_day
from transfers t
inner join underlying_tokens p on t.contract_address = p.token_address
)

, all_days_tvl as (
select d.day, amount, running_balance, token_symbol, token_address, decimals, token_type, sum(running_balance) over (partition by d.day) as tvl from gap_days_tvl b
inner join days d on b.day <= d.day and d.day < b.next_day
order by day desc
)

select distinct day, tvl
from all_days_tvl
)

----------------------------------------END TVL CTEs day, tvl -----------------------------
-------------------------------------------------------------------------------------------

, summary as (
select 
    c.day, tvl,  (vprice - lag(vprice) over (order by c.day)) / lag(vprice) over (order by c.day) as `%`, 
    coalesce(100*0.4*31536000*inf.stecrv_inflation_usd/(r.wsupply*r.vprice),0) as `Min CRV APY Curve, %`,
    coalesce(2.5*100*0.4*31536000*inf.stecrv_inflation_usd/(r.wsupply*r.vprice),0) as `Max CRV APY Curve, %`,
    stecrv_inflation_usd, r.wsupply, r.vprice
from tvl c
left join stecrv_gauge_inflation inf on date_trunc('day', c.day) = inf.day
left join crv_apy_calc_metrics_with_lead r on c.day >= r.day and c.day < r.next_day
order by 1 desc
)

, final_summary as (
--   const apy = (((1 + rateDaily) ** 365) - 1) * 100;
select *,  (power((1+`%`),365)-1) * 100 as base_apy
from summary
)

,temp as (
select day, 
       tvl, '3Crv Pool' as pool,
       coalesce(base_apy/100,0) as `Base APY`,
       coalesce((`Max CRV APY Curve, %` + base_apy)/100,0) as `Total APY`
from final_summary
)
, return_cte as (
select *, sum(`Base APY`/365) over (order by day asc) + 1 as price
from temp
order by day desc
)

select distinct day, pool, tvl, `Base APY`, `Total APY`, price  from return_cte
)

---------------------------------------[End] newdefi__3crvprice -------------------------------------------------
-----------------------------------------------------------------------------------------------------------------

---------------------------------------[Start] cealv_tvl --------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------
, cealv_tvl as (
with
--- Token Pairs 
param as (
select * from (values
    (lower('{{Token1}}'), 'lvUSD', 18, 'Pair'),
    (lower('{{Token2}}'), '3CRV', 18, 'Pair')    
            ) as t (token_address, symbol, decimals, asset_type)
)

-- Pool address
, pool as (
select * from (values
    (lower('{{Pool}}'), 'cealv3CRV-f', 18, 'Pool')
            ) as t (pool_address, symbol, decimals, asset_type)
)

, days as (
   select explode(sequence('2021-03-04'::timestamp, now(), interval '1 day')) as day 
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
select d.day, amount, running_balance, symbol as token_symbol, token_address, decimals, asset_type, tvl from gap_days_tvl b
inner join days d on b.day <= d.day and d.day < b.next_day
order by day desc
)

select * from all_days_tvl
)

---========================================================--threecrvpool_ratio ================================================
---=============================================================================================================================

---========================================================--threecrvpool_ratio ================================================
---=============================================================================================================================
, threecrvpool_ratio as (
with
underlying_tokens as (
select * from (values

	(lower('0xdAC17F958D2ee523a2206206994597C13D831ec7'), 'USDT',      6, 'Token'),
	(lower('0x6B175474E89094C44Da98b954EedeAC495271d0F'), 'DAI',      18, 'Token'),
    (lower('0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'), 'USDC',      6, 'Token')
) as t (token_address, symbol, decimals, token_type)
)

, pool as (
select * from (values
    (lower('0xbEbc44782C7dB0a1A60Cb6fe97d0b483032FF1C7'), 'Curve.fi: DAI/USDC/USDT Pool')
) as t1 (pool_address, poolname)
)

, days as (
   select explode(sequence('2020-09-06'::timestamp, now(), interval '1 day')) as day 
)

-- In and Out of tokens in the Pool
, transfers as (
select day, contract_address, sum(amount) as amount 
from 
    (
    select
        date_trunc('day', evt_block_time) as day,
        contract_address,
        sum(value/power(10,decimals)) as amount 
    from  erc20_ethereum.evt_Transfer a
    inner join underlying_tokens b      on a.contract_address = b.token_address    
 --   where contract_address in (select token_address from underlying_tokens)
    and `to` = (select pool_address from pool)
    group by 1,2

    union all

    select
        date_trunc('day', evt_block_time) as day,
        contract_address,
        sum(-value/power(10,decimals)) as amount
    from  erc20_ethereum.evt_Transfer a
    inner join underlying_tokens b      on a.contract_address = b.token_address
  --  where  contract_address in (select token_address from underlying_tokens)
    and `from` = (select pool_address from pool)
    group by 1,2
    )a1
    group by 1,2
)

, gap_days_tvl as (
select t.day, token_address, amount, symbol, decimals, token_type,
    sum(amount) over (partition by token_address order by day) as running_balance,
    --sum(amount) over (partition by t.day) as tvl,    
    lead(day, 1, now()) over (partition by token_address order by day) AS next_day
from transfers t
inner join underlying_tokens p on t.contract_address = p.token_address
)

, all_days_tvl as (
select d.day, amount, running_balance, symbol, token_address, decimals, token_type, sum(running_balance) over (partition by d.day) as tvl from gap_days_tvl b
inner join days d on b.day <= d.day and d.day < b.next_day
order by day desc
)

, final_summary as (
select day, symbol, running_balance/tvl as prct
from all_days_tvl
)

select * from final_summary
where day >= '2021-03-04' -- Date the first FRAX/3Crv tx 
)
---========================================================--threecrvpool_ratio ================================================
---=============================================================================================================================

, running_3crv_balance as (
select a.day, symbol, running_balance,  prct
from cealv_tvl a
inner join threecrvpool_ratio b on a.day = b.day
where a.token_symbol = '3CRV'
)

, temp as (
select a.day, symbol, prct, running_balance, 
       running_balance * price as running_balance_usd,
       running_balance * price * prct as underlying_running_balance_usd 
from running_3crv_balance a
left join newdefi__3crvprice b on a.day = b.day
order by 1 desc, 2 desc
)


, lvusd_crv_ratio as (
select a.day, symbol, underlying_running_balance_usd as running_balance, '3CRV' as pair
from temp a

union all

select day, token_symbol, running_balance, 'lvUSD' as pair
from cealv_tvl
where token_symbol != '3CRV'
order by 1 desc, 2 desc
)

--select * from lvusd_crv_ratio

, summarize_all as (
select distinct day,
symbol, 
running_balance,
pair,
sum(running_balance) over (partition by day,pair order by day) as tvl,
100 * sum(running_balance) over (partition by day,pair order by day) /nullif(sum(running_balance) over (partition by day),0) as ratio_pair
--100 * sum(running_balance) over (partition by day,token_symbol order by day) /nullif(sum(running_balance) over (partition by day),0) as ratio_all
from lvusd_crv_ratio
--where day >= now() - interval '{{Time Period}}'
--order by day desc, pair desc
)

select day, 'Total' as symbol, sum(running_balance) over (partition by day)as running_balance, 'Total' as pair, tvl, 100 as ratio_pair
from summarize_all
where day >= now() - interval '{{Time Period}}'
--group by 1,2,4,5

union all

select *
from summarize_all
where day >= now() - interval '{{Time Period}}'
order by day desc, pair desc
