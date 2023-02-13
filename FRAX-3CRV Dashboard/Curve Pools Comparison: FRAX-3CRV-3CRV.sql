---https://dune.com/queries/1381807/2350003

with

a3crv as (
with 

underlying_tokens as (
select * from (values

	('\x3Ed3B47Dd13EC9a98b44e6204A523E766B225811'::bytea, 'aUSDT',      6, 'aToken'),
	('\x028171bCA77440897B824Ca71D1c56caC55b68A3'::bytea, 'aDAI',      18, 'aToken'),
    ('\xBcca60bB61934080951369a648Fb03DF4F96263C'::bytea, 'aUSDC',      6, 'aToken')

) as t (token_address, token_symbol, decimals,token_type)
)

, pool as (
select * from (values
    ('\xDeBF20617708857ebe4F679508E7b7863a8A8EeE'::bytea, 'Curve.fi: aDAI/aUSDC/aUSDT Pool')
) as t (pool_address, poolname)
)

, days as (select generate_series('2020-10-13'::timestamp, date_trunc('day', NOW()), '1 day') AS day) -- Generate all days since the first deposit in the pool
, days_underlying_tokens as (select * from days cross join underlying_tokens )

, atoken as (
select  date_trunc('day',minute) as day, token_symbol, atoken as atoken_balance
from (
        select 
                token_symbol, call_block_time as minute, "user" as pool_address, output_0/10^decimals as atoken,
                row_number() over (partition by token_symbol, date_trunc('day',call_block_time) order by call_block_time desc)
        from aave_v2."AToken_call_balanceOf" a 
        inner join pool b                                   on a."user"                = b.pool_address  
        inner join underlying_tokens c                      on a."contract_address"    = c.token_address
     ) q1
        where row_number = 1
        order by 2 desc, 1 desc
)

, aave_rates_gap as (
select a.*, b.supplyrate, b.apy, sum(atoken_balance) over (partition by a.day) as tvl,
lead(a.day, 1, now()) over (partition by token_symbol order by a.day) as next_day
from atoken a
left join dune_user_generated.indexcoop_aave_rates b on a.day = b.day and a.token_symbol =	b.atoken_symbol
order by a.day desc
)

, aave_rates as (
select d.day, b.token_symbol, b.atoken_balance, b.supplyrate, b.apy, b.tvl
from aave_rates_gap b
inner join days d on b.day <= d.day and d.day < b.next_day
order by d.day desc
)

/*
-- old query
, aave_rates as (
select a.*, b.supplyrate, b.apy, sum(atoken_balance) over (partition by a.day) as tvl
from aave_rates_gap a
left join dune_user_generated.indexcoop_aave_rates b on a.day = b.day and a.token_symbol =	b.atoken_symbol
order by a.day desc
)
*/

, stkaave as (
select date_trunc('day',evt_block_time) as day,
       'AAVE' as token_symbol,
       sum(value/1e18) as rewards
from erc20."ERC20_evt_Transfer"
where "from" = '\xd662908ADA2Ea1916B3318327A97eB18aD588b5d'
  and contract_address = '\x4da27a545c0c5B758a6BA100e3a049001de870f5'
  and evt_block_time > '2020-12-21 01:00' --date since the first tx in contract
group by 1
order by 1
)

, gap_days_rewards as (
select *, lead(day, 1, now()) over (partition by token_symbol order by day) as next_day
from (
    select * from stkaave
    ) a2
)

, all_days_rewards as (
select d.day, token_symbol, rewards from gap_days_rewards b
inner join days d on b.day <= d.day and d.day < b.next_day
order by day desc
)

, prices as (
select date_trunc('day', minute) as day, "symbol", avg("price") as price
from prices."usd"
where "contract_address" = '\x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9'
group by 1,2
)

, all_rewards as (
select a.*, sum(b.price * rewards) over (partition by a.day) as reward_amount_usd 
from all_days_rewards a
left join prices b                  on a.day = b.day and a.token_symbol = b.symbol
)

, apy as (
select day, token_symbol, atoken_balance,tvl,apy, atoken_balance/NULLIF(tvl,0) as weight, apy * (atoken_balance/NULLIF(tvl,0)) as aapy,
sum(apy * (atoken_balance/NULLIF(tvl,0))) over (partition by day) as pool_apy
from aave_rates 
)

, summary as (
select distinct on (a.day) a.day, tvl, pool_apy, coalesce(reward_amount_usd,0) as reward_amount_usd, 100 * coalesce(reward_amount_usd,0)/NULLIF(tvl,0) as reward_apr
from apy a 
left join all_rewards b on a.day = b.day
)

, final_summary as (
select 
    day, tvl, pool_apy as lending_apy, 
    ((1+reward_apr/365)^365) - 1 as stkaave_apy, 
    pool_apy +  ((1+reward_apr/365)^365) - 1 as total_apy
from summary
)

select day, 
       tvl, 'AAVE a3Crv Pool' as pool,
     --  null as "Min CRV APY Curve, %", 
      -- null as "CRV Rewards APY", 
     --  (lending_apy)/100 as "Base APY",
       (stkaave_apy + lending_apy)/100 as "Total APY"
from final_summary
)

, threecrv as (
with crv_total_inflation as ( --the crv inflation can see here https://dao.curve.fi/releaseschedule (purple color), 
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
where addr = '\xbFcF63294aD7105dEa65aA58F8AE5BE2D9d0952A'
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
    from curvefi."threepool_gauge_evt_UpdateLiquidityLimit" 
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
    from curvefi."threepool_swap_call_get_virtual_price"
    where "call_success" = 'true'
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

---------------------------------------- TVL CTEs ------------------------------------------
-----------------------------------------------------------------------------------------------
, tvl as (
with 

underlying_tokens as (
select * from (values

	('\xdAC17F958D2ee523a2206206994597C13D831ec7'::bytea, 'USDT',      6, 'Token'),
	('\x6B175474E89094C44Da98b954EedeAC495271d0F'::bytea, 'DAI',      18, 'Token'),
    ('\xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'::bytea, 'USDC',      6, 'Token')

) as t (token_address, token_symbol, decimals, token_type)
)

, pool as (
select * from (values
    ('\xbEbc44782C7dB0a1A60Cb6fe97d0b483032FF1C7'::bytea, 'Curve.fi: DAI/USDC/USDT Pool')
) as t (pool_address, poolname)
)

, days as (
    select generate_series('2020-09-06'::timestamp, date_trunc('day', NOW()), '1 day') AS day -- Generate all days since the first tx in contract
)

-- In and Out of tokens in the Pool
, transfers as (
select day, contract_address, sum(amount) as amount 
from 
    (
    select
        date_trunc('day', evt_block_time) as day,
        contract_address,
        sum(value/10^decimals) as amount 
    from  erc20."ERC20_evt_Transfer" a
    inner join underlying_tokens b      on a.contract_address = b.token_address    
 --   where contract_address in (select token_address from underlying_tokens)
    and "to" = (select pool_address from pool)
    group by 1,2

    union all

    select
        date_trunc('day', evt_block_time) as day,
        contract_address,
        sum(-value/10^decimals) as amount
    from  erc20."ERC20_evt_Transfer" a
    inner join underlying_tokens b      on a.contract_address = b.token_address
  --  where  contract_address in (select token_address from underlying_tokens)
    and "from" = (select pool_address from pool)
    group by 1,2
    )a1
    group by 1,2
)

, gap_days_tvl as (
select t.day, token_address, amount, token_symbol, decimals, token_type,
    sum(amount) over (partition by token_address order by day) as running_balance,
    --sum(amount) over (partition by t.day) as tvl,    
    lead(day, 1, now()) over (partition by token_address order by day) AS next_day
from transfers t
inner join underlying_tokens p on t.contract_address = p.token_address
)

, all_days_tvl as (
select d.day, amount, running_balance, token_symbol, token_address, decimals, token_type, sum(running_balance) over (partition by d.day) as tvl from gap_days_tvl b
inner join days d on b.day <= d.day and d.day < b.next_day
order by day desc
)

select distinct on (day) day, tvl
from all_days_tvl
)
----------------------------------------END TVL CTEs ------------------------------------------
-----------------------------------------------------------------------------------------------

, summary as (
select c.day, tvl,  (vprice - lag(vprice) over (order by c.day)) / lag(vprice) over (order by c.day) as "%", vprice, lag(vprice) over (order by c.day) as lvprice,
-- 31536000*inf.stecrv_inflation_usd*100/tvl as synthetic,
 100*0.4*31536000*inf.stecrv_inflation_usd/(r.wsupply*r.vprice) as "Min CRV APY Curve, %",
 2.5*100*0.4*31536000*inf.stecrv_inflation_usd/(r.wsupply*r.vprice) as "Max CRV APY Curve, %"
--100 * (stecrv_inflation* 100 * weight*31536000/ wsupply* 0.4)/vprice as sdf,
 --31536000*inf.stecrv_inflation_usd*100/tvl as synthetic
-- 100*0.4*31536000*inf.stecrv_inflation_usd/(r.wsupply*r.vprice) as "Min CRV APY Curve, %"
from tvl c
left join stecrv_gauge_inflation inf on date_trunc('day', c.day) = inf.day
left join crv_apy_calc_metrics_with_lead r on c.day >= r.day and c.day < r.next_day
order by 1 desc
)

, final_summary as (
--   const apy = (((1 + rateDaily) ** 365) - 1) * 100;
select *,  (((1+"%")^365)-1) * 100 as base_apy
from summary
)

select day, 
       tvl, '3Crv Pool' as pool,
    --   "Min CRV APY Curve, %", 
    --   ("Max CRV APY Curve, %")/100 as "CRV Rewards APY", 
    --    (base_apy)/100 as "Base APY",
       ("Max CRV APY Curve, %" + base_apy)/100 as "Total APY"
from final_summary
)

, frax3crv as (
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
       (365 * coalesce(daily_frax_allocation,0) * price * 100)/nullif(tvl,0) as "FRAX APY %"
from frax_3crv_pool_tvl a
left join frax_allocation_daily b  on a.day >= b.day and  a.day < b.next_day
left join frax_avg_daily_prices c  on a.day = c.day
)

, final as (
select 
    a.day,
    a.tvl, 'FRAX+3Crv Pool' as pool,
   -- coalesce("Min CRV APY Curve, %",0) as "Min CRV APY Curve, %",
  --  (coalesce("Max CRV APY Curve, %",0))/100 as "CRV Rewards APY",
  --  ("FRAX APY %")/100 as "FRAX APY",
    ("Max CRV APY Curve, %" + "FRAX APY %")/100 as "Total APY"
   --  ("Max CRV APY Curve, %" + "FRAX APY %") as "Total APY, %"
from crv_rewards_apy a
left join frax_rewards_apy b on a.day = b.day
)

select * from final
)

select *, "Total APY" * 100 as "Total APY, %" from frax3crv
where day >= now() - interval '{{Time Period}}'
union all
select *, "Total APY" * 100 as "Total APY, %" from a3crv
where day >= now() - interval '{{Time Period}}'
union all
select *, "Total APY" * 100 as "Total APY, %" from threecrv
where day >= now() - interval '{{Time Period}}'
order by day desc, pool desc
