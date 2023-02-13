with cealv_tvl as (
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
select d.day, amount, running_balance, symbol, token_address, decimals, asset_type, tvl from gap_days_tvl b
inner join days d on b.day <= d.day and d.day < b.next_day
order by day desc
)
select * from all_days_tvl
)


------------------------------------- 3Pool Underlying Assets Info --------------------------------
, threecrvpool_ratio as (
with 
underlying_tokens as (
select * from (values

	(lower('0xdAC17F958D2ee523a2206206994597C13D831ec7'), 'USDT',      6, 'Token'),
	(lower('0x6B175474E89094C44Da98b954EedeAC495271d0F'), 'DAI',      18, 'Token'),
    (lower('0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48'), 'USDC',      6, 'Token')

) as t (token_address, token_symbol, decimals, token_type)
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

, final_summary as (
select day, token_symbol, running_balance/tvl as prct
from all_days_tvl
)


select * from final_summary
--where day >= '2021-03-04' -- Date the first lvusd/3Crv tx 
)
-------------------------------------End 3Pool Underlying Assets Info --------------------------------


, lvusd_crv_ratio as (
select a.day, token_symbol, running_balance * prct as running_balance, '3CRV' as pair
from cealv_tvl a
inner join threecrvpool_ratio b on a.day = b.day
where symbol = '3CRV'

union all

select day, symbol as token_symbol, running_balance, 'lvUSD' as pair
from cealv_tvl
where symbol = 'lvUSD'
order by 1 desc, 2 desc
)


select *, sum(running_balance) over (partition by day,pair order by day) as tvl, 
     -- sum(running_balance) over (partition by day order by day) as `Total Balance`,
100 * sum(running_balance) over (partition by day,pair order by day) /nullif(sum(running_balance) over (partition by day),0) as ratio
from lvusd_crv_ratio
where day >= now() - interval '{{Time Period}}'
--order by day desc, token_symbol desc

union all 


select *,  sum(running_balance) over (partition by day order by day) as tvl, null as ratio
from (
    select 
      day, 'Total Balance' as token_symbol, sum(running_balance) as running_balance, null as pair
    from lvusd_crv_ratio
    where day >= now() - interval '{{Time Period}}'
    group by 1,2,4
) q1
order by day desc, token_symbol desc
-- day, 'Total Balance' as token_symbol,  sum(running_balance) as running_balance,-- sum(running_balance) over (partition by day order by day) as tvl, 'both' as pair,
--100 * sum(running_balance) over (partition by day order by day) /nullif(sum(running_balance) over (partition by day),0) as ratio


