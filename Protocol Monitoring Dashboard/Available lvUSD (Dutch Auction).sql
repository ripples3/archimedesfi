with


--- Token Pairs 
param as (
select * from (values
    (lower('{{Token1}}'), 'lvUSD', 18, 'Pair')
            ) as t (token_address, symbol, decimals, asset_type)
)

-- In and Out of tokens in the Pool
, transfers as (
select day,  sum(amount) as amount 
from 
    (
    select
        date_trunc('day', evt_block_time) as day,
        sum(value/power(10,18)) as amount 
    from  erc20_ethereum.evt_Transfer a
    where contract_address in (select token_address from param)
    and `to` = lower('0x0277785285c5D3013F59d7b5E74B5d65d435544c')
    group by 1

    union all

    select
        date_trunc('day', evt_block_time) as day,
        sum(-value/power(10,18)) as amount
    from  erc20_ethereum.evt_Transfer a
    where contract_address in (select token_address from param)
    and `from` = lower('0x0277785285c5D3013F59d7b5E74B5d65d435544c')
    group by 1
    )a1
    group by 1
)

select day, amount as daily_net_flow, sum(amount) over (order by day) as lvusd_available
from transfers
order by 1 desc
