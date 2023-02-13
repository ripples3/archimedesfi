with

dex_trades_a as (
select  *, 	token_a_amount/token_b_amount as price
from        dex."trades" 
where ( "token_b_address" = '\x2A8e1E676Ec238d8A992307B495b45B3fEAa5e86')
and (token_a_symbol = 'USDT' or token_a_symbol = 'DAI' or token_a_symbol = 'USDC')
and category = 'DEX' 
order by 1
)

, dex_trades_b as (
select  *,	token_b_amount/token_a_amount as price
from        dex."trades" 
where ( "token_a_address" = '\x2A8e1E676Ec238d8A992307B495b45B3fEAa5e86')
and (token_b_symbol = 'USDT' or token_b_symbol = 'DAI' or token_b_symbol = 'USDC')
and category = 'DEX' 
order by 1
)
, combined_all_volume as (
select block_time, price, '\x2A8e1E676Ec238d8A992307B495b45B3fEAa5e86' as contract_address  from dex_trades_a
union all
select block_time, price, '\x2A8e1E676Ec238d8A992307B495b45B3fEAa5e86' as contract_address  from dex_trades_b
)

select date_trunc('day',block_time) as day, contract_address, avg(price) as price
from combined_all_volume
where block_time >= now() - interval '{{Time Period}}'
group by 1,2
