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

--select * from daily_price_gap
