select date_trunc ('day', "hour") as day, 'FRAX' as symbol, avg("median_price") as "FRAX/ETH Price (USD)"
from dex."view_token_prices"
where "contract_address" = '{{Token1}}'
and hour >= now() - interval '{{Time Period}}'
group by 1,2
order by day desc
