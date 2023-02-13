with
curvefi_trades as  (
select        
       evt_block_time AS block_time,
        '' AS version,
        buyer AS taker,
        '' AS maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        NULL AS amount_usd,
        CASE
            WHEN bought_id = 0 THEN lower('0x72dfe359150984C8013105BbbAEe9a152335bD23') -- cealvUSD
            WHEN bought_id = 1 THEN lower('0x6c3F90f043a72FA612cbac8115EE7e52BDe6E490') -- 3Crv
        END as token_bought_address,
        CASE
            WHEN sold_id = 0 THEN lower('0x72dfe359150984C8013105BbbAEe9a152335bD23') -- cealvUSD
            WHEN sold_id = 1 THEN lower('0x6c3F90f043a72FA612cbac8115EE7e52BDe6E490') -- 3Crv
        END as token_sold_address,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        '' AS trace_address,
        evt_index
from curvefi_ethereum.cealv_evt_TokenExchange

union all

select        
       evt_block_time AS block_time,
        '' AS version,
        buyer AS taker,
        '' AS maker,
        tokens_bought AS token_bought_amount_raw,
        tokens_sold AS token_sold_amount_raw,
        NULL AS amount_usd,
        CASE
            WHEN bought_id = 0 THEN lower('0x72dfe359150984C8013105BbbAEe9a152335bD23') -- cealvUSD
            WHEN bought_id = 1 THEN lower('0x6b175474e89094c44da98b954eedeac495271d0f') -- DAI
            WHEN bought_id = 2 THEN lower('0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48') -- USDC
            WHEN bought_id = 3 THEN lower('0xdac17f958d2ee523a2206206994597c13d831ec7') -- USDT
        END as token_bought_address,
        CASE
            WHEN sold_id = 0 THEN lower('0x72dfe359150984C8013105BbbAEe9a152335bD23') -- cealvUSD
            WHEN sold_id = 1 THEN lower('0x6b175474e89094c44da98b954eedeac495271d0f') -- DAI
            WHEN sold_id = 2 THEN lower('0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48') -- USDC
            WHEN sold_id = 3 THEN lower('0xdac17f958d2ee523a2206206994597c13d831ec7') -- USDT
        END as token_sold_address,
        contract_address AS project_contract_address,
        evt_tx_hash AS tx_hash,
        '' AS trace_address,
        evt_index
from curvefi_ethereum.cealv_evt_TokenExchangeUnderlying
)

, temp as (
select block_time, project_contract_address,
case when token_bought_address = lower('0x72dfe359150984C8013105BbbAEe9a152335bD23')  then token_bought_amount_raw/token_sold_amount_raw
     else token_sold_amount_raw/token_bought_amount_raw
end as price
from curvefi_trades
)

, days as (select explode(sequence('2022-10-27'::timestamp, now(), interval '1 day')) as day )-- Generate all days since the creation of the contract

, price_gap_days as (
select *, 'cealvUSD per 3CRV' as remarks, lead(day, 1, now()) over (order by day) as next_day
from (
    select date_trunc('day', block_time) as day, avg(price) as price, 'lvUSD' as symbol
    from temp
    group by 1
     )t1
)

, price_all_days as (
select d.day, symbol as token_symbol, price from price_gap_days b
inner join days d on b.day <= d.day and d.day < b.next_day
order by day desc
)

, threecrv_price as (
select date_trunc('day',minute) as day, symbol, avg(price) as price from prices.usd
where blockchain = 'ethereum'
and contract_address = lower('0x6c3F90f043a72FA612cbac8115EE7e52BDe6E490')
group by 1,2
order by 1 desc
)

select a.day, a.token_symbol, b.price/a.price as price 
from price_all_days a
left join threecrv_price b on a.day = b.day
where a.day >= now() - interval '{{Time Period}}'
order by day desc
