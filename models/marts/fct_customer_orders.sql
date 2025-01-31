-- Import CTEs
with customers as 
(
    select
        *
    from {{ ref('stg_jaffle_shop__customers') }}
)

, orders as 
(
    select
        *
    from {{ ref('stg_jaffle_shop__orders') }}
)

, payments as 
(
    select
        *
    from {{ ref('stg_stripe__payments') }}
    where payment_status <> 'fail'
)


-- Logic
, order_totals as 
(
    select
        order_id
        , payment_status
        , sum(payment_amount) as order_value_dollars
    from payments
    group by 1,2
)

, order_values as 
(
    select
        o.*
        , ot.payment_status
        , ot.order_value_dollars
    from orders o
    left join order_totals ot
        on ot.order_id = o.order_id
)

, customer_order_history as 
(
    select 
        customer_id
        , min(order_date) as first_order_date
        , min(valid_order_date) as first_non_returned_order_date
        , max(valid_order_date) as most_recent_non_returned_order_date
        , coalesce(max(user_order_seq),0) as order_count
        , coalesce(count(case when valid_order_date is not null then 1 end),0) as non_returned_order_count
        , sum
            (
                case
                    when valid_order_date is not null
                    then order_value_dollars else 0
                end
            ) as total_lifetime_value     
        , array_agg(distinct o.order_id) as order_ids
    from order_values as o
    group by 1
)


-- Final select
select 
    o.order_id
    , o.customer_id
    , c.surname
    , c.givenname
    , coh.first_order_date
    , coh.order_count
    , coh.total_lifetime_value
    , o.order_value_dollars
    , o.order_status
    , o.payment_status
from order_values as o
join customers c
    on o.customer_id = c.customer_id
join customer_order_history coh
    on c.customer_id = coh.customer_id
