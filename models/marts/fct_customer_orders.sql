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
)


-- Logic
, customer_order_history as 
(
    select 
        b.customer_id,
        b.full_name,
        b.surname,
        b.givenname,
        min(order_date) as first_order_date,
        min(case when a.order_status not in ('returned','return_pending') then order_date end) as first_non_returned_order_date,
        max(case when a.order_status not in ('returned','return_pending') then order_date end) as most_recent_non_returned_order_date,
        coalesce(max(user_order_seq),0) as order_count,
        coalesce(count(case when a.order_status != 'returned' then 1 end),0) as non_returned_order_count,
        sum
            (
                case
                    when a.order_status not in ('returned','return_pending')
                    then c.payment_amount else 0
                end
            ) as total_lifetime_value,
        sum
            (
                case
                    when a.order_status not in ('returned','return_pending')
                    then c.payment_amount else 0
                end
            )
            /
            nullif(count
            (
                case
                    when a.order_status not in ('returned','return_pending')
                    then 1
                end
            ),0) as avg_non_returned_order_value,
        array_agg(distinct a.order_id) as order_ids
    from orders as a
    join customers as b
        on a.customer_id = b.customer_id
    left join payments c
        on a.order_id = c.order_id
    where a.order_status not in ('pending')
        and c.payment_status != 'fail'
    group by b.customer_id, b.full_name, b.surname, b.givenname
)


-- Final select
select 
    orders.order_id,
    orders.customer_id,
    customers.surname,
    customers.givenname,
    first_order_date,
    order_count,
    total_lifetime_value,
    payment_amount as order_value_dollars,
    orders.order_status,
    payments.payment_status
from orders as orders
join customers
    on orders.customer_id = customers.customer_id
join customer_order_history
    on orders.customer_id = customer_order_history.customer_id
left join  payments
    on orders.order_id = payments.order_id
where payments.payment_status != 'fail'
