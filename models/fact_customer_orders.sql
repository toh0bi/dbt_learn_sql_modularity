WITH 
    paid_orders as (
        select 
            orders.ID as order_id,
            orders.user_id	as customer_id,
            orders.order_date AS order_placed_at,
            orders.status AS order_status,
            p.total_amount_paid,
            p.payment_finalized_date,
            C.first_name as customer_first_name,
            C.last_name as customer_last_name
        FROM test_db.suto_dbt_learn_modularity.orders as orders
        left join 
            (select 
                ORDERID as order_id, max(CREATED) as payment_finalized_date, sum(AMOUNT) / 100.0 as total_amount_paid
                from test_db.suto_dbt_learn_modularity.payments
                where status <> 'fail'
                group by 1
            ) p ON orders.ID = p.order_id
        left join test_db.suto_dbt_learn_modularity.customers C on orders.user_id = C.ID )
    , customer_orders as (
        select 
            C.ID as customer_id
            , min(order_date) as first_order_date
            , max(order_date) as most_recent_order_date
            , count(ORDERS.ID) AS number_of_orders
        from test_db.suto_dbt_learn_modularity.customers C 
        left join test_db.suto_dbt_learn_modularity.orders as orders
        on orders.user_id = C.ID 
        group by 1)
select
    p.*,
    ROW_NUMBER() OVER (ORDER BY p.order_id) as transaction_seq,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY p.order_id) as customer_sales_seq,
    CASE WHEN c.first_order_date = p.order_placed_at
        THEN 'new'
        ELSE 'return' 
        END as nvsr,
    x.clv_bad as customer_lifetime_value,
    c.first_order_date as fdos
FROM paid_orders p
left join customer_orders as c USING (customer_id)
LEFT OUTER JOIN 
(
    select
        p.order_id,
        sum(t2.total_amount_paid) as clv_bad
    from paid_orders p
    left join paid_orders t2 on p.customer_id = t2.customer_id and p.order_id >= t2.order_id
    group by 1
    order by p.order_id
) x on x.order_id = p.order_id
ORDER BY order_id