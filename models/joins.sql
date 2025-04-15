with prod as (
    select
        ct.category_name,
        sp.company_name as suppliers,
        pd.product_name,
        pd.unit_price,
        pd.product_id
    from {{ source('sources', 'products') }} pd
    left join {{ source('sources', 'suppliers') }} sp on (pd.supplier_id = sp.supplier_id)
    left join {{ source('sources', 'categories') }} ct on (pd.category_id = ct.category_id)
),
orderdetails as (
    select
        pd.*,
        od.order_id,
        od.quantity,
        od.discount
    from {{ ref('orderdetails') }} od
    left join prod pd on (od.product_id = pd.product_id)
),
ordrs as (
    select
        ord.order_date,
        ord.order_id,
        cs.company_name as customer,
        ey.name as employee,
        ey.age,
        ey.lengthofservice
    from {{ source('sources', 'orders') }} ord
    left join {{ ref('customers') }} cs on (ord.customer_id = cs.customer_id)
    left join {{ ref('employees') }} ey on (ord.employee_id = ey.employee_id)
    left join {{ source('sources', 'shippers') }} sr on (ord.ship_via = sr.shipper_id)
),
final as (
    select
        od.*,
        o.order_date,
        o.customer,
        o.employee,
        o.age,
        o.lengthofservice
    from orderdetails od
    inner join ordrs o on od.order_id = o.order_id
)

select * from final
