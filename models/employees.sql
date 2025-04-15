-- models/employees_with_name.sql

with base as (
    select
        *,
        floor( (datediff(day, birth_date, current_date)) / 365.25 )::int as age,
        floor( (datediff(day, hire_date, current_date)) / 365.25 )::int as LengthofService,
        last_name || ' ' || first_name as Name
    from {{ source('sources', 'employees') }}
)

select * from base
