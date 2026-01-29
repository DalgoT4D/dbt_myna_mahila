{{ config(
    materialized='table',
    tags=["medicine_inventory", "2025-2026"]
  ) }}


with all_months as (
    select * from {{ ref('stg_april') }}
    union all
    select * from {{ ref('stg_may') }}
    union all
    select * from {{ ref('stg_june') }}
    union all
    select * from {{ ref('stg_july') }}
    union all
    select * from {{ ref('stg_august') }}
    union all
    select * from {{ ref('stg_september') }}
    union all
    select * from {{ ref('stg_october') }}
    union all
    select * from {{ ref('stg_november') }}
    union all
    select * from {{ ref('stg_december') }}
    union all
    select * from {{ ref('stg_january') }}
),

normalized_expiry_dates as (
    select
        medicine,
        month,
        initial_starting_quantity,
        monthly_starting_quantity,
        monthly_ending_quantity,
        total_monthly_liquidation,
        expiry_date,
        expiry_status_in_month,
        case
            when expiry_date ~ '^\d{1,2}-[A-Za-z]{3}-\d{4}$' then
                to_date(expiry_date, 'DD-Mon-YYYY')
            when expiry_date ~ '^\d{1,2}-\d{2}-\d{2}$' then
                to_date('20' || right(expiry_date, 2) || '-' || 
                        substring(expiry_date, 4, 2) || '-' || 
                        left(expiry_date, 2), 'YYYY-MM-DD')
            else null
        end as expiry_date_parsed,
        case 
            when month = '01 - april' then 1
            when month = '02 - may' then 2
            when month = '03 - june' then 3
            when month = '04 - july' then 4
            when month = '05 - august' then 5
            when month = '06 - september' then 6
            when month = '07 - october' then 7
            when month = '08 - november' then 8
            when month = '09 - december' then 9
            when month = '10 - january' then 10
        end as month_num
    from all_months
),

medicine_master_data as (
    select 
        medicine,
        min(case 
            when monthly_starting_quantity is not null 
            then monthly_starting_quantity 
        end) as initial_starting_quantity,
        min(case 
            when monthly_starting_quantity is not null 
            then month_num 
        end) as earliest_month_with_qty,
        min(case when expiry_date is not null and trim(expiry_date) != '' then expiry_date end) as master_expiry_date,
        min(case when expiry_date is not null and trim(expiry_date) != '' then expiry_date_parsed end) as master_expiry_date_parsed
    from normalized_expiry_dates
    group by medicine
),

with_initial_data as (
    select 
        n.medicine,
        n.month,
        n.month_num,
        m.initial_starting_quantity,
        case 
            when m.earliest_month_with_qty = 1 then '01 - april'
            when m.earliest_month_with_qty = 2 then '02 - may'
            when m.earliest_month_with_qty = 3 then '03 - june'
            when m.earliest_month_with_qty = 4 then '04 - july'
            when m.earliest_month_with_qty = 5 then '05 - august'
            when m.earliest_month_with_qty = 6 then '06 - september'
            when m.earliest_month_with_qty = 7 then '07 - october'
            when m.earliest_month_with_qty = 8 then '08 - november'
            when m.earliest_month_with_qty = 9 then '09 - december'
            when m.earliest_month_with_qty = 10 then '10 - january'
        end as initial_starting_month,
        n.monthly_starting_quantity,
        n.monthly_ending_quantity,
        n.total_monthly_liquidation,
        m.master_expiry_date as expiry_date,
        m.master_expiry_date_parsed as expiry_date_parsed
    from normalized_expiry_dates n
    left join medicine_master_data m on n.medicine = m.medicine
),

final_table as (
    select
        medicine,
        month,
        month_num,
        initial_starting_quantity,
        initial_starting_month,
        monthly_starting_quantity,
        monthly_ending_quantity,
        total_monthly_liquidation,
        expiry_date,
        case 
            when expiry_date_parsed is null then 'unknown'
            when expiry_date_parsed < date_trunc('month', make_date(
                case when month_num = 10 then 2026 else 2025 end, 
                case when month_num = 10 then 1 else month_num + 3 end, 1)) then 'expired'
            when expiry_date_parsed <= date_trunc('month', make_date(
                case when month_num = 10 then 2026 else 2025 end, 
                case when month_num = 10 then 1 else month_num + 3 end, 1)) + interval '3 months' then 'short expiry'
            when expiry_date_parsed <= date_trunc('month', make_date(
                case when month_num = 10 then 2026 else 2025 end, 
                case when month_num = 10 then 1 else month_num + 3 end, 1)) + interval '9 months' then 'medium expiry'
            else 'long expiry'
        end as expiry_status_in_month,
        case 
            when expiry_date_parsed is null then 'unknown'
            when expiry_date_parsed < current_date then 'expired'
            when expiry_date_parsed <= current_date + interval '3 months' then 'short expiry'
            when expiry_date_parsed <= current_date + interval '9 months' then 'medium expiry'
            else 'long expiry'
        end as current_expiration_status
    from with_initial_data
)

select 
    medicine,
    month,
    month_num,
    initial_starting_quantity,
    initial_starting_month,
    sum(monthly_starting_quantity) as monthly_starting_quantity,
    sum(monthly_ending_quantity) as monthly_ending_quantity,
    sum(total_monthly_liquidation) as total_monthly_liquidation,
    expiry_date,
    expiry_status_in_month,
    current_expiration_status
from final_table
where month_num >= (
    case 
        when initial_starting_month = '01 - april' then 1
        when initial_starting_month = '02 - may' then 2
        when initial_starting_month = '03 - june' then 3
        when initial_starting_month = '04 - july' then 4
        when initial_starting_month = '05 - august' then 5
        when initial_starting_month = '06 - september' then 6
        when initial_starting_month = '07 - october' then 7
        when initial_starting_month = '08 - november' then 8
        when initial_starting_month = '09 - december' then 9
        when initial_starting_month = '10 - january' then 10
    end
)
group by medicine, month, month_num, initial_starting_quantity, initial_starting_month, expiry_date, expiry_status_in_month, current_expiration_status
order by medicine, month_num