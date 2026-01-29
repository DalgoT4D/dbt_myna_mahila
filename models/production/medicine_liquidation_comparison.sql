{{ config(
    materialized='table',
    tags=["medicine_liquidation", "2025-2026"]
  ) }}

with inventory as (
    select
        lower(trim(medicine)) as medicine,
        month,
        month_num,
        sum(total_monthly_liquidation) as inventory_liquidation
    from {{ ref('medicines_consolidated') }}
    group by 1, 2, 3
),

clinic as (
    select
        lower(trim(medicine_name)) as medicine,
        month_formatted,
        case 
            when lower(month_formatted) like '%april%' then 1
            when lower(month_formatted) like '%may%' then 2
            when lower(month_formatted) like '%june%' then 3
            when lower(month_formatted) like '%july%' then 4
            when lower(month_formatted) like '%august%' then 5
            when lower(month_formatted) like '%september%' then 6
            when lower(month_formatted) like '%october%' then 7
            when lower(month_formatted) like '%november%' then 8
            when lower(month_formatted) like '%december%' then 9
            when lower(month_formatted) like '%january%' then 10
            when lower(month_formatted) like '%february%' then 11
            when lower(month_formatted) like '%march%' then 12
        end as month_num,
        sum(total_pills_prescribed) as clinic_liquidation
    from {{ ref('medicine_prescriptions_by_month') }}
    where total_pills_prescribed is not null
    group by 1, 2, 3
),

health_camp as (
    select
        medicine,
        month,
        month_num,
        sum(camp_liquidation) as camp_liquidation
    from {{ ref('health_camp_medicine_usage') }}
    group by 1, 2, 3
),

clinic_with_labels as (
    select
        medicine,
        clinic_liquidation,
        month_num,
        case 
            when month_num = 1 then '01 - april'
            when month_num = 2 then '02 - may'
            when month_num = 3 then '03 - june'
            when month_num = 4 then '04 - july'
            when month_num = 5 then '05 - august'
            when month_num = 6 then '06 - september'
            when month_num = 7 then '07 - october'
            when month_num = 8 then '08 - november'
            when month_num = 9 then '09 - december'
            when month_num = 10 then '10 - january'
            when month_num = 11 then '11 - february'
            when month_num = 12 then '12 - march'
            else month_formatted
        end as month_label
    from clinic
),

combined as (
    select
        coalesce(i.month, c.month_label, h.month) as month,
        coalesce(i.medicine, c.medicine, h.medicine) as medicine,
        c.clinic_liquidation,
        h.camp_liquidation,
        i.inventory_liquidation,
        coalesce(i.month_num, c.month_num, h.month_num) as month_num
    from inventory i
    full outer join clinic_with_labels c
        on i.medicine = c.medicine
       and i.month_num = c.month_num
    full outer join health_camp h
        on coalesce(i.medicine, c.medicine) = h.medicine
       and coalesce(i.month_num, c.month_num) = h.month_num
)

select
    month,
    medicine,
    clinic_liquidation,
    camp_liquidation,
    inventory_liquidation,
    coalesce(clinic_liquidation, 0) + coalesce(camp_liquidation, 0) as total_usage_liquidation,
    case 
        when inventory_liquidation > 0 
        then (coalesce(clinic_liquidation, 0) + coalesce(camp_liquidation, 0)) * 100.0 / inventory_liquidation
        else null
    end as usage_vs_inventory_percentage
from combined
order by month_num, medicine
