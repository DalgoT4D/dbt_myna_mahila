{{ config(
    materialized='table',
    tags=["medicine_inventory", "2025-2026"]
  ) }}
  
with source as (
    select * from {{ source('staging_medicines', 'August') }}
),

renamed as (
    select
        case 
            when lower(trim("Medicine_Name")) in ('itra 200', 'itrajohn 200', 'legestro-200') then 'itra 200 (itrajohn 200)'
            when lower(trim("Medicine_Name")) like '%itra 100%' then 'itra 100 (itrajohn 100)'
            when lower(trim("Medicine_Name")) = 'trn 500' then 'transemic acid (trn 500)'
            when lower(trim("Medicine_Name")) = 'picosule' then 'picosule (picosule, multivitamin)'
            when lower(trim("Medicine_Name")) = 'zinprovit' then 'mv syrup (zinprovit, multivitamin syrup)'
            when lower(trim("Medicine_Name")) = 'legestro-200' then 'progestrone 200 (legestro-200)'
            when lower(trim("Medicine_Name")) = 'parapic 650' then 'p - 650 (parapic 650, fepadol 650)'
            when lower(trim("Medicine_Name")) = 'omak 20' then 'omee 20 (omak 20)'
            else lower(trim("Medicine_Name"))
        end as medicine,
        '05 - august' as month,
        null::numeric as initial_starting_quantity,
        cast(nullif(trim("Remaining_Qty"), '') as numeric) as monthly_starting_quantity,
        cast(nullif(trim("Remaining_Qty"), '') as numeric) - cast(nullif(trim("Tablet_Stock_Out_1"), '') as numeric) as monthly_ending_quantity,
        cast(nullif(trim("Tablet_Stock_Out_1"), '') as numeric) as total_monthly_liquidation,
        trim("Expiry_Date") as expiry_date,
        trim("Expiry_Status") as expiry_status_in_month
    from source
    where "Medicine_Name" is not null 
      and trim("Medicine_Name") != ''
      and lower("Medicine_Name") not like '%let me know if you want this in a different format%'
)

select * from renamed
