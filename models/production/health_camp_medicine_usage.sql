{{ config(
    materialized='table',
    tags=["health_camp", "medicine_usage", "2025-2026"]
) }}

with health_camp_data as (
    select * from {{ ref('stg_health_camp') }}
),

medicine_rows as (
    select visit_date_cleaned, 
           extract(month from visit_date_cleaned) as visit_month,
           extract(year from visit_date_cleaned) as visit_year,
           'ors' as medicine_name, 
           case when ors_given ~ '^[0-9]+$' then ors_given::integer else 0 end as quantity_dispensed
    from health_camp_data 
    where ors_given is not null and trim(ors_given) != '' and visit_date_cleaned is not null
    
    union all
    
    select visit_date_cleaned, 
           extract(month from visit_date_cleaned) as visit_month,
           extract(year from visit_date_cleaned) as visit_year,
           'hyponid' as medicine_name, 
           case when hyponid_medicine ~ '^[0-9]+$' then hyponid_medicine::integer else 0 end as quantity_dispensed
    from health_camp_data 
    where hyponid_medicine is not null and trim(hyponid_medicine) != '' and visit_date_cleaned is not null
    
    union all
    
    select visit_date_cleaned, 
           extract(month from visit_date_cleaned) as visit_month,
           extract(year from visit_date_cleaned) as visit_year,
           'oral medicine' as medicine_name, 
           case when oral_medicine ~ '^[0-9]+$' then oral_medicine::integer else 0 end as quantity_dispensed
    from health_camp_data 
    where oral_medicine is not null and trim(oral_medicine) != '' and visit_date_cleaned is not null
    
    union all
    
    select visit_date_cleaned, 
           extract(month from visit_date_cleaned) as visit_month,
           extract(year from visit_date_cleaned) as visit_year,
           'diclogem' as medicine_name, 
           case when diclogem_medicine ~ '^[0-9]+$' then diclogem_medicine::integer else 0 end as quantity_dispensed
    from health_camp_data 
    where diclogem_medicine is not null and trim(diclogem_medicine) != '' and visit_date_cleaned is not null
    
    union all
    
    select visit_date_cleaned, 
           extract(month from visit_date_cleaned) as visit_month,
           extract(year from visit_date_cleaned) as visit_year,
           'meftadol' as medicine_name, 
           case when meftadol_medicine ~ '^[0-9]+$' then meftadol_medicine::integer else 0 end as quantity_dispensed
    from health_camp_data 
    where meftadol_medicine is not null and trim(meftadol_medicine) != '' and visit_date_cleaned is not null
    
    union all
    
    select visit_date_cleaned, 
           extract(month from visit_date_cleaned) as visit_month,
           extract(year from visit_date_cleaned) as visit_year,
           'clobego gm' as medicine_name, 
           case when clobego_gm_medicine ~ '^[0-9]+$' then clobego_gm_medicine::integer else 0 end as quantity_dispensed
    from health_camp_data 
    where clobego_gm_medicine is not null and trim(clobego_gm_medicine) != '' and visit_date_cleaned is not null
    
    union all
    
    select visit_date_cleaned, 
           extract(month from visit_date_cleaned) as visit_month,
           extract(year from visit_date_cleaned) as visit_year,
           'flucon 150' as medicine_name, 
           case when flucon_150_medicine ~ '^[0-9]+$' then flucon_150_medicine::integer else 0 end as quantity_dispensed
    from health_camp_data 
    where flucon_150_medicine is not null and trim(flucon_150_medicine) != '' and visit_date_cleaned is not null
    
    union all
    
    select visit_date_cleaned, 
           extract(month from visit_date_cleaned) as visit_month,
           extract(year from visit_date_cleaned) as visit_year,
           'ironfast z' as medicine_name, 
           case when ironfast_z_medicine ~ '^[0-9]+$' then ironfast_z_medicine::integer else 0 end as quantity_dispensed
    from health_camp_data 
    where ironfast_z_medicine is not null and trim(ironfast_z_medicine) != '' and visit_date_cleaned is not null
    
    union all
    
    select visit_date_cleaned, 
           extract(month from visit_date_cleaned) as visit_month,
           extract(year from visit_date_cleaned) as visit_year,
           'calcipic 500' as medicine_name, 
           case when calcipic_500_medicine ~ '^[0-9]+$' then calcipic_500_medicine::integer else 0 end as quantity_dispensed
    from health_camp_data 
    where calcipic_500_medicine is not null and trim(calcipic_500_medicine) != '' and visit_date_cleaned is not null
    
    union all
    
    select visit_date_cleaned, 
           extract(month from visit_date_cleaned) as visit_month,
           extract(year from visit_date_cleaned) as visit_year,
           'cazole forte' as medicine_name, 
           case when cazole_forte_medicine ~ '^[0-9]+$' then cazole_forte_medicine::integer else 0 end as quantity_dispensed
    from health_camp_data 
    where cazole_forte_medicine is not null and trim(cazole_forte_medicine) != '' and visit_date_cleaned is not null
    
    union all
    
    select visit_date_cleaned, 
           extract(month from visit_date_cleaned) as visit_month,
           extract(year from visit_date_cleaned) as visit_year,
           'nor tz 200mg' as medicine_name, 
           case when nor_tz_200mg_medicine ~ '^[0-9]+$' then nor_tz_200mg_medicine::integer else 0 end as quantity_dispensed
    from health_camp_data 
    where nor_tz_200mg_medicine is not null and trim(nor_tz_200mg_medicine) != '' and visit_date_cleaned is not null
    
    union all
    
    select visit_date_cleaned, 
           extract(month from visit_date_cleaned) as visit_month,
           extract(year from visit_date_cleaned) as visit_year,
           'doxylac' as medicine_name, 
           case when doxylac_medicine ~ '^[0-9]+$' then doxylac_medicine::integer else 0 end as quantity_dispensed
    from health_camp_data 
    where doxylac_medicine is not null and trim(doxylac_medicine) != '' and visit_date_cleaned is not null
    
    union all
    
    select visit_date_cleaned, 
           extract(month from visit_date_cleaned) as visit_month,
           extract(year from visit_date_cleaned) as visit_year,
           'm2 tone tablets' as medicine_name, 
           case when m2_tone_tablets ~ '^[0-9]+$' then m2_tone_tablets::integer else 0 end as quantity_dispensed
    from health_camp_data 
    where m2_tone_tablets is not null and trim(m2_tone_tablets) != '' and visit_date_cleaned is not null
    
    union all
    
    select visit_date_cleaned, 
           extract(month from visit_date_cleaned) as visit_month,
           extract(year from visit_date_cleaned) as visit_year,
           'omee/omak' as medicine_name, 
           case when omee_omak_medicine ~ '^[0-9]+$' then omee_omak_medicine::integer else 0 end as quantity_dispensed
    from health_camp_data 
    where omee_omak_medicine is not null and trim(omee_omak_medicine) != '' and visit_date_cleaned is not null
    
    union all
    
    select visit_date_cleaned, 
           extract(month from visit_date_cleaned) as visit_month,
           extract(year from visit_date_cleaned) as visit_year,
           'itra 100' as medicine_name, 
           case when itra_100_medicine ~ '^[0-9]+$' then itra_100_medicine::integer else 0 end as quantity_dispensed
    from health_camp_data 
    where itra_100_medicine is not null and trim(itra_100_medicine) != '' and visit_date_cleaned is not null
    
    union all
    
    select visit_date_cleaned, 
           extract(month from visit_date_cleaned) as visit_month,
           extract(year from visit_date_cleaned) as visit_year,
           'itra 200' as medicine_name, 
           case when itra_200_medicine ~ '^[0-9]+$' then itra_200_medicine::integer else 0 end as quantity_dispensed
    from health_camp_data 
    where itra_200_medicine is not null and trim(itra_200_medicine) != '' and visit_date_cleaned is not null
    
    union all
    
    select visit_date_cleaned, 
           extract(month from visit_date_cleaned) as visit_month,
           extract(year from visit_date_cleaned) as visit_year,
           'tranexamic acid' as medicine_name, 
           case when tranexamic_acid_medicine ~ '^[0-9]+$' then tranexamic_acid_medicine::integer else 0 end as quantity_dispensed
    from health_camp_data 
    where tranexamic_acid_medicine is not null and trim(tranexamic_acid_medicine) != '' and visit_date_cleaned is not null
    
    union all
    
    select visit_date_cleaned, 
           extract(month from visit_date_cleaned) as visit_month,
           extract(year from visit_date_cleaned) as visit_year,
           'letroquin' as medicine_name, 
           case when letroquin_medicine ~ '^[0-9]+$' then letroquin_medicine::integer else 0 end as quantity_dispensed
    from health_camp_data 
    where letroquin_medicine is not null and trim(letroquin_medicine) != '' and visit_date_cleaned is not null
    
    union all
    
    select visit_date_cleaned, 
           extract(month from visit_date_cleaned) as visit_month,
           extract(year from visit_date_cleaned) as visit_year,
           'levocet' as medicine_name, 
           case when levocet_medicine ~ '^[0-9]+$' then levocet_medicine::integer else 0 end as quantity_dispensed
    from health_camp_data 
    where levocet_medicine is not null and trim(levocet_medicine) != '' and visit_date_cleaned is not null
    
    union all
    
    select visit_date_cleaned, 
           extract(month from visit_date_cleaned) as visit_month,
           extract(year from visit_date_cleaned) as visit_year,
           'azinetra kit' as medicine_name, 
           case when azinetra_kit_medicine ~ '^[0-9]+$' then azinetra_kit_medicine::integer else 0 end as quantity_dispensed
    from health_camp_data 
    where azinetra_kit_medicine is not null and trim(azinetra_kit_medicine) != '' and visit_date_cleaned is not null
    
    union all
    
    select visit_date_cleaned, 
           extract(month from visit_date_cleaned) as visit_month,
           extract(year from visit_date_cleaned) as visit_year,
           'progesterone' as medicine_name, 
           case when progesterone_medicine ~ '^[0-9]+$' then progesterone_medicine::integer else 0 end as quantity_dispensed
    from health_camp_data 
    where progesterone_medicine is not null and trim(progesterone_medicine) != '' and visit_date_cleaned is not null
    
    union all
    
    select visit_date_cleaned, 
           extract(month from visit_date_cleaned) as visit_month,
           extract(year from visit_date_cleaned) as visit_year,
           'multivitamin' as medicine_name, 
           case when multivitamin_medicine ~ '^[0-9]+$' then multivitamin_medicine::integer else 0 end as quantity_dispensed
    from health_camp_data 
    where multivitamin_medicine is not null and trim(multivitamin_medicine) != '' and visit_date_cleaned is not null
    
    union all
    
    select visit_date_cleaned, 
           extract(month from visit_date_cleaned) as visit_month,
           extract(year from visit_date_cleaned) as visit_year,
           'paracetamol 650' as medicine_name, 
           case when paracetamol_650_medicine ~ '^[0-9]+$' then paracetamol_650_medicine::integer else 0 end as quantity_dispensed
    from health_camp_data 
    where paracetamol_650_medicine is not null and trim(paracetamol_650_medicine) != '' and visit_date_cleaned is not null
    
    union all
    
    select visit_date_cleaned, 
           extract(month from visit_date_cleaned) as visit_month,
           extract(year from visit_date_cleaned) as visit_year,
           'multivitamin syrup' as medicine_name, 
           case when multivitamin_syrup ~ '^[0-9]+$' then multivitamin_syrup::integer else 0 end as quantity_dispensed
    from health_camp_data 
    where multivitamin_syrup is not null and trim(multivitamin_syrup) != '' and visit_date_cleaned is not null
),

monthly_aggregated as (
    select
        visit_month,
        visit_year,
        case 
            when lower(trim(medicine_name)) = 'levocet' then 'levophine'
            when lower(trim(medicine_name)) = 'paracetamol 650' then 'p - 650 (parapic 650, fepadol 650)'
            when lower(trim(medicine_name)) = 'm2 tone tablets' then 'm2 -tone tablets'
            when lower(trim(medicine_name)) = 'multivitamin' then 'picosule (picosule, multivitamin)'
            when lower(trim(medicine_name)) = 'oral medicine' then 'oral -l'
            when lower(trim(medicine_name)) = 'omee/omak' then 'omee 20 (omak 20)'
            when lower(trim(medicine_name)) = 'letroquin' then 'letsi 2.5 (letroquin 2.5)'
            when lower(trim(medicine_name)) = 'tranexamic acid' then 'transemic acid (trn 500)'
            when lower(trim(medicine_name)) = 'progesterone' then 'progestrone 200 (legestro-200)'
            when lower(trim(medicine_name)) = 'doxylac' then 'doxy (doxylac-b)'
            when lower(trim(medicine_name)) = 'multivitamin syrup' then 'mv syrup (zinprovit, multivitamin syrup)'
            when lower(trim(medicine_name)) = 'itra 100' then 'itra 100 (itrajohn 100)'
            when lower(trim(medicine_name)) = 'itra 200' then 'itra 200 (itrajohn 200)'
            else lower(trim(medicine_name))
        end as medicine,
        sum(quantity_dispensed) as camp_liquidation,
        case 
            when visit_month = 4 then 1
            when visit_month = 5 then 2
            when visit_month = 6 then 3
            when visit_month = 7 then 4
            when visit_month = 8 then 5
            when visit_month = 9 then 6
            when visit_month = 10 then 7
            when visit_month = 11 then 8
            when visit_month = 12 then 9
            when visit_month = 1 then 10
            when visit_month = 2 then 11
            when visit_month = 3 then 12
        end as month_num,
        case 
            when visit_month = 4 then '01 - april'
            when visit_month = 5 then '02 - may'
            when visit_month = 6 then '03 - june'
            when visit_month = 7 then '04 - july'
            when visit_month = 8 then '05 - august'
            when visit_month = 9 then '06 - september'
            when visit_month = 10 then '07 - october'
            when visit_month = 11 then '08 - november'
            when visit_month = 12 then '09 - december'
            when visit_month = 1 then '10 - january'
            when visit_month = 2 then '11 - february'
            when visit_month = 3 then '12 - march'
        end as month_label
    from medicine_rows
    where visit_month is not null 
      and medicine_name is not null
      and quantity_dispensed > 0
    group by visit_month, visit_year, medicine_name
)

select 
    medicine,
    month_label as month,
    month_num,
    camp_liquidation
from monthly_aggregated
order by month_num, medicine