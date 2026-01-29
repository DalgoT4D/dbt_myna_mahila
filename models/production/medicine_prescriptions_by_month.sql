{{ config(
    materialized='table',
    tags=["medicine_prescriptions", "2025-2026"]
  ) }}

with form_data as (
    select * from {{ ref('stg_form_responses') }}
),

medicine_unpivoted as (
    select 
        month_formatted,
        "Myna_UID_",
        medicine_name,
        pills_prescribed
    from (
        select month_formatted, "Myna_UID_", 'ors' as medicine_name, 
               case when trim("ORS") ~ '^[0-9]+(\.[0-9]+)?$' then cast(trim("ORS") as numeric) else null end as pills_prescribed
        from form_data where "ORS" is not null and trim("ORS") != '' and trim("ORS") != 'No'
        union all
        select month_formatted, "Myna_UID_", 'nipple cream' as medicine_name, 
               case when trim("Nipple") ~ '^[0-9]+(\.[0-9]+)?$' then cast(trim("Nipple") as numeric) else null end
        from form_data where "Nipple" is not null and trim("Nipple") != '' and trim("Nipple") != 'No'
        union all
        select month_formatted, "Myna_UID_", 'hyponid' as medicine_name, 
               case when trim("Hyponid") ~ '^[0-9]+(\.[0-9]+)?$' then cast(trim("Hyponid") as numeric) else null end
        from form_data where "Hyponid" is not null and trim("Hyponid") != '' and trim("Hyponid") != 'No'
        union all
        select month_formatted, "Myna_UID_", 'oral -l' as medicine_name, 
               case when trim("Oral__L") ~ '^[0-9]+(\.[0-9]+)?$' then cast(trim("Oral__L") as numeric) else null end
        from form_data where "Oral__L" is not null and trim("Oral__L") != '' and trim("Oral__L") != 'No'
        union all
        select month_formatted, "Myna_UID_", 'azito fs' as medicine_name, 
               case when trim("Azito_FS") ~ '^[0-9]+(\.[0-9]+)?$' then cast(trim("Azito_FS") as numeric) else null end
        from form_data where "Azito_FS" is not null and trim("Azito_FS") != '' and trim("Azito_FS") != 'No'
        union all
        select month_formatted, "Myna_UID_", 'diclogem' as medicine_name, 
               case when trim("Diclogem") ~ '^[0-9]+(\.[0-9]+)?$' then cast(trim("Diclogem") as numeric) else null end
        from form_data where "Diclogem" is not null and trim("Diclogem") != '' and trim("Diclogem") != 'No'
        union all
        select month_formatted, "Myna_UID_", 'meftadol' as medicine_name, 
               case when trim("Meftadol") ~ '^[0-9]+(\.[0-9]+)?$' then cast(trim("Meftadol") as numeric) else null end
        from form_data where "Meftadol" is not null and trim("Meftadol") != '' and trim("Meftadol") != 'No'
        union all
        select month_formatted, "Myna_UID_", 'clobego gm' as medicine_name, 
               case when trim("Clobego_GM") ~ '^[0-9]+(\.[0-9]+)?$' then cast(trim("Clobego_GM") as numeric) else null end
        from form_data where "Clobego_GM" is not null and trim("Clobego_GM") != '' and trim("Clobego_GM") != 'No'
        union all
        select month_formatted, "Myna_UID_", 'flucon 150' as medicine_name, 
               case when trim("Flucon_150") ~ '^[0-9]+(\.[0-9]+)?$' then cast(trim("Flucon_150") as numeric) else null end
        from form_data where "Flucon_150" is not null and trim("Flucon_150") != '' and trim("Flucon_150") != 'No'
        union all
        select month_formatted, "Myna_UID_", 'ironfast z' as medicine_name, 
               case when trim("Ironfast_Z") ~ '^[0-9]+(\.[0-9]+)?$' then cast(trim("Ironfast_Z") as numeric) else null end
        from form_data where "Ironfast_Z" is not null and trim("Ironfast_Z") != '' and trim("Ironfast_Z") != 'No'
        union all
        select month_formatted, "Myna_UID_", 'azinetra kit' as medicine_name, 
               case when trim("Azinetra_Kit") ~ '^[0-9]+(\.[0-9]+)?$' then cast(trim("Azinetra_Kit") as numeric) else null end
        from form_data where "Azinetra_Kit" is not null and trim("Azinetra_Kit") != '' and trim("Azinetra_Kit") != 'No'
        union all
        select month_formatted, "Myna_UID_", 'calcipic-500' as medicine_name, 
               case when trim("Calcipic_500") ~ '^[0-9]+(\.[0-9]+)?$' then cast(trim("Calcipic_500") as numeric) else null end
        from form_data where "Calcipic_500" is not null and trim("Calcipic_500") != '' and trim("Calcipic_500") != 'No'
        union all
        select month_formatted, "Myna_UID_", 'cazole forte' as medicine_name, 
               case when trim("Cazole_Forte") ~ '^[0-9]+(\.[0-9]+)?$' then cast(trim("Cazole_Forte") as numeric) else null end
        from form_data where "Cazole_Forte" is not null and trim("Cazole_Forte") != '' and trim("Cazole_Forte") != 'No'
        union all
        select month_formatted, "Myna_UID_", 'nor tz 200mg' as medicine_name, 
               case when trim("Nor_TZ_200mg") ~ '^[0-9]+(\.[0-9]+)?$' then cast(trim("Nor_TZ_200mg") as numeric) else null end
        from form_data where "Nor_TZ_200mg" is not null and trim("Nor_TZ_200mg") != '' and trim("Nor_TZ_200mg") != 'No'
        union all
        select month_formatted, "Myna_UID_", 'doxy (doxylac-b)' as medicine_name, 
               case when trim("Doxy__Doxylac_B_") ~ '^[0-9]+(\.[0-9]+)?$' then cast(trim("Doxy__Doxylac_B_") as numeric) else null end
        from form_data where "Doxy__Doxylac_B_" is not null and trim("Doxy__Doxylac_B_") != '' and trim("Doxy__Doxylac_B_") != 'No'
        union all
        select month_formatted, "Myna_UID_", 'm2 -tone tablets' as medicine_name, 
               case when trim("M2__Tone_Tablets") ~ '^[0-9]+(\.[0-9]+)?$' then cast(trim("M2__Tone_Tablets") as numeric) else null end
        from form_data where "M2__Tone_Tablets" is not null and trim("M2__Tone_Tablets") != '' and trim("M2__Tone_Tablets") != 'No'
        union all
        select month_formatted, "Myna_UID_", 'omee 20 (omak 20)' as medicine_name, 
               case when trim("Omee_20__Omak_20_") ~ '^[0-9]+(\.[0-9]+)?$' then cast(trim("Omee_20__Omak_20_") as numeric) else null end
        from form_data where "Omee_20__Omak_20_" is not null and trim("Omee_20__Omak_20_") != '' and trim("Omee_20__Omak_20_") != 'No'
        union all
        select month_formatted, "Myna_UID_", 'itra 100 (itrajohn 100)' as medicine_name, 
               case when trim("Itra_100__Itrajohn_100_") ~ '^[0-9]+(\.[0-9]+)?$' then cast(trim("Itra_100__Itrajohn_100_") as numeric) else null end
        from form_data where "Itra_100__Itrajohn_100_" is not null and trim("Itra_100__Itrajohn_100_") != '' and trim("Itra_100__Itrajohn_100_") != 'No'
        union all
        select month_formatted, "Myna_UID_", 'itra 200 (itrajohn 200)' as medicine_name, 
               case when trim("Itra_200__Itrajohn_200_") ~ '^[0-9]+(\.[0-9]+)?$' then cast(trim("Itra_200__Itrajohn_200_") as numeric) else null end
        from form_data where "Itra_200__Itrajohn_200_" is not null and trim("Itra_200__Itrajohn_200_") != '' and trim("Itra_200__Itrajohn_200_") != 'No'
        union all
        select month_formatted, "Myna_UID_", 'transemic acid (trn 500)' as medicine_name, 
               case when trim("Transemic_acid__TRN_500_") ~ '^[0-9]+(\.[0-9]+)?$' then cast(trim("Transemic_acid__TRN_500_") as numeric) else null end
        from form_data where "Transemic_acid__TRN_500_" is not null and trim("Transemic_acid__TRN_500_") != '' and trim("Transemic_acid__TRN_500_") != 'No'
        union all
        select month_formatted, "Myna_UID_", 'letsi 2.5 (letroquin 2.5)' as medicine_name, 
               case when trim("Letsi_2_5__Letroquin_2_5_") ~ '^[0-9]+(\.[0-9]+)?$' then cast(trim("Letsi_2_5__Letroquin_2_5_") as numeric) else null end
        from form_data where "Letsi_2_5__Letroquin_2_5_" is not null and trim("Letsi_2_5__Letroquin_2_5_") != '' and trim("Letsi_2_5__Letroquin_2_5_") != 'No'
        union all
        select month_formatted, "Myna_UID_", 'levophine' as medicine_name, 
               case when trim("Levocet__AZINE_L__Levophine_") ~ '^[0-9]+(\.[0-9]+)?$' then cast(trim("Levocet__AZINE_L__Levophine_") as numeric) else null end
        from form_data where "Levocet__AZINE_L__Levophine_" is not null and trim("Levocet__AZINE_L__Levophine_") != '' and trim("Levocet__AZINE_L__Levophine_") != 'No'
        union all
        select month_formatted, "Myna_UID_", 'azinetra kit' as medicine_name, 
               case when trim("3_Kit__Azinetra_Kit__Azito_FS_") ~ '^[0-9]+(\.[0-9]+)?$' then cast(trim("3_Kit__Azinetra_Kit__Azito_FS_") as numeric) else null end
        from form_data where "3_Kit__Azinetra_Kit__Azito_FS_" is not null and trim("3_Kit__Azinetra_Kit__Azito_FS_") != '' and trim("3_Kit__Azinetra_Kit__Azito_FS_") != 'No'
        union all
        select month_formatted, "Myna_UID_", 'progestrone 200 (legestro-200)' as medicine_name, 
               case when trim("Progestrone_200__Legestro_200_") ~ '^[0-9]+(\.[0-9]+)?$' then cast(trim("Progestrone_200__Legestro_200_") as numeric) else null end
        from form_data where "Progestrone_200__Legestro_200_" is not null and trim("Progestrone_200__Legestro_200_") != '' and trim("Progestrone_200__Legestro_200_") != 'No'
        union all
        select month_formatted, "Myna_UID_", 'picosule (picosule, multivitamin)' as medicine_name, 
               case when trim("Picosule__Picosule__Multivitamin_") ~ '^[0-9]+(\.[0-9]+)?$' then cast(trim("Picosule__Picosule__Multivitamin_") as numeric) else null end
        from form_data where "Picosule__Picosule__Multivitamin_" is not null and trim("Picosule__Picosule__Multivitamin_") != '' and trim("Picosule__Picosule__Multivitamin_") != 'No'
        union all
        select month_formatted, "Myna_UID_", 'p - 650 (parapic 650, fepadol 650)' as medicine_name, 
               case when trim("P___650__Parapic_650__Fepadol_650_") ~ '^[0-9]+(\.[0-9]+)?$' then cast(trim("P___650__Parapic_650__Fepadol_650_") as numeric) else null end
        from form_data where "P___650__Parapic_650__Fepadol_650_" is not null and trim("P___650__Parapic_650__Fepadol_650_") != '' and trim("P___650__Parapic_650__Fepadol_650_") != 'No'
        union all
        select month_formatted, "Myna_UID_", 'mv syrup (zinprovit, multivitamin syrup)' as medicine_name, 
               case when trim("MV_Syrup__Zinprovit__Multivitamin_Syrup_") ~ '^[0-9]+(\.[0-9]+)?$' then cast(trim("MV_Syrup__Zinprovit__Multivitamin_Syrup_") as numeric) else null end
        from form_data where "MV_Syrup__Zinprovit__Multivitamin_Syrup_" is not null and trim("MV_Syrup__Zinprovit__Multivitamin_Syrup_") != '' and trim("MV_Syrup__Zinprovit__Multivitamin_Syrup_") != 'No'
    ) med_data
)

select
    month_formatted,
    medicine_name,
    sum(pills_prescribed) as total_pills_prescribed,
    count(distinct "Myna_UID_") as unique_patients
from medicine_unpivoted
where pills_prescribed > 0
group by month_formatted, medicine_name
order by 
    case 
        when month_formatted like '%april%' then 1
        when month_formatted like '%may%' then 2
        when month_formatted like '%june%' then 3
        when month_formatted like '%july%' then 4
        when month_formatted like '%august%' then 5
        when month_formatted like '%september%' then 6
        when month_formatted like '%october%' then 7
        when month_formatted like '%november%' then 8
        when month_formatted like '%december%' then 9
        when month_formatted like '%january%' then 10
        when month_formatted like '%february%' then 11
        when month_formatted like '%march%' then 12
        else 99
    end, medicine_name