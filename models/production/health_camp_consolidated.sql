{{ config(
    materialized='table',
    tags=["health_camp", "2025-2026"]
) }}

with health_camp_data as (
    select * from {{ ref('stg_health_camp') }}
),

medicine_dispensed as (
    select 
        airbyte_id,
        patient_name,
        myna_patient_id,
        visit_date_cleaned,
        camp_location,
        doctor_name,
        chief_complaint,
        doctor_diagnosis,
        
        case when ors_given is not null and trim(ors_given) != '' then 1 else 0 end +
        case when hyponid_medicine is not null and trim(hyponid_medicine) != '' then 1 else 0 end +
        case when oral_medicine is not null and trim(oral_medicine) != '' then 1 else 0 end +
        case when diclogem_medicine is not null and trim(diclogem_medicine) != '' then 1 else 0 end +
        case when meftadol_medicine is not null and trim(meftadol_medicine) != '' then 1 else 0 end +
        case when clobego_gm_medicine is not null and trim(clobego_gm_medicine) != '' then 1 else 0 end +
        case when flucon_150_medicine is not null and trim(flucon_150_medicine) != '' then 1 else 0 end +
        case when ironfast_z_medicine is not null and trim(ironfast_z_medicine) != '' then 1 else 0 end +
        case when calcipic_500_medicine is not null and trim(calcipic_500_medicine) != '' then 1 else 0 end +
        case when cazole_forte_medicine is not null and trim(cazole_forte_medicine) != '' then 1 else 0 end +
        case when nor_tz_200mg_medicine is not null and trim(nor_tz_200mg_medicine) != '' then 1 else 0 end +
        case when doxylac_medicine is not null and trim(doxylac_medicine) != '' then 1 else 0 end +
        case when m2_tone_tablets is not null and trim(m2_tone_tablets) != '' then 1 else 0 end +
        case when omee_omak_medicine is not null and trim(omee_omak_medicine) != '' then 1 else 0 end +
        case when itra_100_medicine is not null and trim(itra_100_medicine) != '' then 1 else 0 end +
        case when itra_200_medicine is not null and trim(itra_200_medicine) != '' then 1 else 0 end +
        case when tranexamic_acid_medicine is not null and trim(tranexamic_acid_medicine) != '' then 1 else 0 end +
        case when letroquin_medicine is not null and trim(letroquin_medicine) != '' then 1 else 0 end +
        case when levocet_medicine is not null and trim(levocet_medicine) != '' then 1 else 0 end +
        case when azinetra_kit_medicine is not null and trim(azinetra_kit_medicine) != '' then 1 else 0 end +
        case when progesterone_medicine is not null and trim(progesterone_medicine) != '' then 1 else 0 end +
        case when multivitamin_medicine is not null and trim(multivitamin_medicine) != '' then 1 else 0 end +
        case when paracetamol_650_medicine is not null and trim(paracetamol_650_medicine) != '' then 1 else 0 end +
        case when multivitamin_syrup is not null and trim(multivitamin_syrup) != '' then 1 else 0 end
        as total_medicines_dispensed,
        
        sanitary_packets_count_int,
        case 
            when camp_booklet_given is not null and lower(trim(camp_booklet_given)) in ('yes', 'y', '1') then 1 
            else 0 
        end as booklet_provided,
        
        has_partnership,
        partnership_type,
        partner_name,
        
        experience_rating_numeric,
        additional_comments,
        data_entry_person,
        
        patient_phone,
        whatsapp_number,
        
        extracted_at
        
    from health_camp_data
),

summary_stats as (
    select 
        *,
        case 
            when visit_date_cleaned is not null 
            then extract(month from visit_date_cleaned)
            else null 
        end as visit_month,
        case 
            when visit_date_cleaned is not null 
            then extract(year from visit_date_cleaned)
            else null 
        end as visit_year,
        case 
            when visit_date_cleaned is not null 
            then extract(dow from visit_date_cleaned)
            else null 
        end as visit_day_of_week
    from medicine_dispensed
)

select 
    airbyte_id,
    patient_name,
    myna_patient_id,
    visit_date_cleaned as visit_date,
    visit_month,
    visit_year,
    visit_day_of_week,
    camp_location,
    doctor_name,
    chief_complaint,
    doctor_diagnosis,
    total_medicines_dispensed,
    sanitary_packets_count_int as sanitary_packets_provided,
    booklet_provided,
    has_partnership,
    partnership_type,
    partner_name,
    experience_rating_numeric,
    additional_comments,
    data_entry_person,
    patient_phone,
    whatsapp_number,
    extracted_at
from summary_stats
where patient_name is not null
order by visit_date desc, patient_name