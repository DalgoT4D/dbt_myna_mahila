{{ config(
    materialized='table',
    tags=["health_camp", "2025-2026"]
) }}

with source as (
    select * from {{ source('staging_health_camp', 'Form_Responses_1') }}
),

renamed_cleaned as (
    select
        "Age" as patient_age,
        "ORS" as ors_given,
        "Date" as visit_date,
        "Dr_Name" as doctor_name,
        "Hyponid" as hyponid_medicine,
        "Myna_ID" as myna_patient_id,
        "Oral__L" as oral_medicine,
        "Diclogem" as diclogem_medicine,
        "Location" as camp_location,
        "Meftadol" as meftadol_medicine,
        "Timestamp" as form_timestamp,
        "Clobego_GM" as clobego_gm_medicine,
        "Flucon_150" as flucon_150_medicine,
        "Ironfast_Z" as ironfast_z_medicine,
        "Partnership" as partnership_status,
        "WhatsApp_no" as whatsapp_number,
        "Calcipic_500" as calcipic_500_medicine,
        "Camp_Booklet" as camp_booklet_given,
        "Cazole_Forte" as cazole_forte_medicine,
        "Nor_TZ_200mg" as nor_tz_200mg_medicine,
        "Phone_number" as patient_phone,
        "Comment_if_any" as additional_comments,
        "Doxy__Doxylac_B_" as doxylac_medicine,
        "M2__Tone_Tablets" as m2_tone_tablets,
        "Partnership_Type" as partnership_type,
        "Data_Entry_Person" as data_entry_person,
        "Doctors_Diagnosis" as doctor_diagnosis,
        "Omee_20__Omak_20_" as omee_omak_medicine,
        "Name_of_the_Partner" as partner_name,
        "Name_of_the_Patient" as patient_name,
        "Chief_Complaint_Type" as chief_complaint,
        "Itra_100__Itrajohn_100_" as itra_100_medicine,
        "Itra_200__Itrajohn_200_" as itra_200_medicine,
        "Transemic_acid__TRN_500_" as tranexamic_acid_medicine,
        "Letsi_2_5__Letroquin_2_5_" as letroquin_medicine,
        "Levocet__AZINE_L__Levophine_" as levocet_medicine,
        "3_Kit__Azinetra_Kit__Azito_FS_" as azinetra_kit_medicine,
        "Progestrone_200__Legestro_200_" as progesterone_medicine,
        "Picosule__Picosule__Multivitamin_" as multivitamin_medicine,
        "P___650__Parapic_650__Fepadol_650_" as paracetamol_650_medicine,
        "Number_of_sanitary_packets_provided" as sanitary_packets_count,
        "MV_Syrup__Zinprovit__Multivitamin_Syrup_" as multivitamin_syrup,
        "How_patient_rate_overall_experience_at_camp_" as patient_experience_rating,
        
        case 
            when trim("Date") != '' and "Date" is not null 
            then cast("Date" as date)
            else null
        end as visit_date_cleaned,
        
        case 
            when lower(trim("Partnership")) in ('yes', 'y', '1') then true
            when lower(trim("Partnership")) in ('no', 'n', '0') then false
            else null
        end as has_partnership,
        
        case 
            when trim("Number_of_sanitary_packets_provided") ~ '^[0-9]+$' 
            then cast("Number_of_sanitary_packets_provided" as integer)
            else null
        end as sanitary_packets_count_int,
        
        case
            when trim("How_patient_rate_overall_experience_at_camp_") ~ '^[0-9]+$'
            then cast("How_patient_rate_overall_experience_at_camp_" as integer)
            else null
        end as experience_rating_numeric,
        
        "_airbyte_raw_id" as airbyte_id,
        "_airbyte_extracted_at" as extracted_at,
        "_airbyte_meta" as airbyte_metadata
        
    from source
    where "_airbyte_raw_id" is not null
)

select * from renamed_cleaned