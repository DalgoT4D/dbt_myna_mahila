{{ config(
    materialized='table',
    tags=["college_data", "2025-2026"]
) }}

with source as (
    select * from {{ source('staging_college', 'Data') }}
),

cleaned_college_data as (
    select
        "Grade" as grade_level,
        "Sr_no" as serial_number,
        "Trainer" as trainer_name,
        "Division" as class_division,
        "Standard" as academic_standard,
        "Cross_check" as cross_check_status,
        "App_Download" as app_download_status,
        "Mobile_Number" as mobile_number,
        "Date_of_session" as session_date,
        "Dublicate_Check" as duplicate_check_status,
        "Name_of_College" as college_name,
        "Mobile_number_len" as mobile_number_length,
        "Pre_Form_Collected" as pre_form_collected,
        "Post_Form_Collected" as post_form_collected,
        "Name_of_Beneficiaries" as beneficiary_name,
        
        -- Data type conversions and cleaning
        case 
            when trim("Sr_no") ~ '^[0-9]+$' then "Sr_no"::integer
            else null
        end as serial_number_numeric,
        
        case 
            when trim("Mobile_Number") ~ '^[0-9]+$' and length(trim("Mobile_Number")) = 10 
            then trim("Mobile_Number")
            else null
        end as mobile_number_cleaned,
        
        case 
            when trim("Date_of_session") != '' and "Date_of_session" is not null 
            then cast("Date_of_session" as date)
            else null
        end as session_date_cleaned,
        
        case 
            when trim("Cross_check") ~ '^[0-9]+$' then "Cross_check"::integer
            else null
        end as cross_check_numeric,
        
        case 
            when trim("Dublicate_Check") ~ '^[0-9]+$' then "Dublicate_Check"::integer
            else null
        end as duplicate_check_numeric,
        
        -- Boolean conversions for form collection status
        case 
            when lower(trim("Pre_Form_Collected")) in ('yes', 'y', '1') then true
            when lower(trim("Pre_Form_Collected")) in ('no', 'n', '0') then false
            else null
        end as pre_form_collected_bool,
        
        case 
            when lower(trim("Post_Form_Collected")) in ('yes', 'y', '1') then true
            when lower(trim("Post_Form_Collected")) in ('no', 'n', '0') then false
            else null
        end as post_form_collected_bool,
        
        -- Extract grade number from grade level
        case 
            when "Grade" ~ 'Grade ([0-9]+)' then 
                substring("Grade", 'Grade ([0-9]+)')::integer
            else null
        end as grade_number,
        
        -- Standardize trainer names
        trim(upper("Trainer")) as trainer_name_standardized,
        
        -- Clean college names
        trim("Name_of_College") as college_name_cleaned,
        
        -- Extract session month and year
        case 
            when cast("Date_of_session" as date) is not null 
            then extract(month from cast("Date_of_session" as date))
            else null 
        end as session_month,
        
        case 
            when cast("Date_of_session" as date) is not null 
            then extract(year from cast("Date_of_session" as date))
            else null 
        end as session_year,
        
        case 
            when cast("Date_of_session" as date) is not null 
            then extract(dow from cast("Date_of_session" as date))
            else null 
        end as session_day_of_week
        
    from source
    where "Name_of_Beneficiaries" is not null
)

select * from cleaned_college_data