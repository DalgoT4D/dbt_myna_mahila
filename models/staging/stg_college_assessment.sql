{{ config(
    materialized='table',
    tags=["college_assessment", "2025-2026"]
) }}

with source as (
    select * from {{ source('staging_college', 'Form_Responses_1') }}
),

cleaned_assessment as (
    select
        "Age" as student_age,
        "Timestamp" as form_timestamp,
        "Institution" as college_name,
        "Student_Name" as student_name,
        "Mobile_Number" as mobile_number,
        "Pre_Form_Date" as pre_form_date,
        "Post_Form_Date" as post_form_date,
        "Not_answered___22" as not_answered_22,
        "Name_of_Data_Entry_Person" as data_entry_person,
        "Mobile_number_wise_Cross_check" as mobile_crosscheck,
        
        -- Pre-assessment questions
        "Which_nutrients_are_most_important_to_replenish_during_periods_" as nutrients_pre_assessment,
        "What_can_help_relieve_pain_during_periods______________________" as pain_relief_pre_assessment,
        "After_how_many_days_do_periods_usually_occur___________________" as period_frequency_pre_assessment,
        "Tick_the_correct_option___Pre__Assessment__Washing_hair_is_not_" as washing_hair_myth_pre_assessment,
        "Tick_the_correct_option___Pre__Assessment__Eating_sour_foods_du" as sour_foods_myth_pre_assessment,
        "Tick_the_correct_option___Pre__Assessment__You_should_not_exerc" as exercise_myth_pre_assessment,
        "Tick_the_correct_option___Pre__Assessment__Touching_pickle_spoi" as pickle_myth_pre_assessment,
        "How_should_used_sanitary_pads_be_disposed_of___________________" as pad_disposal_pre_assessment,
        "Which_of_the_following_is_a_good_natural_source_of_Vitamin_C___" as vitamin_c_source_pre_assessment,
        "What_problems_can_arise_from_poor_menstrual_hygiene____________" as hygiene_problems_pre_assessment,
        "Tick_the_correct_option___Pre__Assessment__We_can_t_visit_holy_" as temple_myth_pre_assessment,
        "What_is_considered_a_healthy_and_acceptable_number_of_days_for_" as healthy_period_days_pre_assessment,
        "Changing_a_sanitary_pad_or_tampon_every______hours_is_generally" as pad_change_frequency_pre_assessment,
        
        -- Post-assessment questions
        "Which_nutrients_are_most_impo25_periods____Post__Assessment" as nutrients_post_assessment,
        "What_can_help_relieve_pain_du56__________Post__Assessment" as pain_relief_post_assessment,
        "After_how_many_days_do_period58_________Post__Assessment" as period_frequency_post_assessment,
        "Tick_the_correct_option___Post__Assessment__Washing_hair_is_not" as washing_hair_myth_post_assessment,
        "Tick_the_correct_option___Post__Assessment__Eating_sour_foods_d" as sour_foods_myth_post_assessment,
        "Tick_the_correct_option___Post__Assessment__You_should_not_exer" as exercise_myth_post_assessment,
        "Tick_the_correct_option___Post__Assessment__Touching_pickle_spo" as pickle_myth_post_assessment,
        "How_should_used_sanitary_pads70_________Post__Assessment" as pad_disposal_post_assessment,
        "Which_of_the_following_is_a_g82_________Post__Assessment" as vitamin_c_source_post_assessment,
        "What_problems_can_arise_from_89_________Post__Assessment" as hygiene_problems_post_assessment,
        "Tick_the_correct_option___Post__Assessment__We_can_t_visit_holy" as temple_myth_post_assessment,
        "What_is_considered_a_healthy_116_________Post__Assessment" as healthy_period_days_post_assessment,
        "Changing_a_sanitary_pad_or_ta134_________Post__Assessment" as pad_change_frequency_post_assessment,
        
        -- Session feedback (Likert scale 1-5)
        "Likert_Scale___1_to_5__The_quality_of_session__________________" as session_quality_rating,
        "Likert_Scale___1_to_5__Content_of_the_session__________________" as session_content_rating,
        "Likert_Scale___1_to_5__I_learned_new_things_from_this_session__" as learning_rating,
        "Likert_Scale___1_to_5__I_liked_the_way_the_session_was_conducte" as session_conduct_rating,
        "Likert_Scale___1_to_5__I_now_understand_menstrual_problems_bett" as understanding_rating,
        "Likert_Scale___1_to_5__The_app_was_easy_for_me_to_use__________" as app_usability_rating,
        "Likert_Scale___1_to_5__I_feel_confident_in_maintaining_hygiene_" as hygiene_confidence_rating,
        
        -- Open feedback
        "What_did_you_like_most_about_this_session______________________" as session_liked_feedback,
        "How_can_we_improve_this_session_further________________________" as session_improvement_feedback,
        
        -- Data type conversions and cleaning
        case 
            when trim("Age") ~ '^[0-9]+$' then "Age"::integer
            else null
        end as student_age_numeric,
        
        case 
            when trim("Mobile_Number") ~ '^[0-9]+$' and length(trim("Mobile_Number")) = 10 
            then trim("Mobile_Number")
            else null
        end as mobile_number_cleaned,
        
        case 
            when trim("Pre_Form_Date") != '' and "Pre_Form_Date" is not null 
            then cast("Pre_Form_Date" as date)
            else null
        end as pre_form_date_cleaned,
        
        case 
            when trim("Post_Form_Date") != '' and "Post_Form_Date" is not null 
            then cast("Post_Form_Date" as date)
            else null
        end as post_form_date_cleaned,
        
        case 
            when trim("Timestamp") != '' and "Timestamp" is not null 
            then cast("Timestamp" as timestamp)
            else null
        end as form_timestamp_cleaned,
        
        -- Convert Likert scale ratings to numeric
        case 
            when trim("Likert_Scale___1_to_5__The_quality_of_session__________________") ~ '^[1-5]$' 
            then "Likert_Scale___1_to_5__The_quality_of_session__________________"::integer
            else null
        end as session_quality_rating_numeric,
        
        case 
            when trim("Likert_Scale___1_to_5__Content_of_the_session__________________") ~ '^[1-5]$' 
            then "Likert_Scale___1_to_5__Content_of_the_session__________________"::integer
            else null
        end as session_content_rating_numeric,
        
        case 
            when trim("Likert_Scale___1_to_5__I_learned_new_things_from_this_session__") ~ '^[1-5]$' 
            then "Likert_Scale___1_to_5__I_learned_new_things_from_this_session__"::integer
            else null
        end as learning_rating_numeric,
        
        case 
            when trim("Likert_Scale___1_to_5__I_liked_the_way_the_session_was_conducte") ~ '^[1-5]$' 
            then "Likert_Scale___1_to_5__I_liked_the_way_the_session_was_conducte"::integer
            else null
        end as session_conduct_rating_numeric,
        
        case 
            when trim("Likert_Scale___1_to_5__I_now_understand_menstrual_problems_bett") ~ '^[1-5]$' 
            then "Likert_Scale___1_to_5__I_now_understand_menstrual_problems_bett"::integer
            else null
        end as understanding_rating_numeric,
        
        case 
            when trim("Likert_Scale___1_to_5__The_app_was_easy_for_me_to_use__________") ~ '^[1-5]$' 
            then "Likert_Scale___1_to_5__The_app_was_easy_for_me_to_use__________"::integer
            else null
        end as app_usability_rating_numeric,
        
        case 
            when trim("Likert_Scale___1_to_5__I_feel_confident_in_maintaining_hygiene_") ~ '^[1-5]$' 
            then "Likert_Scale___1_to_5__I_feel_confident_in_maintaining_hygiene_"::integer
            else null
        end as hygiene_confidence_rating_numeric
        
    from source
    where "Student_Name" is not null
)

select * from cleaned_assessment