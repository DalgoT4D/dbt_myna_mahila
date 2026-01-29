SELECT COUNT (student_id), grade 
FROM {{ ref('stg_test') }} 
GROUP BY grade