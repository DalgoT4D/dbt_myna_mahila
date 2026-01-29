SELECT COUNT (gender), grade 
FROM {{ ref('stg_test') }} 
GROUP BY grade