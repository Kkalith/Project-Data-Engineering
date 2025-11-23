with 

src_status_appointments as (

    select status from {{ source('hospital', 'appointments') }}

),

renamed AS (
    SELECT DISTINCT
        {{ dbt_utils.generate_surrogate_key(['status']) }} AS id_status_appointment,
        status,

        CASE
            WHEN lower(status) IN ('no-show', 'cancelled') 
                THEN 'missed appointment'
            WHEN lower(status) = 'completed' 
                THEN 'attended appointment'
            WHEN lower(status) = 'scheduled' 
                THEN 'upcoming appointment'
            ELSE 'unknown'
        END AS appointment_category

    FROM src_status_appointments
)

select * from renamed