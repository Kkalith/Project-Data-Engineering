WITH src AS (
    SELECT
        appointment_id,
        medications_administered
    FROM {{ source('hospital', 'treatments') }}
),

clean AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            "cast(appointment_id as varchar)",
            "cast(medications_administered as varchar)"
        ]) }} AS id_appointment_medication
    FROM src
)

SELECT *
FROM clean;
