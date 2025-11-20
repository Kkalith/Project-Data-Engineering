with 

source as (

    select * from {{ source('hospital', 'treatments') }}

),

renamed as (

    select
        md5(treatment_id) as id_treatment,
        md5(appointment_id) as id_appointment,
        description,
        cost as cost_dolars,
        treatment_date,
        treatment_outcome,
        medications_administered,
        complications,
        duration_minutes,
        equipment_used,
        risk_level,
        md5(treatment_type) as id_treatment_type

    from source

)

select * from renamed