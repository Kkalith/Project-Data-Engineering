with 

src_appointments as (

    select * from {{ source('hospital', 'appointments') }}

),

renamed as (

    select
        md5(appointment_id) as id_appointment,
        md5(patient_id) as id_patient,
        md5(doctor_id) as id_doctor,
        appointment_date as date_appointment,
        appointment_time as time_appointment,
        reason_for_visit,
        status,
        check_in_time,
        check_out_time,
        wait_time_minutes,
        duration_minutes,
        cancel_reason

    from src_appointments

)

select * from renamed