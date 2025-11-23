with 

src_appointments as (

    select * from {{ source('hospital', 'appointments') }}

),

renamed as (

    select
        appointment_id as id_appointment,
        
        appointment_date as date_appointment,
        appointment_time as time_appointment,

        check_in_time,
        check_out_time,

        timestamp_from_parts(date_appointment, time_appointment) as appointment_datetime,

        --(case when check_in_time is not null then true else false end) as attended_flag,(case when status = 'no-show' then true else false end) as no_show_flag,
        --(case when cancel_reason != 'none' then true else false end) as cancel_flag,
        wait_time_minutes::integer as wait_time_minutes,
        duration_minutes::integer as duration_minutes,
        ROUND(wait_time_minutes::numeric / 60, 2) AS wait_time_hours, -- tiempo esperado en horas
        ROUND(duration_minutes::numeric / 60, 2) AS duration_hours, -- duración cita en horas
        case
        when check_in_time is null then 'The patient did not show up to the appointment'
        else 'The patient attended the appointment'
        end as show_up_message,
        lower(trim(cancel_reason)) as id_cancel_reason,
        lower(trim(reason_for_visit)) as id_reason_for_visit,
        lower(trim(status)) as id_status_appointment,
        patient_id as id_patient,
        doctor_id as id_doctor

    from src_appointments

)

select * from renamed