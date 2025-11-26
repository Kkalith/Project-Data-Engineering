WITH base AS (
    SELECT
        a.id_appointment,
        a.date_appointment,

        a.wait_time_minutes,
        a.duration_minutes,
        a.wait_time_hours,
        a.duration_hours,
        a.show_up_message,

        -- denormalized reason for visit
        rv.reason_for_visit,
        rv.reason_category,

        -- denormalized cancellation reason
        cr.cancel_reason,
        cr.cancellation_status,

        -- denormalized appointment status
        st.status AS appointment_status,
        st.appointment_category,



    FROM {{ ref('stg_hospital__appointments') }} a
    LEFT JOIN {{ ref('stg_hospital__reason_for_visit') }} rv
        ON a.id_reason_for_visit = rv.id_reason_for_visit

    LEFT JOIN {{ ref('stg_hospital__cancel_reason') }} cr
        ON a.id_cancel_reason = cr.id_cancel_reason

    LEFT JOIN {{ ref('stg_hospital__status_appointment') }} st
        ON a.id_status_appointment = st.id_status_appointment
)

SELECT * FROM base