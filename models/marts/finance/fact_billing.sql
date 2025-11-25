{{ config(
    materialized = "incremental",
) }}

-- ============================================================
-- FACT BILLING
-- ============================================================
-- Este modelo integra los datos ya normalizados en staging
-- para construir la tabla de hechos principal del sistema.
-- ============================================================

WITH billing AS (
    SELECT *
    FROM {{ ref('stg_hospital__billing') }}
),

patients AS (
    SELECT id_patient
    FROM {{ ref('dim_patients') }}
),

doctors AS (
    SELECT id_doctor
    FROM {{ ref('dim_doctors') }}
),

payment_methods AS (
    SELECT id_payment_method
    FROM {{ ref('dim_payment_method') }}
),

payment_status AS (
    SELECT id_payment_status
    FROM {{ ref('dim_payment_status') }}
),

treatment_type AS (
    SELECT id_treatment_type
    FROM {{ ref('dim_treatment_type') }}
),

date_dim AS (
    SELECT id_date
    FROM {{ ref('dim_date') }}
),

-- ============================================================
-- ENRIQUECER BILLING CON DIMENSIONES
-- ============================================================

final AS (
    SELECT
        b.id_bill
        -- FKs a dimensiones
        p.id_patient,
        d.id_doctor,
        t.id_treatment_type,
        pm.id_payment_method,
        ps.id_payment_status,
        dd.id_date,

        -- Métricas del hecho
        b.amount_dolars,
        b.payment_status,
        b.is_payed,
        b.payments_days
        b.billing_delay_days,
        b.is_late,
        b.late_fee_dolars,
        b.insurance_coverage_amount,
        b.patient_payment_amount,
        b.total_amount

        -- Fechas
        b.bill_date,
        b.payment_date,
        b.due_date


    FROM billing b
    LEFT JOIN dim_patients        p   ON p.id_patient = b.id_patient
    LEFT JOIN dim_doctors         d   ON d.id_doctor = b.id_doctor
    LEFT JOIN dim_treatment_type  t   ON t.treatment_type = b.treatment_type
    LEFT JOIN dim_payment_methods pm  ON pm.payment_method = b.payment_method
    LEFT JOIN dim_payment_status  ps  ON ps.payment_status = b.payment_status
    LEFT JOIN dim_date        dd  ON dd.full_date = b.bill_date

    --{% if is_incremental() %}
      --WHERE b.updated_at > (SELECT MAX(updated_at) FROM {{ this }})
    --{% endif %}
)

SELECT *
FROM final;