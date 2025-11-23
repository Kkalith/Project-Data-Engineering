with 

src_billing as (

    select * from {{ source('hospital', 'billing') }}

),

renamed as (

    select
        bill_id,
        bill_date, --fecha que surgio el cobro
        amount as amount_dolars, --dinero 
        {{ clean_string('payment_status') }} as payment_status, -- normalizar??
        case when {{ clean_string('payment_status') }} = 'paid' then true else false end as is_payed,
        payment_date, --fecha de pago
        due_date, -- fecha vencimiento
        DATEDIFF(day, due_date, payment_date) AS payments_days, -- dias que tardo en pagar
        datediff(day, bill_date, payment_date) as billing_delay_days, -- dias que ??
        case when payment_date > due_date then true else false end as is_late_flag, -- fecha de retrado entre pago y fecha vencimiento
        late_fee as late_fee_dolars, 
        insurance_coverage_amount,
        patient_payment_amount,
        patient_payment_amount + insurance_coverage_amount as total_amount,
        (patient_payment_amount - insurance_coverage_amount) as diference_patient_insurance,
        md5(patient_id) as id_patient,
        md5(treatment_id) as id_treatment,
        md5(payment_method) as id_payment_method

    from src_billing

)

select * from renamed